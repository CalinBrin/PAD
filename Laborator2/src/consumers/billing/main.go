// Command billing is the second consumer: it charges every order at the
// external payment service (idempotent, with deadlines and reconciliation).
package main

import (
	"os"
	"strings"

	"google.golang.org/grpc"

	paymentv1 "pad-lab2/gen/payment/v1"
	"pad-lab2/src/consumers/billing/billing"
	"pad-lab2/src/consumers/common"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/payment-service/payment"
)

func main() {
	log := rpcx.NewLogger("billing")
	token := rpcx.Env("PAYMENT_API_TOKEN", "")
	if token == "" {
		log.Error("PAYMENT_API_TOKEN is not set (see .env.example)")
		os.Exit(1)
	}
	paymentAddr := rpcx.Env("PAYMENT_ADDR", "localhost:9000")
	conn, err := client.Dial(paymentAddr, grpc.WithPerRPCCredentials(payment.TokenCredentials(token)))
	if err != nil {
		log.Error("cannot create payment client", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	policy := billing.DefaultPolicy
	policy.MaxAttempts = rpcx.EnvInt("PAYMENT_MAX_ATTEMPTS", policy.MaxAttempts)
	policy.PerAttemptTimeout = rpcx.EnvDuration("PAYMENT_TIMEOUT", policy.PerAttemptTimeout)
	proc := &billing.Processor{Payment: paymentv1.NewPaymentClient(conn), Policy: policy,
		LookupTimeout: rpcx.EnvDuration("PAYMENT_LOOKUP_TIMEOUT", policy.PerAttemptTimeout), Currencies: billing.DefaultCurrencies, Log: log}
	(&common.Process{
		Group: "billing", Workers: rpcx.EnvInt("CONSUMER_WORKERS", 2), Prefetch: rpcx.EnvInt("CONSUMER_PREFETCH", 1),
		AckMode: "after", BrokerAddr: rpcx.Env("BROKER_ADDR", "localhost:7000"), HealthAddr: rpcx.Env("CONSUMER_HEALTH_ADDR", ":8090"),
		Handler: proc.Handle, Log: log,
		Dependency: func() (string, string) { return "payment", strings.ToLower(conn.GetState().String()) },
	}).Run()
}
