// Command order-service runs the HTTP API, records orders and publishes
// OrderCreated events through the broker (gRPC), with an outbox fallback.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pad-lab2/src/api"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/order-service/service"
)

func main() {
	log := rpcx.NewLogger("order-service")
	brokerAddr := rpcx.Env("BROKER_ADDR", "localhost:7000")
	conn, err := client.Dial(brokerAddr)
	if err != nil {
		log.Error("cannot create broker client", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	policy := client.DefaultPublishPolicy
	policy.MaxAttempts = rpcx.EnvInt("PUBLISH_MAX_ATTEMPTS", policy.MaxAttempts)
	policy.PerAttemptTimeout = rpcx.EnvDuration("PUBLISH_TIMEOUT", policy.PerAttemptTimeout)
	svc, err := service.New(rpcx.Env("ORDER_DATA_DIR", "./data/orders"), client.NewPublisher(conn, policy, log), log)
	if err != nil {
		log.Error("cannot open order store", "error", err)
		os.Exit(1)
	}
	defer svc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go svc.RunOutbox(ctx, rpcx.EnvDuration("OUTBOX_INTERVAL", 2*time.Second))

	srv := &http.Server{
		Addr:              rpcx.Env("ORDER_LISTEN", ":8080"),
		Handler:           api.NewHandler(svc, func() string { return strings.ToLower(conn.GetState().String()) }, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("order-service listening", "event", "order_service_started", "addr", srv.Addr, "broker", brokerAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server stopped", "error", err)
		os.Exit(1)
	}
}
