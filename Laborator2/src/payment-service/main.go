// Command payment-service is the external service called by billing.
package main

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/payment-service/payment"
)

func main() {
	log := rpcx.NewLogger("payment")
	svc, err := payment.New(payment.Config{
		DataDir:    rpcx.Env("PAYMENT_DATA_DIR", "./data/payment"),
		Token:      rpcx.Env("PAYMENT_API_TOKEN", ""),
		LimitCents: int64(rpcx.EnvInt("PAYMENT_LIMIT_CENTS", 1_000_000)),
		Latency:    rpcx.EnvDuration("PAYMENT_LATENCY", 0),
		Logger:     log,
	})
	if err != nil {
		log.Error("start failed (is PAYMENT_API_TOKEN set? see .env.example)", "error", err)
		os.Exit(1)
	}
	defer svc.Close()
	svc.Faults.Remaining.Store(int64(rpcx.EnvInt("PAYMENT_LOSE_RESPONSES", 0)))

	ln, err := net.Listen("tcp", rpcx.Env("PAYMENT_LISTEN", ":9000"))
	if err != nil {
		log.Error("listen failed", "error", err)
		os.Exit(1)
	}
	gs := svc.GRPCServer()
	go func() {
		if err := gs.Serve(ln); err != nil {
			log.Error("grpc server stopped", "error", err)
		}
	}()
	go func() {
		if err := http.ListenAndServe(rpcx.Env("PAYMENT_ADMIN", ":8082"), svc.AdminHandler()); err != nil {
			log.Error("admin server stopped", "error", err)
		}
	}()
	log.Info("payment service listening", "event", "payment_started", "grpc", ln.Addr().String())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	gs.GracefulStop()
}
