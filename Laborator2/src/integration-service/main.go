// Command integration-service runs the message broker (gRPC).
package main

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	brokerv1 "pad-lab2/gen/broker/v1"
	"pad-lab2/src/integration-service/broker"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/integration-service/server"
)

func main() {
	log := rpcx.NewLogger("broker")
	cfg := broker.Config{
		DataDir:        rpcx.Env("BROKER_DATA_DIR", "./data/broker"),
		MaxAttempts:    rpcx.EnvInt("BROKER_MAX_ATTEMPTS", 3),
		AckTimeout:     rpcx.EnvDuration("BROKER_ACK_TIMEOUT", 0),
		RetryBase:      rpcx.EnvDuration("BROKER_RETRY_BASE", 0),
		DedupRetention: rpcx.EnvDuration("BROKER_DEDUP_RETENTION", 0),
		Logger:         log,
	}
	b, err := broker.New(cfg)
	if err != nil {
		log.Error("start failed", "error", err)
		os.Exit(1)
	}
	for _, spec := range strings.Split(rpcx.Env("BROKER_QUEUES", ""), ",") {
		if spec = strings.TrimSpace(spec); spec == "" {
			continue
		}
		topic, group, ok := strings.Cut(spec, ":")
		if !ok {
			log.Error("invalid BROKER_QUEUES entry, expected topic:group", "entry", spec)
			os.Exit(1)
		}
		if err := b.Declare(topic, group); err != nil {
			log.Error("declare failed", "error", err)
			os.Exit(1)
		}
	}

	// Demo fault injection: lose the answer of the next N publishes (after commit).
	faults := &rpcx.LoseResponses{Methods: map[string]bool{brokerv1.Broker_Publish_FullMethodName: true}, Log: log}
	faults.Remaining.Store(int64(rpcx.EnvInt("BROKER_LOSE_PUBLISH_RESPONSES", 0)))
	var extra []grpc.UnaryServerInterceptor
	if faults.Remaining.Load() > 0 {
		extra = append(extra, faults.Interceptor())
	}
	gs, _ := server.New(b, log, extra...)

	listen := rpcx.Env("BROKER_LISTEN", ":7000")
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Error("listen failed", "error", err)
		os.Exit(1)
	}
	go func() {
		if err := gs.Serve(ln); err != nil {
			log.Error("grpc server stopped", "error", err)
		}
	}()
	if admin := rpcx.Env("BROKER_ADMIN", ":8081"); admin != "" {
		go func() {
			if err := http.ListenAndServe(admin, b.AdminHandler()); err != nil {
				log.Error("admin server stopped", "error", err)
			}
		}()
	}
	log.Info("broker listening", "event", "broker_started", "grpc", ln.Addr().String(), "max_attempts", cfg.MaxAttempts)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down", "event", "broker_stopping")
	_ = b.Close() // ends the consume sessions; unacked messages stay in the log
	gs.Stop()
}
