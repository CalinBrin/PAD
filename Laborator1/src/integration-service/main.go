// Command integration-service runs the message broker.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pad-lab1/src/integration-service/broker"
)

func main() {
	listen := flag.String("listen", ":7000", "broker TCP address")
	admin := flag.String("admin", ":8081", "admin HTTP address (empty to disable)")
	data := flag.String("data", "./data/broker", "directory of the persistent log")
	maxAttempts := flag.Int("max-attempts", 3, "deliveries before a message goes to the DLQ")
	ackTimeout := flag.Duration("ack-timeout", 10*time.Second, "redeliver messages not acked within this time")
	retryBase := flag.Duration("retry-base", 500*time.Millisecond, "base of the exponential retry backoff")
	queues := flag.String("queues", "", "pre-declared queues, e.g. orders:inventory,orders:billing")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("component", "broker")
	b, err := broker.New(broker.Config{DataDir: *data, MaxAttempts: *maxAttempts, AckTimeout: *ackTimeout, RetryBase: *retryBase, Logger: log})
	if err != nil {
		log.Error("start failed", "error", err)
		os.Exit(1)
	}
	for _, spec := range strings.Split(*queues, ",") {
		if spec = strings.TrimSpace(spec); spec == "" {
			continue
		}
		topic, group, ok := strings.Cut(spec, ":")
		if !ok {
			log.Error("invalid -queues entry, expected topic:group", "entry", spec)
			os.Exit(1)
		}
		if err := b.Declare(topic, group); err != nil {
			log.Error("declare failed", "error", err)
			os.Exit(1)
		}
	}
	if err := b.Listen(*listen); err != nil {
		log.Error("listen failed", "error", err)
		os.Exit(1)
	}
	log.Info("broker listening", "addr", b.Addr(), "max_attempts", *maxAttempts, "ack_timeout", ackTimeout.String())
	if *admin != "" {
		go func() {
			if err := http.ListenAndServe(*admin, b.AdminHandler()); err != nil {
				log.Error("admin server stopped", "error", err)
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
	_ = b.Close()
}
