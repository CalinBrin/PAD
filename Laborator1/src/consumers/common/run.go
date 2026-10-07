package common

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
)

// Spec describes one consumer service.
type Spec struct {
	Name     string // also the consumer group, e.g. "inventory"
	Action   string
	Validate func(protocol.OrderCreated) error
}

// Run parses the flags and runs the consumer until SIGINT/SIGTERM.
func Run(spec Spec) {
	brokerAddr := flag.String("broker", "localhost:7000", "broker address")
	workers := flag.Int("workers", 2, "concurrent subscribers of this group inside the process")
	prefetch := flag.Int("prefetch", 1, "max unacknowledged messages per subscriber")
	dataDir := flag.String("data", "./data/"+spec.Name, "directory of the effect store")
	dedup := flag.Bool("dedup", true, "idempotent consumer: skip messages already processed")
	ackMode := flag.String("ack-mode", "after", "after = at-least-once, before = at-most-once")
	crashOnce := flag.String("crash-once", "", "failure injection: before_effect or after_effect, crashes once, then never again")
	delay := flag.Duration("work-delay", 0, "simulated processing time per message")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("component", spec.Name)

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Error("data dir", "error", err)
		os.Exit(1)
	}
	store, err := OpenEffectStore(filepath.Join(*dataDir, "effects.jsonl"))
	if err != nil {
		log.Error("effect store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	var crashHook func(string, client.Delivery) bool
	if *crashOnce != "" {
		marker := filepath.Join(*dataDir, "crashed.marker")
		var mu sync.Mutex
		crashHook = func(stage string, d client.Delivery) bool {
			mu.Lock()
			defer mu.Unlock()
			if stage != *crashOnce {
				return false
			}
			if _, err := os.Stat(marker); err == nil {
				return false // already crashed once in a previous run
			}
			_ = os.WriteFile(marker, []byte(d.Message.ID), 0o644)
			return true
		}
	}

	proc := &Processor{Consumer: spec.Name, Action: spec.Action, Store: store, Dedup: *dedup, Validate: spec.Validate, CrashAt: crashHook, Delay: *delay, Log: log}

	stop := make(chan struct{})
	var once sync.Once
	exitCode := 0
	var subs []*client.Subscriber
	for i := 1; i <= *workers; i++ {
		name := fmt.Sprintf("%s-%d", spec.Name, i)
		s, err := client.Subscribe(*brokerAddr, client.SubscribeOptions{Topic: protocol.TopicOrders, Group: spec.Name, Name: name, Prefetch: *prefetch, AckBeforeProcess: *ackMode == "before"}, proc.Handle)
		if err != nil {
			log.Error("cannot subscribe", "worker", name, "error", err)
			os.Exit(1)
		}
		subs = append(subs, s)
		go func(s *client.Subscriber, name string) {
			<-s.Done()
			if err := s.Err(); err != nil {
				if errors.Is(err, client.ErrCrash) {
					log.Error("process crashing now (exit 137)", "worker", name)
					os.Exit(137) // hard exit: no deferred cleanup, like a kill -9
				}
				log.Error("connection to broker lost", "worker", name, "error", err)
				exitCode = 1
				once.Do(func() { close(stop) })
			}
		}(s, name)
	}
	log.Info("consumer started", "group", spec.Name, "workers", *workers, "ack_mode", *ackMode, "dedup", *dedup, "crash_once", *crashOnce)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-stop:
	}
	for _, s := range subs {
		s.Close()
	}
	time.Sleep(100 * time.Millisecond)
	os.Exit(exitCode)
}
