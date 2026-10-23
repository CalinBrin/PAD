package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
)

// Process is the common main of a consumer: N subscribers of one group in one
// process (competing consumers), a health endpoint and a clean shutdown.
type Process struct {
	Group      string
	Workers    int
	Prefetch   int
	AckMode    string // "after" (at-least-once) or "before" (at-most-once)
	BrokerAddr string
	HealthAddr string
	Handler    client.Handler
	// Dependency, if set, reports the state of an external dependency for /readyz.
	Dependency func() (name, state string)
	Log        *slog.Logger
}

// CrashOnceHook returns the failure-injection hook for CRASH_ONCE: the
// process crashes the first time it reaches the stage, and a marker file in
// dataDir keeps it from crashing again after the restart.
func CrashOnceHook(stage, dataDir string) func(string, client.Delivery) bool {
	if stage == "" {
		return nil
	}
	marker := filepath.Join(dataDir, "crashed.marker")
	return func(s string, d client.Delivery) bool {
		if s != stage {
			return false
		}
		if _, err := os.Stat(marker); err == nil {
			return false
		}
		_ = os.WriteFile(marker, []byte(d.Message.MessageId+"\n"), 0o644)
		return true
	}
}

func (p *Process) Run() {
	conn, err := client.Dial(p.BrokerAddr)
	if err != nil {
		p.Log.Error("cannot create broker client", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var subscribed atomic.Int32
	if p.HealthAddr != "" {
		go p.serveHealth(&subscribed)
	}
	var wg sync.WaitGroup
	for i := 1; i <= p.Workers; i++ {
		name := fmt.Sprintf("%s-%s-%d", p.Group, hostname(), i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Run(ctx, conn, client.Options{Topic: envelope.TopicOrders, Group: p.Group, Name: name, Prefetch: p.Prefetch,
				AckBeforeProcess: p.AckMode == "before", Reconnect: true, Log: p.Log,
				OnSubscribed: func(up bool) {
					if up {
						subscribed.Add(1)
					} else {
						subscribed.Add(-1)
					}
				}}, p.Handler)
			if errors.Is(err, client.ErrCrash) {
				p.Log.Error("process crashing now (exit 137)", "event", "process_crash", "worker", name)
				os.Exit(137) // hard exit, like kill -9: no cleanup, no ACK
			}
		}()
	}
	p.Log.Info("consumer started", "event", "consumer_started", "group", p.Group, "workers", p.Workers, "ack_mode", p.AckMode, "broker", p.BrokerAddr)
	wg.Wait()
	p.Log.Info("consumer stopped", "event", "consumer_stopped")
}

// serveHealth: /healthz = process alive; /readyz = 200 when at least one
// subscriber holds a session with the broker; dependency states are reported
// separately so "broker down" and "payment down" are told apart.
func (p *Process) serveHealth(subscribed *atomic.Int32) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "alive"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		deps := map[string]string{"broker": "subscribed"}
		code, st := 200, "ready"
		if subscribed.Load() == 0 {
			deps["broker"] = "unavailable"
			code, st = 503, "not_ready"
		}
		if p.Dependency != nil {
			name, state := p.Dependency()
			deps[name] = state
			if state != "ready" && state != "idle" && code == 200 {
				st = "degraded"
			}
		}
		writeJSON(w, code, map[string]any{"status": st, "dependencies": deps})
	})
	if err := http.ListenAndServe(p.HealthAddr, mux); err != nil {
		p.Log.Error("health server stopped", "error", err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "local"
	}
	if len(h) > 12 {
		h = h[:12]
	}
	return h
}

// Logger is the logger of a consumer process.
func Logger(component string) *slog.Logger { return rpcx.NewLogger(component) }
