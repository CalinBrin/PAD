package dw

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// Faults are injected through the admin port of a warehouse replica
// (never reachable through the proxy). They reproduce a slow node, a node that
// answers 503, a node that fails its readiness check, and a write that is
// applied but whose answer arrives late.
type Faults struct {
	latency    atomic.Int64 // added before every data request
	writeDelay atomic.Int64 // added after a write is committed, before answering
	fail       atomic.Bool  // data requests answer 503
	notReady   atomic.Bool  // /readyz answers 503
	served     atomic.Int64 // data requests served (evidence for load balancing)
}

func (f *Faults) Latency() time.Duration        { return time.Duration(f.latency.Load()) }
func (f *Faults) WriteDelay() time.Duration     { return time.Duration(f.writeDelay.Load()) }
func (f *Faults) SetLatency(d time.Duration)    { f.latency.Store(int64(d)) }
func (f *Faults) SetWriteDelay(d time.Duration) { f.writeDelay.Store(int64(d)) }
func (f *Faults) SetFail(v bool)                { f.fail.Store(v) }
func (f *Faults) SetNotReady(v bool)            { f.notReady.Store(v) }
func (f *Faults) Served() int64                 { return f.served.Load() }

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

type faultState struct {
	Latency    string `json:"latency"`
	WriteDelay string `json:"write_delay"`
	Fail       bool   `json:"fail"`
	NotReady   bool   `json:"not_ready"`
	Served     int64  `json:"served"`
}

// AdminHandler serves GET/POST /admin/faults and GET /admin/stats.
// POST /admin/faults?latency=300ms&write_delay=3s&fail=true&ready=false, ?reset=1
func (f *Faults) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	state := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(faultState{f.Latency().String(), f.WriteDelay().String(),
			f.fail.Load(), f.notReady.Load(), f.served.Load()})
	}
	mux.HandleFunc("GET /admin/faults", func(w http.ResponseWriter, r *http.Request) { state(w) })
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, r *http.Request) { state(w) })
	mux.HandleFunc("POST /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("reset") != "" {
			f.SetLatency(0)
			f.SetWriteDelay(0)
			f.SetFail(false)
			f.SetNotReady(false)
		}
		if v := q.Get("latency"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				f.SetLatency(d)
			}
		}
		if v := q.Get("write_delay"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				f.SetWriteDelay(d)
			}
		}
		if v := q.Get("fail"); v != "" {
			f.SetFail(v == "true" || v == "1")
		}
		if v := q.Get("ready"); v != "" {
			f.SetNotReady(!(v == "true" || v == "1"))
		}
		state(w)
	})
	return mux
}
