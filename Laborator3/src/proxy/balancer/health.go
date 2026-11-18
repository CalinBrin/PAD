package balancer

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// HealthChecker polls GET /readyz of every backend (active detection).
// A backend leaves the rotation after Fall consecutive failed checks and
// comes back after Rise consecutive successful ones, even when no client
// request is sent to it.
type HealthChecker struct {
	Pool     *Pool
	Interval time.Duration
	Timeout  time.Duration
	Fall     int
	Rise     int
	Client   *http.Client
	Log      *slog.Logger

	mu    sync.Mutex
	fails map[*Backend]int
	oks   map[*Backend]int
}

// Run checks immediately, then every Interval, until ctx is done.
func (h *HealthChecker) Run(ctx context.Context) {
	if h.Interval <= 0 {
		h.Interval = 2 * time.Second
	}
	if h.Timeout <= 0 {
		h.Timeout = time.Second
	}
	if h.Fall <= 0 {
		h.Fall = 2
	}
	if h.Rise <= 0 {
		h.Rise = 2
	}
	if h.Client == nil {
		h.Client = &http.Client{}
	}
	t := time.NewTicker(h.Interval)
	defer t.Stop()
	for {
		h.CheckAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// CheckAll probes every backend once, in parallel.
func (h *HealthChecker) CheckAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range h.Pool.Backends() {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			h.record(b, h.probe(ctx, b))
		}(b)
	}
	wg.Wait()
}

func (h *HealthChecker) probe(ctx context.Context, b *Backend) error {
	ctx, cancel := context.WithTimeout(ctx, h.Timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.URL.String()+"/readyz", nil)
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &statusErr{resp.StatusCode}
	}
	return nil
}

type statusErr struct{ code int }

func (e *statusErr) Error() string { return "readyz answered " + http.StatusText(e.code) }

func (h *HealthChecker) record(b *Backend, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fails == nil {
		h.fails, h.oks = map[*Backend]int{}, map[*Backend]int{}
	}
	if err != nil {
		h.oks[b] = 0
		h.fails[b]++
		if h.fails[b] >= h.Fall && b.SetHealthy(false) && h.Log != nil {
			h.Log.Warn("backend removed from rotation (active health check)", "event", "backend_down",
				"backend", b.Name, "consecutive_failures", h.fails[b], "error", err)
		}
		return
	}
	h.fails[b] = 0
	h.oks[b]++
	if h.oks[b] >= h.Rise && b.SetHealthy(true) && h.Log != nil {
		h.Log.Info("backend back in rotation (active health check)", "event", "backend_up",
			"backend", b.Name, "consecutive_successes", h.oks[b])
	}
}
