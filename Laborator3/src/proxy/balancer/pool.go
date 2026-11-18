// Package balancer chooses the warehouse replica that serves a request:
// round-robin (default) or least-connections over the replicas that are
// healthy (active health check) and whose circuit breaker admits traffic
// (passive detection).
package balancer

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// Backend is one warehouse replica.
type Backend struct {
	Name    string
	URL     *url.URL
	Breaker *Breaker

	healthy  atomic.Bool
	inflight atomic.Int64
	served   atomic.Int64
	failed   atomic.Int64
}

func (b *Backend) Healthy() bool          { return b.healthy.Load() }
func (b *Backend) SetHealthy(v bool) bool { return b.healthy.Swap(v) != v }
func (b *Backend) InFlight() int64        { return b.inflight.Load() }

// Start marks a request as sent to the backend; call the returned func when done.
func (b *Backend) Start() func() {
	b.inflight.Add(1)
	b.served.Add(1)
	return func() { b.inflight.Add(-1) }
}

func (b *Backend) CountFailure() { b.failed.Add(1) }

// Status is the public view of a backend (admin /proxy/stats).
type Status struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Healthy  bool   `json:"healthy"`
	Breaker  string `json:"breaker"`
	InFlight int64  `json:"inflight"`
	Served   int64  `json:"served"`
	Failed   int64  `json:"failed"`
}

const (
	RoundRobin = "round_robin"
	LeastConn  = "least_conn"
)

// Pool holds the backends and the selection algorithm. Safe for concurrent use.
type Pool struct {
	backends []*Backend
	algo     atomic.Value // string
	next     atomic.Uint64
	mu       sync.Mutex // serialises Acquire in least_conn picks
}

// ParseBackends reads "dw-1=http://dw-1:8000,dw-2=http://dw-2:8000".
func ParseBackends(specs []string, newBreaker func() *Breaker) ([]*Backend, error) {
	var out []*Backend
	for i, s := range specs {
		name, raw, ok := strings.Cut(s, "=")
		if !ok {
			name, raw = fmt.Sprintf("dw-%d", i+1), s
		}
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("invalid backend %q", s)
		}
		b := &Backend{Name: name, URL: u, Breaker: newBreaker()}
		b.healthy.Store(true)
		out = append(out, b)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no backend configured")
	}
	return out, nil
}

func NewPool(backends []*Backend, algorithm string) *Pool {
	p := &Pool{backends: backends}
	p.SetAlgorithm(algorithm)
	return p
}

func (p *Pool) SetAlgorithm(a string) error {
	switch a {
	case "", RoundRobin:
		p.algo.Store(RoundRobin)
	case LeastConn:
		p.algo.Store(LeastConn)
	default:
		return fmt.Errorf("unknown algorithm %q (round_robin | least_conn)", a)
	}
	return nil
}

func (p *Pool) Algorithm() string { return p.algo.Load().(string) }

func (p *Pool) Backends() []*Backend { return p.backends }

// Pick returns an available backend not in exclude, or nil. The breaker of
// the returned backend has admitted the request (Acquire).
func (p *Pool) Pick(exclude map[*Backend]bool) *Backend {
	n := len(p.backends)
	if p.Algorithm() == LeastConn {
		p.mu.Lock()
		defer p.mu.Unlock()
		var best *Backend
		start := int(p.next.Add(1)) // rotate the start so ties are spread
		for i := 0; i < n; i++ {
			b := p.backends[(start+i)%n]
			if exclude[b] || !b.Healthy() || !b.Breaker.Available() {
				continue
			}
			if best == nil || b.InFlight() < best.InFlight() {
				best = b
			}
		}
		if best != nil && best.Breaker.Acquire() {
			return best
		}
		return nil
	}
	// round robin: the next available backend after the previous choice
	start := p.next.Add(1) - 1
	for i := 0; i < n; i++ {
		b := p.backends[(int(start)+i)%n]
		if exclude[b] || !b.Healthy() {
			continue
		}
		if b.Breaker.Acquire() {
			return b
		}
	}
	return nil
}

// Status returns the state of every backend.
func (p *Pool) Status() []Status {
	out := make([]Status, 0, len(p.backends))
	for _, b := range p.backends {
		out = append(out, Status{b.Name, b.URL.String(), b.Healthy(), b.Breaker.State(),
			b.inflight.Load(), b.served.Load(), b.failed.Load()})
	}
	return out
}
