// Package testutil has small helpers shared by the test packages.
package testutil

import (
	"sync"
	"testing"
	"time"

	"pad-lab1/src/integration-service/broker"
	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
)

// FastConfig returns a broker config with short timings suitable for tests.
func FastConfig(dir string) broker.Config {
	return broker.Config{DataDir: dir, MaxAttempts: 3, AckTimeout: 300 * time.Millisecond, RetryBase: 10 * time.Millisecond}
}

// StartBroker starts a broker on a random local port and closes it at test end.
func StartBroker(t *testing.T, cfg broker.Config) *broker.Broker {
	t.Helper()
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	if err := b.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// WaitFor polls cond until it is true or the timeout expires.
func WaitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// Publish publishes one OrderCreated message and returns it.
func Publish(t *testing.T, addr, correlationID string, amount float64) protocol.Message {
	t.Helper()
	p, err := client.DialPublisher(addr)
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer p.Close()
	return PublishWith(t, p, correlationID, amount)
}

func PublishWith(t *testing.T, p *client.Publisher, correlationID string, amount float64) protocol.Message {
	t.Helper()
	m, err := protocol.NewMessage(protocol.TypeOrderCreated, correlationID, protocol.OrderCreated{OrderID: "ord-" + protocol.NewID()[:6], CustomerID: "c1", Amount: amount})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if _, err := p.Publish(protocol.TopicOrders, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return m
}

// Recorder collects the IDs of delivered messages in a thread-safe way.
type Recorder struct {
	mu    sync.Mutex
	ids   []string
	count map[string]int
}

func NewRecorder() *Recorder { return &Recorder{count: map[string]int{}} }

func (r *Recorder) Add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
	r.count[id]++
}

func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

func (r *Recorder) Distinct() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.count)
}

func (r *Recorder) Count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count[id]
}

func (r *Recorder) IDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}
