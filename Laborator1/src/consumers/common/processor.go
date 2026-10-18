// Package common holds what both consumers (inventory, billing) share:
// the idempotent processor, the effect store and the process runner.
package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
)

// Effect is the "local effect" of processing a message (what the consumer
// does to its own state). It is an append-only file; the message ID inside it
// doubles as the deduplication key, so effect and dedup record are one atomic write.
type Effect struct {
	MessageID     string    `json:"message_id"`
	CorrelationID string    `json:"correlation_id"`
	Consumer      string    `json:"consumer"`
	Action        string    `json:"action"`
	OrderID       string    `json:"order_id"`
	Amount        float64   `json:"amount"`
	At            time.Time `json:"at"`
}

type EffectStore struct {
	mu   sync.Mutex
	f    *os.File
	seen map[string]int // message ID -> number of times the effect was applied
}

// OpenEffectStore opens (or creates) the effect file and loads it, so that a
// restarted consumer still remembers what it already did.
func OpenEffectStore(path string) (*EffectStore, error) {
	s := &EffectStore{seen: map[string]int{}}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var e Effect
			if json.Unmarshal(sc.Bytes(), &e) == nil && e.MessageID != "" {
				s.seen[e.MessageID]++
			}
		}
		f.Close()
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	return s, nil
}

func (s *EffectStore) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id] > 0
}

// Count returns how many times the effect of a message was applied.
func (s *EffectStore) Count(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id]
}

// Total returns the number of applied effects.
func (s *EffectStore) Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.seen {
		n += c
	}
	return n
}

// Apply appends the effect and fsyncs it.
func (s *EffectStore) Apply(e Effect) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.seen[e.MessageID]++
	return nil
}

func (s *EffectStore) Close() error { return s.f.Close() }

// Crash stages used by the failure-injection hook.
const (
	StageBeforeEffect = "before_effect"
	StageAfterEffect  = "after_effect"
)

// Processor is the message handler of a consumer.
type Processor struct {
	Consumer string // e.g. "inventory"
	Action   string // e.g. "reserve_stock"
	Store    *EffectStore
	Dedup    bool // skip messages whose effect was already applied (idempotent consumer)
	Validate func(protocol.OrderCreated) error
	// CrashAt is an optional failure-injection hook: returning true at the
	// given stage makes the processor "crash" (drop the connection with no ACK).
	CrashAt func(stage string, d client.Delivery) bool
	Delay   time.Duration // simulated work time
	Log     *slog.Logger
}

// Handle implements client.Handler.
func (p *Processor) Handle(d client.Delivery) error {
	m := d.Message
	log := p.Log.With("correlation_id", m.CorrelationID, "message_id", m.ID, "attempt", d.Attempt)

	if m.Type != protocol.TypeOrderCreated || m.SchemaVersion != protocol.SchemaVersion {
		log.Error("unsupported message", "type", m.Type, "schema_version", m.SchemaVersion)
		return client.Permanent(errors.New("unsupported message type or schema version"))
	}
	// Errors about the message itself are permanent: a retry would fail the
	// same way, so the message goes to the DLQ at once.
	var oc protocol.OrderCreated
	if err := json.Unmarshal(m.Payload, &oc); err != nil {
		log.Error("invalid payload", "error", err)
		return client.Permanent(err)
	}
	if p.Validate != nil {
		if err := p.Validate(oc); err != nil {
			log.Warn("business validation failed, permanent error", "order_id", oc.OrderID, "error", err)
			return client.Permanent(err)
		}
	}
	if p.Dedup && p.Store.Has(m.ID) {
		log.Info("duplicate delivery, effect skipped, will ACK", "order_id", oc.OrderID)
		return nil
	}
	if p.crash(StageBeforeEffect, d) {
		log.Error("SIMULATED CRASH before effect", "order_id", oc.OrderID)
		return client.ErrCrash
	}
	if p.Delay > 0 {
		time.Sleep(p.Delay)
	}
	if err := p.Store.Apply(Effect{MessageID: m.ID, CorrelationID: m.CorrelationID, Consumer: p.Consumer, Action: p.Action, OrderID: oc.OrderID, Amount: oc.Amount, At: time.Now().UTC()}); err != nil {
		log.Error("could not apply effect, will be retried", "error", err)
		return err // transient (disk, I/O): retried by the broker
	}
	log.Info("local effect applied", "action", p.Action, "order_id", oc.OrderID)
	if p.crash(StageAfterEffect, d) {
		log.Error("SIMULATED CRASH after effect, before ACK", "order_id", oc.OrderID)
		return client.ErrCrash
	}
	return nil
}

func (p *Processor) crash(stage string, d client.Delivery) bool {
	return p.CrashAt != nil && p.CrashAt(stage, d)
}
