// Package common holds what the consumers share: the effect store (local
// effect + deduplication key in one write), the inventory processor and the
// process runner.
package common

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Effect is the local effect of processing a message. The file is
// append-only; the message ID inside it doubles as the deduplication key, so
// the effect and its dedup record are written atomically, in one line.
type Effect struct {
	MessageID     string    `json:"message_id"`
	CorrelationID string    `json:"correlation_id"`
	Consumer      string    `json:"consumer"`
	Action        string    `json:"action"`
	OrderID       string    `json:"order_id"`
	AmountCents   int64     `json:"amount_cents"`
	At            time.Time `json:"at"`
}

type EffectStore struct {
	mu   sync.Mutex
	f    *os.File
	seen map[string]int
}

// OpenEffectStore opens (or creates) the effect file and loads it, so a
// restarted consumer remembers what it already did.
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

func (s *EffectStore) Has(id string) bool { return s.Count(id) > 0 }

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

// Apply appends the effect and fsyncs it, without checking for duplicates.
func (s *EffectStore) Apply(e Effect) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyLocked(e)
}

// ApplyOnce applies the effect unless one for the same message ID exists. The
// check and the write happen under one lock, so two concurrent deliveries of
// the same message (a slow consumer and the redelivery after its ack timeout)
// cannot both apply it. applied is false for a duplicate.
func (s *EffectStore) ApplyOnce(e Effect) (applied bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[e.MessageID] > 0 {
		return false, nil
	}
	return true, s.applyLocked(e)
}

func (s *EffectStore) applyLocked(e Effect) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
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
