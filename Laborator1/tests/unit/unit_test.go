package unit

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"pad-lab1/src/consumers/common"
	"pad-lab1/src/integration-service/broker"
	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
)

func TestNewMessageHasAllMandatoryFields(t *testing.T) {
	m, err := protocol.NewMessage(protocol.TypeOrderCreated, "corr-1", protocol.OrderCreated{OrderID: "o1", CustomerID: "c1", Amount: 5})
	if err != nil {
		t.Fatal(err)
	}
	if m.ID == "" || m.Timestamp.IsZero() || m.CorrelationID != "corr-1" || m.SchemaVersion != protocol.SchemaVersion {
		t.Fatalf("incomplete envelope: %+v", m)
	}
}

func TestNewMessageGeneratesCorrelationIDAndUniqueIDs(t *testing.T) {
	a, _ := protocol.NewMessage("T", "", map[string]int{"x": 1})
	b, _ := protocol.NewMessage("T", "", map[string]int{"x": 1})
	if a.CorrelationID == "" || a.ID == b.ID || a.CorrelationID == b.CorrelationID {
		t.Fatalf("ids must be generated and unique: %+v %+v", a, b)
	}
}

func TestValidateRejectsBadMessages(t *testing.T) {
	good, _ := protocol.NewMessage("T", "c", map[string]int{})
	cases := map[string]func(m *protocol.Message){
		"no id":             func(m *protocol.Message) { m.ID = "" },
		"no type":           func(m *protocol.Message) { m.Type = "" },
		"no correlation id": func(m *protocol.Message) { m.CorrelationID = "" },
		"no timestamp":      func(m *protocol.Message) { m.Timestamp = time.Time{} },
		"future schema":     func(m *protocol.Message) { m.SchemaVersion = 99 },
		"schema zero":       func(m *protocol.Message) { m.SchemaVersion = 0 },
		"bad payload":       func(m *protocol.Message) { m.Payload = []byte("{oops") },
	}
	for name, mutate := range cases {
		m := good
		mutate(&m)
		if m.Validate() == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	base := 100 * time.Millisecond
	if broker.Backoff(base, 1) != base || broker.Backoff(base, 2) != 2*base || broker.Backoff(base, 3) != 4*base {
		t.Fatal("backoff must double each attempt")
	}
	if broker.Backoff(base, 50) > 30*time.Second {
		t.Fatal("backoff must be capped")
	}
}

func TestEffectStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.jsonl")
	s, err := common.OpenEffectStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Has("m1") {
		t.Fatal("empty store must not know m1")
	}
	if err := s.Apply(common.Effect{MessageID: "m1", Consumer: "inventory"}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := common.OpenEffectStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.Has("m1") || s2.Count("m1") != 1 || s2.Total() != 1 {
		t.Fatal("restarted store must remember applied effects (dedup key)")
	}
}

// Errors about the message itself are permanent; a failed local write is not.
func TestProcessorClassifiesPermanentErrors(t *testing.T) {
	store, err := common.OpenEffectStore(filepath.Join(t.TempDir(), "effects.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p := &common.Processor{Consumer: "billing", Action: "create_invoice", Store: store, Dedup: true, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Validate: func(o protocol.OrderCreated) error {
			if o.Amount <= 0 {
				return errors.New("cannot invoice a non-positive amount")
			}
			return nil
		}}

	bad, _ := protocol.NewMessage(protocol.TypeOrderCreated, "c", protocol.OrderCreated{OrderID: "o1", CustomerID: "c1", Amount: -5})
	if err := p.Handle(client.Delivery{Message: bad, Attempt: 1}); !client.IsPermanent(err) {
		t.Fatalf("business validation error must be permanent, got %v", err)
	}
	unknown, _ := protocol.NewMessage("SomethingElse", "c", map[string]int{})
	if err := p.Handle(client.Delivery{Message: unknown, Attempt: 1}); !client.IsPermanent(err) {
		t.Fatalf("unsupported type must be permanent, got %v", err)
	}
	good, _ := protocol.NewMessage(protocol.TypeOrderCreated, "c", protocol.OrderCreated{OrderID: "o2", CustomerID: "c1", Amount: 5})
	store.Close() // the local write now fails: a transient error, to be retried
	if err := p.Handle(client.Delivery{Message: good, Attempt: 1}); err == nil || client.IsPermanent(err) {
		t.Fatalf("a failed effect write must be a transient error, got %v", err)
	}
}
