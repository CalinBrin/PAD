package failure

import (
	"encoding/json"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"io"
	"log/slog"

	"pad-lab1/src/consumers/common"
	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
	"pad-lab1/tests/testutil"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// crashOnce returns a hook that "crashes" the first time it reaches the given stage.
func crashOnce(stage string) func(string, client.Delivery) bool {
	var fired atomic.Bool
	return func(s string, d client.Delivery) bool { return s == stage && fired.CompareAndSwap(false, true) }
}

type attempt struct {
	Attempt     int
	Redelivered bool
}

// runCrashScenario is THE failure scenario of the lab: the consumer applies
// its local effect, then dies before sending ACK. A second consumer instance
// (the restarted process, with the same effect file) receives the redelivery.
func runCrashScenario(t *testing.T, dedup bool) (effects int, redelivery attempt, queueEmpty bool) {
	t.Helper()
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	effectFile := filepath.Join(t.TempDir(), "effects.jsonl")

	store1, err := common.OpenEffectStore(effectFile)
	if err != nil {
		t.Fatal(err)
	}
	p1 := &common.Processor{Consumer: "inventory", Action: "reserve_stock", Store: store1, Dedup: dedup, CrashAt: crashOnce(common.StageAfterEffect), Log: quiet}
	s1, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "inv-A"}, p1.Handle)
	if err != nil {
		t.Fatal(err)
	}

	m := testutil.Publish(t, b.Addr(), "corr-crash", 99)

	// Consumer A applies the effect and crashes before ACK.
	select {
	case <-s1.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("consumer A did not crash")
	}
	if s1.Err() != client.ErrCrash {
		t.Fatalf("expected simulated crash, got %v", s1.Err())
	}
	if store1.Count(m.ID) != 1 {
		t.Fatal("the local effect must have been applied before the crash")
	}
	store1.Close()
	// The broker never got an ACK, so the message is not lost: it is pending again.
	testutil.WaitFor(t, 2*time.Second, "message requeued after consumer crash", func() bool {
		s := b.Stats()["orders.inventory"]
		return s.Pending == 1 && s.Inflight == 0
	})

	// "Restart": a new consumer process opens the same effect file.
	store2, err := common.OpenEffectStore(effectFile)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	p2 := &common.Processor{Consumer: "inventory", Action: "reserve_stock", Store: store2, Dedup: dedup, Log: quiet}
	var mu sync.Mutex
	var seen attempt
	s2, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "inv-B"}, func(d client.Delivery) error {
		mu.Lock()
		seen = attempt{d.Attempt, d.Redelivered}
		mu.Unlock()
		return p2.Handle(d)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	testutil.WaitFor(t, 2*time.Second, "redelivered message acked", func() bool {
		s := b.Stats()["orders.inventory"]
		return s.Pending == 0 && s.Inflight == 0 && s.Retrying == 0
	})
	mu.Lock()
	defer mu.Unlock()
	return store2.Count(m.ID), seen, true
}

// At-least-once + idempotent consumer = the effect happens exactly once.
func TestCrashAfterEffectBeforeAck_WithDedup_EffectAppliedOnce(t *testing.T) {
	effects, redelivery, empty := runCrashScenario(t, true)
	if effects != 1 {
		t.Fatalf("with deduplication the effect must be applied exactly once, got %d", effects)
	}
	if redelivery.Attempt != 2 || !redelivery.Redelivered {
		t.Fatalf("message must be redelivered as attempt 2, got %+v", redelivery)
	}
	if !empty {
		t.Fatal("queue must be empty after the redelivery was acked")
	}
}

// At-least-once without deduplication: the duplicate is visible. This is why consumers must be idempotent.
func TestCrashAfterEffectBeforeAck_WithoutDedup_EffectAppliedTwice(t *testing.T) {
	effects, redelivery, _ := runCrashScenario(t, false)
	if effects != 2 {
		t.Fatalf("without deduplication at-least-once duplicates the effect, expected 2 got %d", effects)
	}
	if redelivery.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %+v", redelivery)
	}
}

// At-most-once (ACK before processing): the same crash LOSES the message.
func TestAckBeforeProcess_CrashLosesMessage_AtMostOnce(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	store, _ := common.OpenEffectStore(filepath.Join(t.TempDir(), "effects.jsonl"))
	defer store.Close()
	p := &common.Processor{Consumer: "inventory", Action: "reserve_stock", Store: store, Dedup: true, CrashAt: crashOnce(common.StageBeforeEffect), Log: quiet}
	s, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "inv-A", AckBeforeProcess: true}, p.Handle)
	if err != nil {
		t.Fatal(err)
	}
	m := testutil.Publish(t, b.Addr(), "corr-amo", 1)
	<-s.Done()

	testutil.WaitFor(t, time.Second, "queue empty", func() bool {
		st := b.Stats()["orders.inventory"]
		return st.Pending == 0 && st.Inflight == 0 && st.Retrying == 0
	})
	time.Sleep(100 * time.Millisecond)
	if store.Count(m.ID) != 0 {
		t.Fatal("effect must not exist: crash happened before it")
	}
	if st := b.Stats()["orders.inventory"]; st.Pending != 0 || st.DLQ != 0 {
		t.Fatalf("message is gone for good (at-most-once), stats %+v", st)
	}
}

// Grade 8: a consumer that disconnects while processing does not lose the message.
func TestConsumerDisconnectsMidProcessing_MessageRedeliveredToOtherConsumer(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var once sync.Once
	victim, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "victim"}, func(d client.Delivery) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	m := testutil.Publish(t, b.Addr(), "corr-disc", 1)
	<-started

	rec := testutil.NewRecorder()
	backup, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "backup"}, func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()

	victim.Close() // unexpected disconnect, no ack sent
	testutil.WaitFor(t, 2*time.Second, "redelivery to backup", func() bool { return rec.Count(m.ID) == 1 })
	testutil.WaitFor(t, 2*time.Second, "consumer removed", func() bool { return b.Stats()["orders.inventory"].Consumers == 1 })
}

// A consumer that hangs (connected, but never acks) is handled by the ack timeout.
func TestAckTimeoutRedeliversHungMessage(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir())) // ack timeout 300ms
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hung, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "hung"}, func(d client.Delivery) error {
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hung.Close()
	m := testutil.Publish(t, b.Addr(), "corr-hang", 1)

	rec := testutil.NewRecorder()
	healthy, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "healthy"}, func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()

	testutil.WaitFor(t, 3*time.Second, "redelivery after ack timeout", func() bool { return rec.Count(m.ID) == 1 })
}

// Grade 8: a publisher that dies in the middle of a frame, or sends garbage, must not hurt the broker.
func TestPublisherDisconnectsUnexpectedly_BrokerKeepsWorking(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	rec := testutil.NewRecorder()
	s, err := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "i1"}, func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1) half a frame, then the connection dies
	c1, _ := net.Dial("tcp", b.Addr())
	c1.Write([]byte(`{"type":"publish","topic":"orders","message":{"id":"x"`))
	c1.Close()
	// 2) garbage
	c2, _ := net.Dial("tcp", b.Addr())
	c2.Write([]byte("not json at all\n"))
	c2.Close()
	// 3) a complete but invalid message (no correlation id) gets an error frame, not a crash
	c3, _ := net.Dial("tcp", b.Addr())
	json.NewEncoder(c3).Encode(protocol.Frame{Type: protocol.FramePublish, Topic: "orders", Message: &protocol.Message{ID: "bad"}})
	var reply protocol.Frame
	c3.SetReadDeadline(time.Now().Add(time.Second))
	json.NewDecoder(c3).Decode(&reply)
	c3.Close()
	if reply.Type != protocol.FrameError {
		t.Fatalf("invalid message must get an error frame, got %+v", reply)
	}

	good := testutil.Publish(t, b.Addr(), "corr-after-garbage", 1)
	testutil.WaitFor(t, 2*time.Second, "healthy flow continues", func() bool { return rec.Count(good.ID) == 1 })
	if rec.Len() != 1 {
		t.Fatalf("only the valid message may be delivered, got %d", rec.Len())
	}
}

// A publisher whose connection drops after publish: the message was confirmed, so it must still be delivered.
func TestPublisherDisconnectsAfterConfirm_MessageStillDelivered(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	b.Declare(protocol.TopicOrders, "inventory")
	m := testutil.Publish(t, b.Addr(), "corr-pub-gone", 1) // publisher closes right after the confirm

	rec := testutil.NewRecorder()
	s, _ := client.Subscribe(b.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "late"}, func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		return nil
	})
	defer s.Close()
	testutil.WaitFor(t, 2*time.Second, "delivery", func() bool { return rec.Count(m.ID) == 1 })
}
