package failure

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pad-lab2/src/consumers/common"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/tests/testutil"
)

func crashOnce(stage string) func(string, client.Delivery) bool {
	var fired atomic.Bool
	return func(s string, d client.Delivery) bool { return s == stage && fired.CompareAndSwap(false, true) }
}

// runCrashScenario: the consumer applies its local effect, then dies before
// ACK. A new process of the same group (new instance name, same effect file)
// gets the redelivery: deliveries belong to the group's queue, not to the
// process, so the restart never strands them.
func runCrashScenario(t *testing.T, dedup bool) (effects int, attempt int, redelivered bool) {
	t.Helper()
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	effectFile := filepath.Join(t.TempDir(), "effects.jsonl")

	store1, _ := common.OpenEffectStore(effectFile)
	p1 := &common.Inventory{Store: store1, Dedup: dedup, CrashAt: crashOnce(common.StageAfterEffect), Log: rpcx.Discard()}
	s1 := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "inventory-process-1"}, p1.Handle)
	m := testutil.Publish(t, conn, testutil.Order(t, "corr-crash", 99, "MDL"))

	select {
	case <-s1.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not crash")
	}
	if s1.Err() != client.ErrCrash || store1.Count(m.MessageId) != 1 {
		t.Fatalf("want a crash after the effect, got %v, effects=%d", s1.Err(), store1.Count(m.MessageId))
	}
	store1.Close()
	testutil.WaitFor(t, 2*time.Second, "message back in the queue", func() bool {
		s := b.Stats()["orders.inventory"]
		return s.Pending == 1 && s.Inflight == 0 && s.Consumers == 0
	})

	store2, _ := common.OpenEffectStore(effectFile)
	defer store2.Close()
	p2 := &common.Inventory{Store: store2, Dedup: dedup, Log: rpcx.Discard()}
	var mu sync.Mutex
	testutil.Subscribe(t, conn, "inventory", "inventory-process-2", func(ctx context.Context, d client.Delivery) error {
		mu.Lock()
		attempt, redelivered = d.Attempt, d.Redelivered
		mu.Unlock()
		return p2.Handle(ctx, d)
	})
	testutil.WaitFor(t, 2*time.Second, "redelivery acked", func() bool {
		return testutil.Idle(b, "orders.inventory") && b.Stats()["orders.inventory"].Consumers == 1
	})
	mu.Lock()
	defer mu.Unlock()
	return store2.Count(m.MessageId), attempt, redelivered
}

// At-least-once + idempotent consumer: the effect happens exactly once.
func TestCrashAfterEffectBeforeAck_WithDedup_EffectAppliedOnce(t *testing.T) {
	effects, attempt, redelivered := runCrashScenario(t, true)
	if effects != 1 || attempt != 2 || !redelivered {
		t.Fatalf("want 1 effect after a redelivery (attempt 2), got effects=%d attempt=%d redelivered=%v", effects, attempt, redelivered)
	}
}

// Without deduplication the same scenario applies the effect twice.
func TestCrashAfterEffectBeforeAck_WithoutDedup_EffectAppliedTwice(t *testing.T) {
	if effects, _, _ := runCrashScenario(t, false); effects != 2 {
		t.Fatalf("without dedup the effect must be applied twice, got %d", effects)
	}
}

// At-most-once: ACK before the effect; a crash in between loses the message.
func TestAckBeforeProcess_CrashLosesMessage_AtMostOnce(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	store, _ := common.OpenEffectStore(filepath.Join(t.TempDir(), "effects.jsonl"))
	defer store.Close()
	p := &common.Inventory{Store: store, Dedup: true, CrashAt: crashOnce(common.StageBeforeEffect), Log: rpcx.Discard()}
	s := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "amo", AckBeforeProcess: true}, p.Handle)
	m := testutil.Publish(t, conn, testutil.Order(t, "corr-amo", 1, "MDL"))
	<-s.Done()
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, conn, "inventory", "amo-2", rec.Ack)
	time.Sleep(200 * time.Millisecond)
	if store.Count(m.MessageId) != 0 || rec.Len() != 0 || !testutil.Idle(b, "orders.inventory") {
		t.Fatal("at-most-once: the message must be gone, with no effect and no redelivery")
	}
}

// Cancellation on the consumer side: closing the session (process stops,
// network drops) returns the in-flight delivery to the queue at once.
func TestConsumerSessionCancelled_InflightGoesToAnotherConsumer(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	conn := testutil.Dial(t, b.Addr)
	started := make(chan struct{})
	var once sync.Once
	victim := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "victim"},
		func(ctx context.Context, d client.Delivery) error {
			once.Do(func() { close(started) })
			<-ctx.Done() // the handler sees the cancellation
			return ctx.Err()
		})
	testutil.WaitFor(t, 2*time.Second, "victim subscribed", func() bool { return b.Stats()["orders.inventory"].Consumers == 1 })
	m := testutil.Publish(t, conn, testutil.Order(t, "corr-cancel", 1, "MDL"))
	<-started
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, conn, "inventory", "backup", rec.Ack)
	victim.Close()
	testutil.WaitFor(t, 2*time.Second, "redelivery to backup", func() bool { return rec.Count(m.MessageId) == 1 && testutil.Idle(b, "orders.inventory") })
}

// A slow consumer (processing longer than the ack timeout) causes a duplicate
// delivery; the idempotent consumer still applies the effect once.
func TestAckTimeoutDuplicateIsAbsorbedByDeduplication(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "") // ack timeout 300ms
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	store, _ := common.OpenEffectStore(filepath.Join(t.TempDir(), "effects.jsonl"))
	defer store.Close()
	slow := &common.Inventory{Store: store, Dedup: true, Delay: 500 * time.Millisecond, Log: rpcx.Discard()}
	fast := &common.Inventory{Store: store, Dedup: true, Log: rpcx.Discard()}
	testutil.Subscribe(t, conn, "inventory", "slow", slow.Handle)
	testutil.WaitFor(t, 2*time.Second, "slow subscribed", func() bool { return b.Stats()["orders.inventory"].Consumers == 1 })
	m := testutil.Publish(t, conn, testutil.Order(t, "corr-slow", 1, "MDL"))
	time.Sleep(50 * time.Millisecond)
	testutil.Subscribe(t, conn, "inventory", "fast", fast.Handle)
	testutil.WaitFor(t, 3*time.Second, "settled", func() bool { return testutil.Idle(b, "orders.inventory") })
	time.Sleep(600 * time.Millisecond) // let the slow consumer finish its late attempt
	if n := store.Count(m.MessageId); n != 1 {
		t.Fatalf("the effect must be applied once despite the duplicate delivery, got %d", n)
	}
}
