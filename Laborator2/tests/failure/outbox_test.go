package failure

import (
	"context"
	"testing"
	"time"

	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/order-service/service"
	"pad-lab2/tests/testutil"
)

var oneShot = rpcx.Policy{MaxAttempts: 2, BaseDelay: 10 * time.Millisecond, PerAttemptTimeout: 100 * time.Millisecond}

// Fallback of the order service: the broker is down when the order arrives.
// The order is recorded as pending (the client is told so, not lied to), and
// the outbox publishes it when the broker is back.
func TestBrokerDown_OrderKeptPendingAndPublishedByOutbox(t *testing.T) {
	addr := testutil.FreeAddr(t)
	svc, err := service.New(t.TempDir(), client.NewPublisher(testutil.Dial(t, addr), oneShot, nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	o, _, err := svc.Create(context.Background(), service.Input{RequestID: "req-outbox", CustomerID: "c1", AmountCents: 100, CorrelationID: "corr-outbox"})
	if err != nil || o.Status != service.StatusPending {
		t.Fatalf("broker down: the order must be recorded as pending, got %+v %v", o, err)
	}
	dir := t.TempDir()
	b := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	b.Declare(envelope.TopicOrders, "inventory")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunOutbox(ctx, 50*time.Millisecond)
	testutil.WaitFor(t, 10*time.Second, "published by the outbox", func() bool {
		got, _ := svc.Get(o.OrderID)
		return got.Status == service.StatusPublished
	})
	if s := b.Stats()["orders.inventory"]; s.Pending != 1 {
		t.Fatalf("one event expected, got %+v", s)
	}
}

// The publish answer is lost: the order stays pending, the outbox republishes
// with the same message ID, the broker recognises the duplicate: one event.
func TestLostPublishResponse_OutboxDoesNotDuplicateTheEvent(t *testing.T) {
	// The answers of both attempts of the request are lost: the first one was
	// committed, the second was recognised as a duplicate.
	f := losePublishResponses(2)
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "", f.Interceptor())
	b.Declare(envelope.TopicOrders, "inventory")
	svc, _ := service.New(t.TempDir(), client.NewPublisher(testutil.Dial(t, b.Addr), oneShot, nil), nil)
	o, _, _ := svc.Create(context.Background(), service.Input{RequestID: "req-lost", CustomerID: "c1", AmountCents: 100, CorrelationID: "corr-lost-outbox"})
	if o.Status != service.StatusPending {
		t.Fatalf("unknown outcome: the order must stay pending, got %s", o.Status)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunOutbox(ctx, 50*time.Millisecond)
	testutil.WaitFor(t, 5*time.Second, "published", func() bool {
		got, _ := svc.Get(o.OrderID)
		return got.Status == service.StatusPublished
	})
	if s := b.Stats()["orders.inventory"]; s.Pending != 1 {
		t.Fatalf("exactly one event expected in the queue, got %+v", s)
	}
}
