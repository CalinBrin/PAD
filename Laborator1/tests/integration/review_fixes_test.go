package integration

import (
	"errors"
	"testing"
	"time"

	"pad-lab1/src/integration-service/broker"
	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
	"pad-lab1/tests/testutil"
)

func publishTwice(t *testing.T, addr string) protocol.Message {
	t.Helper()
	m, err := protocol.NewMessage(protocol.TypeOrderCreated, "corr-dup", protocol.OrderCreated{OrderID: "ord-dup", CustomerID: "c1", Amount: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err := client.DialPublisher(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 2; i++ { // e.g. a producer that retries after a lost puback
		if _, err := p.Publish(protocol.TopicOrders, m); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

// Fix 1: two messages with the same ID are two deliveries with different
// tags. The consumer that got the first copy can ACK it and keeps receiving.
func TestDuplicateMessageIDDoesNotBlockConsumer(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	recA, recB := testutil.NewRecorder(), testutil.NewRecorder()
	subscribe(t, b.Addr(), "inventory", "A", func(d client.Delivery) error { recA.Add(d.Message.ID); return nil })
	subscribe(t, b.Addr(), "inventory", "B", func(d client.Delivery) error { recB.Add(d.Message.ID); return nil })

	m := publishTwice(t, b.Addr())
	testutil.WaitFor(t, 2*time.Second, "both copies acked", func() bool {
		s := b.Stats()["orders.inventory"]
		return recA.Count(m.ID)+recB.Count(m.ID) == 2 && s.Pending == 0 && s.Inflight == 0
	})

	// Both consumers must still have free capacity: new messages reach both.
	for i := 0; i < 4; i++ {
		testutil.Publish(t, b.Addr(), "corr-after-dup", 1)
	}
	testutil.WaitFor(t, 2*time.Second, "new messages delivered and acked", func() bool {
		s := b.Stats()["orders.inventory"]
		return recA.Len()+recB.Len() == 6 && s.Pending == 0 && s.Inflight == 0
	})
	if recA.Len() < 2 || recB.Len() < 2 {
		t.Fatalf("a consumer stopped receiving after the duplicate: A=%d B=%d", recA.Len(), recB.Len())
	}
}

// Fix 1, persistence: each copy is acked separately in the log as well.
func TestDuplicateMessageIDSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	b1, err := broker.New(testutil.FastConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	b1.Declare(protocol.TopicOrders, "inventory")
	m := publishTwice(t, b1.Addr())
	b1.Close()

	b2, err := broker.New(testutil.FastConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if got := b2.Stats()["orders.inventory"].Pending; got != 2 {
		t.Fatalf("both copies must be recovered, got %d", got)
	}
	rec := testutil.NewRecorder()
	s, err := client.Subscribe(b2.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "i1"}, func(d client.Delivery) error { rec.Add(d.Message.ID); return nil })
	if err != nil {
		t.Fatal(err)
	}
	testutil.WaitFor(t, 2*time.Second, "both copies acked", func() bool {
		st := b2.Stats()["orders.inventory"]
		return rec.Count(m.ID) == 2 && st.Pending == 0 && st.Inflight == 0
	})
	s.Close()
	b2.Close()

	b3 := testutil.StartBroker(t, testutil.FastConfig(dir))
	if got := b3.Stats()["orders.inventory"].Pending; got != 0 {
		t.Fatalf("acked copies must not come back after restart, got %d pending", got)
	}
}

// Fix 2: a retried message is not handed back to the consumer that just
// failed it while a healthy consumer exists, even if that consumer is busy.
func TestRetryPrefersAnotherConsumer(t *testing.T) {
	cfg := testutil.FastConfig(t.TempDir()) // retry base 10ms, ack timeout 300ms
	b := testutil.StartBroker(t, cfg)
	recBad, recGood := testutil.NewRecorder(), testutil.NewRecorder()
	subscribe(t, b.Addr(), "billing", "bad", func(d client.Delivery) error {
		recBad.Add(d.Message.ID)
		return errors.New("this instance is broken")
	})
	subscribe(t, b.Addr(), "billing", "good", func(d client.Delivery) error {
		time.Sleep(150 * time.Millisecond) // busy while the retry becomes due
		recGood.Add(d.Message.ID)
		return nil
	})

	m1 := testutil.Publish(t, b.Addr(), "corr-r1", 1) // round-robin: to "bad"
	m2 := testutil.Publish(t, b.Addr(), "corr-r2", 1) // to "good"

	testutil.WaitFor(t, 3*time.Second, "both messages processed by the healthy consumer", func() bool {
		return recGood.Count(m1.ID) == 1 && recGood.Count(m2.ID) == 1
	})
	if n := recBad.Count(m1.ID); n != 1 {
		t.Fatalf("the failed message went back to the broken consumer %d times", n)
	}
	if s := b.Stats()["orders.billing"]; s.DLQ != 0 {
		t.Fatalf("message must not be dead-lettered while a healthy consumer exists: %+v", s)
	}
}

// Fix 4: a permanent error goes to the DLQ after one delivery, no retries.
func TestPermanentErrorIsNotRetried(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	rec := testutil.NewRecorder()
	subscribe(t, b.Addr(), "billing", "b1", func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		return client.Permanent(errors.New("cannot invoice a non-positive amount"))
	})
	m := testutil.Publish(t, b.Addr(), "corr-perm", -5)

	testutil.WaitFor(t, 2*time.Second, "message in DLQ", func() bool { return b.Stats()["orders.billing"].DLQ == 1 })
	time.Sleep(100 * time.Millisecond) // a retry (10ms backoff) would show up by now
	if got := rec.Count(m.ID); got != 1 {
		t.Fatalf("permanent error must not be retried, got %d deliveries", got)
	}
	dlq := b.DLQ("orders.billing")
	if len(dlq) != 1 || dlq[0].Attempts != 1 || !dlq[0].Permanent {
		t.Fatalf("unexpected DLQ entry: %+v", dlq)
	}
	if s := b.Stats()["orders.billing"]; s.Pending != 0 || s.Inflight != 0 || s.Retrying != 0 {
		t.Fatalf("queue must be empty: %+v", s)
	}
}
