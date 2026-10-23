package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	brokerv1 "pad-lab2/gen/broker/v1"
	"pad-lab2/src/api"
	"pad-lab2/src/consumers/billing/billing"
	"pad-lab2/src/consumers/common"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/order-service/service"
	"pad-lab2/src/payment-service/payment"
	"pad-lab2/tests/testutil"
)

// Grade 5: one publisher, one subscriber, over gRPC.
func TestOnePublisherOneSubscriber(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	conn := testutil.Dial(t, b.Addr)
	rec := testutil.NewRecorder()
	var got *brokerv1.Envelope
	var mu sync.Mutex
	testutil.Subscribe(t, conn, "inventory", "i1", func(ctx context.Context, d client.Delivery) error {
		mu.Lock()
		got = d.Message
		mu.Unlock()
		return rec.Ack(ctx, d)
	})
	testutil.WaitFor(t, 2*time.Second, "subscribed", func() bool { return b.Stats()["orders.inventory"].Consumers == 1 })
	m := testutil.Order(t, "corr-5", 100, "MDL")
	if res := testutil.Publish(t, conn, m); res.RoutedQueues != 1 || res.Duplicate {
		t.Fatalf("unexpected publish response %+v", res)
	}
	testutil.WaitFor(t, 2*time.Second, "delivery and ack", func() bool { return rec.Count(m.MessageId) == 1 && testutil.Idle(b, "orders.inventory") })
	mu.Lock()
	defer mu.Unlock()
	if got.CorrelationId != "corr-5" || got.SchemaVersion != envelope.OrderCreatedSchema || got.OccurredAt != m.OccurredAt {
		t.Fatalf("envelope changed in transit: %+v", got)
	}
}

func TestTwoGroupsEachGetEveryMessage(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	conn := testutil.Dial(t, b.Addr)
	inv, bill := testutil.NewRecorder(), testutil.NewRecorder()
	testutil.Subscribe(t, conn, "inventory", "i1", inv.Ack)
	testutil.Subscribe(t, conn, "billing", "b1", bill.Ack)
	testutil.WaitFor(t, 2*time.Second, "subscribed", func() bool { return len(b.Stats()) == 2 })
	for i := 0; i < 5; i++ {
		testutil.Publish(t, conn, testutil.Order(t, "c", 10, "MDL"))
	}
	testutil.WaitFor(t, 2*time.Second, "both groups got all", func() bool { return inv.Distinct() == 5 && bill.Distinct() == 5 })
}

// Grade 6: several publishers and subscribers at the same time.
func TestManyPublishersManySubscribersConcurrently(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	rec := testutil.NewRecorder()
	for i := 0; i < 4; i++ {
		testutil.Subscribe(t, testutil.Dial(t, b.Addr), "inventory", fmt.Sprintf("i%d", i), rec.Ack)
	}
	const publishers, each = 5, 20
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := testutil.Dial(t, b.Addr)
			for i := 0; i < each; i++ {
				testutil.Publish(t, conn, testutil.Order(t, "c", 1, "MDL"))
			}
		}()
	}
	wg.Wait()
	testutil.WaitFor(t, 5*time.Second, "all messages processed once", func() bool {
		return rec.Distinct() == publishers*each && testutil.Idle(b, "orders.inventory")
	})
	if rec.Len() != publishers*each {
		t.Fatalf("no failure was injected, so no redelivery is expected: %d deliveries for %d messages", rec.Len(), publishers*each)
	}
}

// Grade 7: subscribers and publishers join and leave while messages flow.
func TestDynamicSubscribersAndPublishers(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	rec := testutil.NewRecorder()
	ctx, stop := context.WithCancel(context.Background())
	var pubWG sync.WaitGroup
	var mu sync.Mutex
	want := map[string]bool{}
	for p := 0; p < 3; p++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for ctx.Err() == nil {
				conn, _ := client.Dial(b.Addr) // a new publisher connection each round
				for i := 0; i < 3; i++ {
					m := testutil.Order(t, "c", 1, "MDL")
					if _, err := client.NewPublisher(conn, testutil.FastPolicy, nil).Publish(context.Background(), envelope.TopicOrders, m); err == nil {
						mu.Lock()
						want[m.MessageId] = true
						mu.Unlock()
					}
				}
				conn.Close()
			}
		}()
	}
	conn := testutil.Dial(t, b.Addr)
	for round := 0; round < 6; round++ {
		s1 := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "dyn-a"}, rec.Ack)
		s2 := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "dyn-b"}, rec.Ack)
		time.Sleep(20 * time.Millisecond)
		s1.Close()
		time.Sleep(10 * time.Millisecond)
		s2.Close()
	}
	stop()
	pubWG.Wait()
	testutil.Subscribe(t, conn, "inventory", "final", rec.Ack)
	mu.Lock()
	n := len(want)
	mu.Unlock()
	testutil.WaitFor(t, 5*time.Second, "every published message processed", func() bool { return rec.Distinct() >= n && testutil.Idle(b, "orders.inventory") })
	mu.Lock()
	defer mu.Unlock()
	for id := range want {
		if rec.Count(id) == 0 {
			t.Fatalf("message %s lost", id)
		}
	}
}

// Load balancing: competing consumers of a group share the messages (round-robin, prefetch 1).
func TestLoadBalancingAcrossConsumersOfAGroup(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "billing")
	conn := testutil.Dial(t, b.Addr)
	recs := []*testutil.Recorder{testutil.NewRecorder(), testutil.NewRecorder(), testutil.NewRecorder()}
	for i, r := range recs {
		r := r
		testutil.Subscribe(t, conn, "billing", fmt.Sprintf("billing-%d", i+1), func(ctx context.Context, d client.Delivery) error {
			time.Sleep(5 * time.Millisecond) // some work, so the others get a share
			return r.Ack(ctx, d)
		})
	}
	testutil.WaitFor(t, 2*time.Second, "3 consumers", func() bool { return b.Stats()["orders.billing"].Consumers == 3 })
	const n = 60
	for i := 0; i < n; i++ {
		testutil.Publish(t, conn, testutil.Order(t, "c", 1, "MDL"))
	}
	testutil.WaitFor(t, 5*time.Second, "all processed", func() bool {
		return recs[0].Len()+recs[1].Len()+recs[2].Len() == n && testutil.Idle(b, "orders.billing")
	})
	for i, r := range recs {
		if r.Len() < n/6 {
			t.Errorf("consumer %d got %d of %d messages: load is not balanced", i+1, r.Len(), n)
		}
		t.Logf("billing-%d processed %d messages", i+1, r.Len())
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	conn := testutil.Dial(t, b.Addr)
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, conn, "billing", "b1", func(ctx context.Context, d client.Delivery) error {
		rec.Add(d.Message.MessageId)
		return errors.New("always fails")
	})
	testutil.WaitFor(t, 2*time.Second, "subscribed", func() bool { return b.Stats()["orders.billing"].Consumers == 1 })
	m := testutil.Order(t, "poison", 1, "MDL")
	testutil.Publish(t, conn, m)
	testutil.WaitFor(t, 3*time.Second, "message in DLQ", func() bool { return b.Stats()["orders.billing"].DLQ == 1 })
	if got := rec.Count(m.MessageId); got != 3 {
		t.Fatalf("want MaxAttempts=3 deliveries, got %d", got)
	}
	dlq := b.DLQ("orders.billing")
	if len(dlq) != 1 || dlq[0].Attempts != 3 || dlq[0].Reason != "always fails" || dlq[0].Permanent {
		t.Fatalf("unexpected DLQ: %+v", dlq)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	conn := testutil.Dial(t, b.Addr)
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, conn, "billing", "b1", func(ctx context.Context, d client.Delivery) error {
		rec.Add(d.Message.MessageId)
		return client.Permanent(errors.New("currency not supported"))
	})
	testutil.WaitFor(t, 2*time.Second, "subscribed", func() bool { return b.Stats()["orders.billing"].Consumers == 1 })
	m := testutil.Order(t, "perm", 1, "GBP")
	testutil.Publish(t, conn, m)
	testutil.WaitFor(t, 2*time.Second, "DLQ", func() bool { return b.Stats()["orders.billing"].DLQ == 1 })
	time.Sleep(100 * time.Millisecond)
	if rec.Count(m.MessageId) != 1 || !b.DLQ("orders.billing")[0].Permanent {
		t.Fatalf("permanent error must be delivered once and dead-lettered as permanent, got %d", rec.Count(m.MessageId))
	}
}

// A retried message goes to a healthy consumer, not back to the one that failed it.
func TestRetryPrefersAnotherConsumer(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	conn := testutil.Dial(t, b.Addr)
	bad, good := testutil.NewRecorder(), testutil.NewRecorder()
	testutil.Subscribe(t, conn, "billing", "bad", func(ctx context.Context, d client.Delivery) error {
		bad.Add(d.Message.MessageId)
		return errors.New("this instance is broken")
	})
	testutil.WaitFor(t, 2*time.Second, "bad subscribed", func() bool { return b.Stats()["orders.billing"].Consumers == 1 })
	testutil.Subscribe(t, conn, "billing", "good", func(ctx context.Context, d client.Delivery) error {
		time.Sleep(150 * time.Millisecond)
		return good.Ack(ctx, d)
	})
	testutil.WaitFor(t, 2*time.Second, "good subscribed", func() bool { return b.Stats()["orders.billing"].Consumers == 2 })
	m1 := testutil.Order(t, "r1", 1, "MDL")
	m2 := testutil.Order(t, "r2", 1, "MDL")
	testutil.Publish(t, conn, m1) // round-robin: to "bad"
	testutil.Publish(t, conn, m2) // to "good"
	testutil.WaitFor(t, 3*time.Second, "both processed by good", func() bool { return good.Count(m1.MessageId) == 1 && good.Count(m2.MessageId) == 1 })
	if bad.Count(m1.MessageId) != 1 || b.Stats()["orders.billing"].DLQ != 0 {
		t.Fatalf("the failed message went back to the broken consumer %d times", bad.Count(m1.MessageId))
	}
}

// Publish is idempotent by message_id: a repeated publish stores nothing new.
func TestPublishIsIdempotentByMessageID(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	m := testutil.Order(t, "dup", 1, "MDL")
	for i := 0; i < 3; i++ {
		res := testutil.Publish(t, conn, m)
		if res.Duplicate != (i > 0) {
			t.Fatalf("publish %d: duplicate=%v", i+1, res.Duplicate)
		}
	}
	if s := b.Stats()["orders.inventory"]; s.Pending != 1 {
		t.Fatalf("one message expected in the queue, got %+v", s)
	}
}

// Feedback of the other team: a message published while no consumer is
// connected must not be lost; it waits in the durable queue of the group.
func TestMessagePublishedWithoutConnectedConsumerIsDeliveredLater(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	m := testutil.Order(t, "early", 1, "MDL")
	if res := testutil.Publish(t, conn, m); res.RoutedQueues != 1 {
		t.Fatalf("the message must be routed to the declared queue, got %+v", res)
	}
	time.Sleep(50 * time.Millisecond)
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, conn, "inventory", "late", rec.Ack)
	testutil.WaitFor(t, 2*time.Second, "late delivery", func() bool { return rec.Count(m.MessageId) == 1 })
}

// A topic without any queue is refused explicitly: nothing is stored silently.
func TestPublishToTopicWithoutQueueIsRefused(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	_, err := client.NewPublisher(testutil.Dial(t, b.Addr), testutil.FastPolicy, nil).Publish(context.Background(), envelope.TopicOrders, testutil.Order(t, "c", 1, "MDL"))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

// Grade 9: undelivered messages survive a broker restart and are retransmitted.
func TestPersistenceUndeliveredMessagesSurviveBrokerRestart(t *testing.T) {
	dir, addr := t.TempDir(), testutil.FreeAddr(t)
	b1 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	b1.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, addr)
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, testutil.Publish(t, conn, testutil.Order(t, fmt.Sprintf("c%d", i), 1, "MDL")).MessageId)
	}
	b1.Stop()
	b2 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, testutil.Dial(t, addr), "inventory", "i1", rec.Ack)
	testutil.WaitFor(t, 3*time.Second, "retransmission after restart", func() bool { return rec.Distinct() == 3 && testutil.Idle(b2, "orders.inventory") })
	got := rec.IDs()
	for i := range ids {
		if got[i] != ids[i] {
			t.Fatalf("FIFO order not preserved: want %v got %v", ids, got)
		}
	}
}

// The deduplication window survives a restart, even after the message was acked.
func TestPersistenceDeduplicationSurvivesRestart(t *testing.T) {
	dir, addr := t.TempDir(), testutil.FreeAddr(t)
	b1 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	b1.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, addr)
	rec := testutil.NewRecorder()
	s := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "i1"}, rec.Ack)
	m := testutil.Order(t, "dedup-restart", 1, "MDL")
	testutil.Publish(t, conn, m)
	testutil.WaitFor(t, 2*time.Second, "acked", func() bool { return rec.Count(m.MessageId) == 1 && testutil.Idle(b1, "orders.inventory") })
	s.Close()
	b1.Stop()
	b2 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	if res := testutil.Publish(t, conn, m); !res.Duplicate {
		t.Fatal("a publish repeated after a broker restart must still be recognised as a duplicate")
	}
	if s := b2.Stats()["orders.inventory"]; s.Pending != 0 {
		t.Fatalf("nothing must be enqueued again, got %+v", s)
	}
}

func TestPersistenceDLQSurvivesRestart(t *testing.T) {
	dir, addr := t.TempDir(), testutil.FreeAddr(t)
	b1 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	conn := testutil.Dial(t, addr)
	s := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "billing", Name: "b1"},
		func(ctx context.Context, d client.Delivery) error { return client.Permanent(errors.New("bad order")) })
	testutil.WaitFor(t, 2*time.Second, "subscribed", func() bool { return b1.Stats()["orders.billing"].Consumers == 1 })
	m := testutil.Order(t, "dlq-restart", 1, "MDL")
	testutil.Publish(t, conn, m)
	testutil.WaitFor(t, 2*time.Second, "DLQ", func() bool { return b1.Stats()["orders.billing"].DLQ == 1 })
	s.Close()
	b1.Stop()
	b2 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	if dlq := b2.DLQ("orders.billing"); len(dlq) != 1 || dlq[0].Message.ID != m.MessageId || !dlq[0].Permanent {
		t.Fatalf("DLQ lost after restart: %+v", dlq)
	}
}

// A subscriber with Reconnect keeps working across a broker restart.
func TestSubscriberReconnectsAfterBrokerRestart(t *testing.T) {
	dir, addr := t.TempDir(), testutil.FreeAddr(t)
	b1 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	conn := testutil.Dial(t, addr)
	rec := testutil.NewRecorder()
	s := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: "inventory", Name: "i1", Reconnect: true}, rec.Ack)
	defer s.Close()
	testutil.WaitFor(t, 2*time.Second, "subscribed", func() bool { return b1.Stats()["orders.inventory"].Consumers == 1 })
	b1.Stop()
	b2 := testutil.StartBroker(t, testutil.FastConfig(dir), addr)
	testutil.WaitFor(t, 10*time.Second, "resubscribed", func() bool { return b2.Stats()["orders.inventory"].Consumers == 1 })
	m := testutil.Order(t, "after-restart", 1, "MDL")
	testutil.Publish(t, conn, m)
	testutil.WaitFor(t, 2*time.Second, "delivery after reconnect", func() bool { return rec.Count(m.MessageId) == 1 })
}

// End to end: HTTP API -> order service -> broker -> inventory + billing ->
// payment service, with the same correlation ID everywhere.
func TestEndToEndOrderFlowKeepsCorrelationID(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	b.Declare(envelope.TopicOrders, "billing")
	p := testutil.StartPayment(t, payment.Config{DataDir: t.TempDir()}, "")
	brokerConn := testutil.Dial(t, b.Addr)

	svc, err := service.New(t.TempDir(), client.NewPublisher(brokerConn, testutil.FastPolicy, nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewHandler(svc, func() string { return "ready" }, nil))
	defer srv.Close()

	store, _ := common.OpenEffectStore(t.TempDir() + "/effects.jsonl")
	defer store.Close()
	inv := &common.Inventory{Store: store, Dedup: true, Log: rpcx.Discard()}
	testutil.Subscribe(t, brokerConn, "inventory", "inv-1", inv.Handle)
	outcomes := make(chan billing.Outcome, 4)
	bill := &billing.Processor{Payment: testutil.PaymentClient(t, p.Addr), Policy: testutil.FastPolicy, Currencies: billing.DefaultCurrencies,
		Log: rpcx.Discard(), OnOutcome: func(o billing.Outcome) { outcomes <- o }}
	testutil.Subscribe(t, brokerConn, "billing", "bill-1", bill.Handle)

	req, _ := http.NewRequest("POST", srv.URL+"/orders", bytes.NewBufferString(`{"customer_id":"c42","amount_cents":12345,"currency":"EUR"}`))
	req.Header.Set(api.IdempotencyHeader, "e2e-1")
	req.Header.Set(api.CorrelationHeader, "corr-e2e")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var order service.Order
	json.NewDecoder(res.Body).Decode(&order)
	res.Body.Close()
	if res.StatusCode != 202 || order.Status != service.StatusPublished {
		t.Fatalf("want 202 published, got %d %+v", res.StatusCode, order)
	}
	select {
	case o := <-outcomes:
		if o.Result != "approved" || o.OrderID != order.OrderID {
			t.Fatalf("unexpected billing outcome %+v", o)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("billing did not charge the order")
	}
	testutil.WaitFor(t, 2*time.Second, "inventory effect", func() bool { return store.Has(order.MessageID) })
	charges := p.Charges()
	if len(charges) != 1 || charges[0].OrderID != order.OrderID || charges[0].CorrelationID != "corr-e2e" || charges[0].AmountCents != 12345 {
		t.Fatalf("the payment service must see one charge with the propagated correlation ID: %+v", charges)
	}
	testutil.WaitFor(t, 2*time.Second, "queues idle", func() bool { return testutil.Idle(b, "orders.inventory") && testutil.Idle(b, "orders.billing") })
}

// The gRPC health service answers SERVING (liveness of the broker process).
func TestHealthServiceAnswers(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	res, err := healthpb.NewHealthClient(testutil.Dial(t, b.Addr)).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil || res.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health check: %v %v", res, err)
	}
	if err := b.Ready(); err != nil {
		t.Fatalf("broker must be ready: %v", err)
	}
}
