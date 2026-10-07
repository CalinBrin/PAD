package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"pad-lab1/src/api"
	"pad-lab1/src/integration-service/broker"
	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
	"pad-lab1/src/order-service/service"
	"pad-lab1/tests/testutil"
)

func subscribe(t *testing.T, addr, group, name string, h client.Handler) *client.Subscriber {
	t.Helper()
	s, err := client.Subscribe(addr, client.SubscribeOptions{Topic: protocol.TopicOrders, Group: group, Name: name}, h)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// Grade 5: one publisher, one subscriber, end to end.
func TestOnePublisherOneSubscriber(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	rec := testutil.NewRecorder()
	subscribe(t, b.Addr(), "inventory", "inv-1", func(d client.Delivery) error { rec.Add(d.Message.ID); return nil })

	m := testutil.Publish(t, b.Addr(), "corr-1", 10)

	testutil.WaitFor(t, 2*time.Second, "delivery", func() bool { return rec.Count(m.ID) == 1 })
	testutil.WaitFor(t, 2*time.Second, "ack", func() bool { return b.Stats()["orders.inventory"].Inflight == 0 })
}

// Every consumer group receives its own copy of each message.
func TestTwoGroupsEachGetEveryMessage(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	inv, bill := testutil.NewRecorder(), testutil.NewRecorder()
	subscribe(t, b.Addr(), "inventory", "i1", func(d client.Delivery) error { inv.Add(d.Message.ID); return nil })
	subscribe(t, b.Addr(), "billing", "b1", func(d client.Delivery) error { bill.Add(d.Message.ID); return nil })

	for i := 0; i < 5; i++ {
		testutil.Publish(t, b.Addr(), fmt.Sprintf("c%d", i), 1)
	}
	testutil.WaitFor(t, 2*time.Second, "both groups get 5", func() bool { return inv.Distinct() == 5 && bill.Distinct() == 5 })
}

// Grade 6: several publishers and several subscribers, concurrently.
// Within one group every message is processed by exactly one subscriber.
func TestManyPublishersManySubscribersConcurrently(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	rec := testutil.NewRecorder()
	handler := func(d client.Delivery) error { rec.Add(d.Message.ID); return nil }
	for i := 0; i < 4; i++ {
		subscribe(t, b.Addr(), "inventory", fmt.Sprintf("inv-%d", i), handler)
	}

	const publishers, perPublisher = 5, 40
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub, err := client.DialPublisher(b.Addr())
			if err != nil {
				t.Error(err)
				return
			}
			defer pub.Close()
			for i := 0; i < perPublisher; i++ {
				m, _ := protocol.NewMessage(protocol.TypeOrderCreated, "", protocol.OrderCreated{OrderID: "o", CustomerID: "c", Amount: 1})
				if _, err := pub.Publish(protocol.TopicOrders, m); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	total := publishers * perPublisher
	testutil.WaitFor(t, 5*time.Second, "all messages processed", func() bool { return rec.Distinct() == total })
	if rec.Len() != total {
		t.Fatalf("expected %d deliveries, got %d (duplicates without failures)", total, rec.Len())
	}
}

// Grade 7: publishers and subscribers are added and removed while traffic flows.
func TestDynamicSubscribersAndPublishers(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	rec := testutil.NewRecorder()
	handler := func(d client.Delivery) error { rec.Add(d.Message.ID); return nil }

	first := subscribe(t, b.Addr(), "inventory", "first", handler)
	testutil.WaitFor(t, time.Second, "1 consumer", func() bool { return b.Stats()["orders.inventory"].Consumers == 1 })

	var published int
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	startPublisher := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub, err := client.DialPublisher(b.Addr())
			if err != nil {
				return
			}
			defer pub.Close()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m, _ := protocol.NewMessage(protocol.TypeOrderCreated, "", protocol.OrderCreated{OrderID: "o", CustomerID: "c", Amount: 1})
				if _, err := pub.Publish(protocol.TopicOrders, m); err == nil {
					mu.Lock()
					published++
					mu.Unlock()
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	startPublisher()

	second := subscribe(t, b.Addr(), "inventory", "second", handler) // added at runtime
	startPublisher()                                                 // added at runtime
	testutil.WaitFor(t, time.Second, "2 consumers", func() bool { return b.Stats()["orders.inventory"].Consumers == 2 })

	time.Sleep(100 * time.Millisecond)
	first.Close() // removed at runtime
	testutil.WaitFor(t, time.Second, "1 consumer left", func() bool { return b.Stats()["orders.inventory"].Consumers == 1 })
	time.Sleep(100 * time.Millisecond)

	close(stop)
	wg.Wait()
	mu.Lock()
	want := published
	mu.Unlock()
	testutil.WaitFor(t, 5*time.Second, "all published messages processed", func() bool { return rec.Distinct() == want })
	_ = second
}

// Retry and DLQ: a message that always fails is retried and then dead-lettered.
func TestRetryThenDeadLetter(t *testing.T) {
	cfg := testutil.FastConfig(t.TempDir())
	cfg.MaxAttempts = 3
	b := testutil.StartBroker(t, cfg)
	rec := testutil.NewRecorder()
	subscribe(t, b.Addr(), "billing", "b1", func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		return errors.New("always fails")
	})

	m := testutil.Publish(t, b.Addr(), "poison-1", -1)

	testutil.WaitFor(t, 3*time.Second, "message in DLQ", func() bool { return b.Stats()["orders.billing"].DLQ == 1 })
	if got := rec.Count(m.ID); got != 3 {
		t.Fatalf("expected exactly MaxAttempts=3 deliveries, got %d", got)
	}
	dlq := b.DLQ("orders.billing")
	if len(dlq) != 1 || dlq[0].Message.ID != m.ID || dlq[0].Attempts != 3 || dlq[0].Reason != "always fails" {
		t.Fatalf("unexpected DLQ content: %+v", dlq)
	}
	if s := b.Stats()["orders.billing"]; s.Pending != 0 || s.Inflight != 0 {
		t.Fatalf("queue must be empty after DLQ: %+v", s)
	}
}

// A transient failure is retried and eventually succeeds (no DLQ).
func TestTransientFailureIsRetried(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	rec := testutil.NewRecorder()
	subscribe(t, b.Addr(), "inventory", "i1", func(d client.Delivery) error {
		rec.Add(d.Message.ID)
		if d.Attempt < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	m := testutil.Publish(t, b.Addr(), "c", 1)
	testutil.WaitFor(t, 3*time.Second, "success on attempt 3", func() bool {
		return rec.Count(m.ID) == 3 && b.Stats()["orders.inventory"].Inflight == 0 && b.Stats()["orders.inventory"].Retrying == 0
	})
	if b.Stats()["orders.inventory"].DLQ != 0 {
		t.Fatal("message must not be dead-lettered")
	}
}

// Grade 9: messages are persisted; undelivered messages are retransmitted after a broker restart.
func TestPersistenceUndeliveredMessagesSurviveBrokerRestart(t *testing.T) {
	dir := t.TempDir()
	b1, err := broker.New(testutil.FastConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := b1.Declare(protocol.TopicOrders, "inventory"); err != nil { // queue exists, no consumer yet
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, testutil.Publish(t, b1.Addr(), fmt.Sprintf("c%d", i), 1).ID)
	}
	b1.Close() // "crash": nothing was ever delivered

	b2 := testutil.StartBroker(t, testutil.FastConfig(dir))
	rec := testutil.NewRecorder()
	subscribe(t, b2.Addr(), "inventory", "i1", func(d client.Delivery) error { rec.Add(d.Message.ID); return nil })

	testutil.WaitFor(t, 2*time.Second, "retransmission after restart", func() bool { return rec.Distinct() == 3 })
	for _, id := range ids {
		if rec.Count(id) != 1 {
			t.Fatalf("message %s delivered %d times", id, rec.Count(id))
		}
	}
	// FIFO per queue for first deliveries survives the restart.
	got := rec.IDs()
	for i := range ids {
		if got[i] != ids[i] {
			t.Fatalf("order not preserved: want %v got %v", ids, got)
		}
	}
}

// Acknowledged messages are not redelivered after a restart.
func TestPersistenceAckedMessagesStayAcked(t *testing.T) {
	dir := t.TempDir()
	b1, _ := broker.New(testutil.FastConfig(dir))
	b1.Listen("127.0.0.1:0")
	rec := testutil.NewRecorder()
	s, err := client.Subscribe(b1.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "inventory", Name: "i1"}, func(d client.Delivery) error { rec.Add(d.Message.ID); return nil })
	if err != nil {
		t.Fatal(err)
	}
	m := testutil.Publish(t, b1.Addr(), "c", 1)
	testutil.WaitFor(t, 2*time.Second, "processed and acked", func() bool { return rec.Count(m.ID) == 1 && b1.Stats()["orders.inventory"].Inflight == 0 })
	s.Close()
	b1.Close()

	b2 := testutil.StartBroker(t, testutil.FastConfig(dir))
	rec2 := testutil.NewRecorder()
	subscribe(t, b2.Addr(), "inventory", "i1", func(d client.Delivery) error { rec2.Add(d.Message.ID); return nil })
	time.Sleep(200 * time.Millisecond)
	if rec2.Len() != 0 {
		t.Fatalf("acked message was redelivered after restart")
	}
}

// DLQ content is persistent too.
func TestPersistenceDLQSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testutil.FastConfig(dir)
	cfg.MaxAttempts = 1
	b1, _ := broker.New(cfg)
	b1.Listen("127.0.0.1:0")
	s, _ := client.Subscribe(b1.Addr(), client.SubscribeOptions{Topic: protocol.TopicOrders, Group: "billing", Name: "b1"}, func(client.Delivery) error { return errors.New("bad") })
	testutil.Publish(t, b1.Addr(), "c", -5)
	testutil.WaitFor(t, 2*time.Second, "dlq", func() bool { return b1.Stats()["orders.billing"].DLQ == 1 })
	s.Close()
	b1.Close()

	b2 := testutil.StartBroker(t, cfg)
	if n := len(b2.DLQ("orders.billing")); n != 1 {
		t.Fatalf("DLQ must survive restart, got %d entries", n)
	}
}

// End to end through the HTTP API: correlation id travels from the request to the consumer.
func TestHTTPAPIEndToEndKeepsCorrelationID(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()))
	var mu sync.Mutex
	var got protocol.Message
	done := make(chan struct{}, 1)
	subscribe(t, b.Addr(), "inventory", "i1", func(d client.Delivery) error {
		mu.Lock()
		got = d.Message
		mu.Unlock()
		done <- struct{}{}
		return nil
	})
	svc := service.New(b.Addr(), nil)
	defer svc.Close()
	srv := httptest.NewServer(api.NewHandler(svc))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/orders", bytes.NewBufferString(`{"customer_id":"c1","amount":42.5}`))
	req.Header.Set(api.CorrelationHeader, "my-correlation-id")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var res api.OrderResult
	json.NewDecoder(resp.Body).Decode(&res)
	if res.CorrelationID != "my-correlation-id" || res.Routed != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not receive the message")
	}
	mu.Lock()
	defer mu.Unlock()
	if got.CorrelationID != "my-correlation-id" || got.ID != res.MessageID {
		t.Fatalf("correlation lost: %+v", got)
	}
}

func TestHTTPAPIValidationAndBrokerDown(t *testing.T) {
	svc := service.New("127.0.0.1:1", nil) // nothing listens there
	defer svc.Close()
	srv := httptest.NewServer(api.NewHandler(svc))
	defer srv.Close()

	resp, _ := http.Post(srv.URL+"/orders", "application/json", bytes.NewBufferString(`{"amount":1}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing customer_id must be 400, got %d", resp.StatusCode)
	}
	resp, _ = http.Post(srv.URL+"/orders", "application/json", bytes.NewBufferString(`{"customer_id":"c","amount":1}`))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("broker down must be 503, got %d", resp.StatusCode)
	}
}
