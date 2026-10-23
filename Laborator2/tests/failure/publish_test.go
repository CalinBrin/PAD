package failure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	brokerv1 "pad-lab2/gen/broker/v1"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/tests/testutil"
)

func losePublishResponses(n int64) *rpcx.LoseResponses {
	f := &rpcx.LoseResponses{Methods: map[string]bool{brokerv1.Broker_Publish_FullMethodName: true}}
	f.Remaining.Store(n)
	return f
}

// The ambiguity itself: the broker stored the message, but the answer was
// lost. The caller sees an error although the operation happened.
func TestLostPublishResponse_CallerCannotTellTheOutcome(t *testing.T) {
	f := losePublishResponses(1)
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "", f.Interceptor())
	b.Declare(envelope.TopicOrders, "inventory")
	c := brokerv1.NewBrokerClient(testutil.Dial(t, b.Addr))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: testutil.Order(t, "corr-amb", 1, "MDL")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("the caller must see DeadlineExceeded, got %v", err)
	}
	if s := b.Stats()["orders.inventory"]; s.Pending != 1 {
		t.Fatalf("yet the message was stored: want pending=1, got %+v", s)
	}
}

// CRITICAL SCENARIO (producer side): the answer of Publish is lost after the
// commit. The publisher retries with the same message_id, the broker answers
// duplicate=true, and the consumer receives the message exactly once.
func TestPublishResponseLostAfterCommit_RetryIsDeduplicated(t *testing.T) {
	f := losePublishResponses(1)
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "", f.Interceptor())
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	m := testutil.Order(t, "corr-lost-puback", 1, "MDL")
	res, err := client.NewPublisher(conn, testutil.FastPolicy, nil).Publish(context.Background(), envelope.TopicOrders, m)
	if err != nil {
		t.Fatalf("the retry must succeed: %v", err)
	}
	if !res.Duplicate {
		t.Fatal("the retry must be recognised as a duplicate of the stored message")
	}
	rec := testutil.NewRecorder()
	testutil.Subscribe(t, conn, "inventory", "i1", rec.Ack)
	testutil.WaitFor(t, 2*time.Second, "delivered", func() bool { return rec.Count(m.MessageId) == 1 && testutil.Idle(b, "orders.inventory") })
	time.Sleep(100 * time.Millisecond)
	if rec.Len() != 1 {
		t.Fatalf("exactly one delivery expected, got %d", rec.Len())
	}
}

// Feedback of the other team: a failed write must never be confirmed. The
// broker answers UNAVAILABLE (nothing stored), the publisher retries, and the
// message is stored once.
func TestStoreWriteFailure_IsReportedAndRetrySucceeds(t *testing.T) {
	var fail atomic.Int32
	fail.Store(1)
	cfg := testutil.FastConfig(t.TempDir())
	cfg.WriteFault = func(op string) error {
		if op == "enqueue" && fail.Add(-1) >= 0 {
			return errors.New("disk full (injected)")
		}
		return nil
	}
	b := testutil.StartBroker(t, cfg, "")
	b.Declare(envelope.TopicOrders, "inventory")
	conn := testutil.Dial(t, b.Addr)
	c := brokerv1.NewBrokerClient(conn)
	m := testutil.Order(t, "corr-disk", 1, "MDL")
	if _, err := c.Publish(context.Background(), &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: m}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a failed write must be reported as Unavailable, got %v", err)
	}
	if s := b.Stats()["orders.inventory"]; s.Pending != 0 {
		t.Fatalf("nothing may be stored after a failed write, got %+v", s)
	}
	if b.Ready() == nil {
		t.Fatal("readiness must report the storage failure")
	}
	res, err := client.NewPublisher(conn, testutil.FastPolicy, nil).Publish(context.Background(), envelope.TopicOrders, m)
	if err != nil || res.Duplicate {
		t.Fatalf("the retry must store the message (not claim a duplicate): %+v %v", res, err)
	}
	if s := b.Stats()["orders.inventory"]; s.Pending != 1 || b.Ready() != nil {
		t.Fatalf("one message expected and readiness restored, got %+v", s)
	}
}

// Deadline + cancellation: if the caller's deadline expires before the
// broker commits, the broker stores nothing.
func TestPublishDeadlineExpiresBeforeCommit_NothingStored(t *testing.T) {
	slow := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		time.Sleep(200 * time.Millisecond) // e.g. a congested server
		return handler(ctx, req)
	}
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "", slow)
	b.Declare(envelope.TopicOrders, "inventory")
	c := brokerv1.NewBrokerClient(testutil.Dial(t, b.Addr))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: testutil.Order(t, "corr-deadline", 1, "MDL")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if s := b.Stats()["orders.inventory"]; s.Pending != 0 {
		t.Fatalf("an expired request must not be committed, got %+v", s)
	}
}
