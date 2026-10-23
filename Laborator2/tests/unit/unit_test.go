package unit

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	brokerv1 "pad-lab2/gen/broker/v1"
	ordersv1 "pad-lab2/gen/orders/v1"
	"pad-lab2/src/consumers/billing/billing"
	"pad-lab2/src/consumers/common"
	"pad-lab2/src/integration-service/broker"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
)

func validEnvelope(t *testing.T) *brokerv1.Envelope {
	env, err := envelope.NewOrderCreated("m1", "c1", time.Now(), &ordersv1.OrderCreated{OrderId: "o1", CustomerId: "cu", AmountCents: 100, Currency: "MDL"})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestNewOrderCreatedHasAllMandatoryFields(t *testing.T) {
	env := validEnvelope(t)
	if env.MessageId != "m1" || env.CorrelationId != "c1" || env.MessageType != envelope.TypeOrderCreated ||
		env.SchemaVersion != envelope.OrderCreatedSchema || !strings.HasSuffix(env.OccurredAt, "Z") || len(env.Payload) == 0 {
		t.Fatalf("incomplete envelope: %+v", env)
	}
}

func TestValidateRejectsBadEnvelopes(t *testing.T) {
	cases := map[string]func(e *brokerv1.Envelope){
		"no id":             func(e *brokerv1.Envelope) { e.MessageId = "" },
		"no correlation id": func(e *brokerv1.Envelope) { e.CorrelationId = "" },
		"no type":           func(e *brokerv1.Envelope) { e.MessageType = "" },
		"schema 0":          func(e *brokerv1.Envelope) { e.SchemaVersion = 0 },
		"bad time":          func(e *brokerv1.Envelope) { e.OccurredAt = "yesterday" },
		"not UTC":           func(e *brokerv1.Envelope) { e.OccurredAt = "2026-10-23T20:00:00+03:00" },
	}
	for name, mutate := range cases {
		e := validEnvelope(t)
		mutate(e)
		if envelope.Validate(e) == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestDeriveIDIsStablePerRequest(t *testing.T) {
	a, b := envelope.DeriveID("orders", "req-1"), envelope.DeriveID("orders", "req-1")
	if a != b || len(a) != 32 {
		t.Fatalf("same request must give the same 32-hex id: %s %s", a, b)
	}
	if envelope.DeriveID("orders", "req-2") == a || envelope.DeriveID("other", "req-1") == a {
		t.Fatal("different requests or namespaces must give different ids")
	}
}

func TestSamePayloadEncodesToSameBytes(t *testing.T) {
	when := time.Date(2026, 10, 23, 17, 0, 0, 0, time.UTC)
	o := &ordersv1.OrderCreated{OrderId: "o1", CustomerId: "c", AmountCents: 5, Currency: "EUR"}
	a, _ := envelope.NewOrderCreated("m", "c", when, o)
	b, _ := envelope.NewOrderCreated("m", "c", when, o)
	if string(a.Payload) != string(b.Payload) || a.OccurredAt != b.OccurredAt {
		t.Fatal("a republished order must produce an identical envelope (the broker compares content)")
	}
}

func TestBackoffFullJitterIsBounded(t *testing.T) {
	p := rpcx.Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	for n := 1; n <= 10; n++ {
		window := p.BaseDelay << (n - 1)
		if window > p.MaxDelay {
			window = p.MaxDelay
		}
		for i := 0; i < 200; i++ {
			if d := p.Backoff(n); d < 0 || d > window {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", n, d, window)
			}
		}
	}
	distinct := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		distinct[p.Backoff(3)] = true
	}
	if len(distinct) < 10 {
		t.Fatal("delays must be randomised (jitter)")
	}
}

func TestBrokerBackoffGrowsAndIsCapped(t *testing.T) {
	if broker.Backoff(10*time.Millisecond, 1) != 10*time.Millisecond || broker.Backoff(10*time.Millisecond, 3) != 40*time.Millisecond {
		t.Fatal("backoff must double per attempt")
	}
	if broker.Backoff(time.Second, 50) > 30*time.Second {
		t.Fatal("backoff must be capped")
	}
}

func TestRetryableAndAmbiguousCodes(t *testing.T) {
	for _, c := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted} {
		if !rpcx.Retryable(status.Error(c, "")) {
			t.Errorf("%v must be retryable", c)
		}
	}
	for _, c := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists, codes.NotFound, codes.Unauthenticated, codes.Internal} {
		if rpcx.Retryable(status.Error(c, "")) {
			t.Errorf("%v must not be retried", c)
		}
	}
	if !rpcx.Ambiguous(status.Error(codes.DeadlineExceeded, "")) || rpcx.Ambiguous(status.Error(codes.InvalidArgument, "")) {
		t.Fatal("deadline exceeded is an unknown outcome, invalid argument is a known one")
	}
}

func TestRetryStopsAtMaxAttempts(t *testing.T) {
	calls := 0
	err := rpcx.Do(context.Background(), rpcx.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond}, nil, "op", rpcx.Retryable,
		func(ctx context.Context, attempt int) error { calls++; return status.Error(codes.Unavailable, "down") })
	if calls != 3 || !errors.Is(err, rpcx.ErrAttemptsExhausted) {
		t.Fatalf("want 3 calls and ErrAttemptsExhausted, got %d, %v", calls, err)
	}
}

func TestRetryDoesNotRepeatNonRetryableErrors(t *testing.T) {
	calls := 0
	err := rpcx.Do(context.Background(), rpcx.Policy{MaxAttempts: 5, BaseDelay: time.Millisecond}, nil, "op", rpcx.Retryable,
		func(ctx context.Context, attempt int) error {
			calls++
			return status.Error(codes.InvalidArgument, "bad")
		})
	if calls != 1 || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a non-retryable error must stop at once, got %d calls", calls)
	}
}

func TestRetryGivesEachAttemptADeadlineAndStopsOnParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := rpcx.Do(ctx, rpcx.Policy{MaxAttempts: 10, BaseDelay: time.Millisecond, PerAttemptTimeout: 20 * time.Millisecond}, nil, "op", rpcx.Retryable,
		func(actx context.Context, attempt int) error {
			calls++
			if _, ok := actx.Deadline(); !ok {
				t.Error("each attempt must have a deadline")
			}
			if attempt == 2 {
				cancel()
			}
			<-actx.Done()
			return status.FromContextError(actx.Err()).Err()
		})
	if calls != 2 || err == nil {
		t.Fatalf("cancelling the parent must stop retries, got %d calls, %v", calls, err)
	}
}

func TestEffectStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.jsonl")
	s, err := common.OpenEffectStore(path)
	if err != nil {
		t.Fatal(err)
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
	if !s2.Has("m1") || s2.Total() != 1 {
		t.Fatal("restarted store must remember applied effects (dedup key)")
	}
}

func TestBillingIdempotencyKeyDependsOnlyOnTheOrder(t *testing.T) {
	if billing.IdempotencyKey("ord-1") != billing.IdempotencyKey("ord-1") || billing.IdempotencyKey("ord-1") == billing.IdempotencyKey("ord-2") {
		t.Fatal("one order must map to exactly one charge key")
	}
}
