package failure

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentv1 "pad-lab2/gen/payment/v1"
	"pad-lab2/src/consumers/billing/billing"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/payment-service/payment"
	"pad-lab2/tests/testutil"
)

type outcomes struct {
	mu   sync.Mutex
	list []billing.Outcome
}

func (o *outcomes) add(x billing.Outcome) { o.mu.Lock(); o.list = append(o.list, x); o.mu.Unlock() }
func (o *outcomes) has(result string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, x := range o.list {
		if x.Result == result {
			return true
		}
	}
	return false
}
func (o *outcomes) last() billing.Outcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.list[len(o.list)-1]
}

func billingProcessor(t *testing.T, paymentAddr string, policy rpcx.Policy, out *outcomes) *billing.Processor {
	return &billing.Processor{Payment: testutil.PaymentClient(t, paymentAddr), Policy: policy, LookupTimeout: 200 * time.Millisecond,
		Currencies: billing.DefaultCurrencies, Log: rpcx.Discard(), OnOutcome: out.add}
}

// CRITICAL SCENARIO (external service): the payment is executed but its answer
// is lost. Billing does not know the outcome, reconciles with GetCharge,
// finds the executed charge, ACKs, and the customer is charged exactly once.
func TestChargeResponseLostAfterCommit_BillingReconcilesAndChargesOnce(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "billing")
	p := testutil.StartPayment(t, payment.Config{DataDir: t.TempDir()}, "")
	p.Faults.Remaining.Store(1)
	out := &outcomes{}
	conn := testutil.Dial(t, b.Addr)
	testutil.Subscribe(t, conn, "billing", "billing-1", billingProcessor(t, p.Addr, testutil.FastPolicy, out).Handle)

	testutil.Publish(t, conn, testutil.Order(t, "corr-lost-charge", 4200, "MDL"))
	testutil.WaitFor(t, 3*time.Second, "charge approved", func() bool { return out.has("approved") })
	testutil.WaitFor(t, 2*time.Second, "acked", func() bool { return testutil.Idle(b, "orders.billing") })

	charges := p.Charges()
	if len(charges) != 1 {
		t.Fatalf("the customer must be charged exactly once, got %d charges", len(charges))
	}
	if o := out.last(); o.ChargeID != charges[0].ChargeID || !o.Replayed {
		t.Fatalf("billing must have recovered the executed charge (replayed), got %+v", o)
	}
	if b.Stats()["orders.billing"].DLQ != 0 || p.Faults.Remaining.Load() != 0 {
		t.Fatal("no DLQ expected, and the injected fault must have been consumed")
	}
}

// Deadline propagation + cancellation at the remote side: when the caller's
// deadline expires before the payment service commits, the charge is not
// executed, and GetCharge says so (NOT_FOUND): the outcome is resolved.
func TestChargeDeadlineExpiresBeforeCommit_NotExecuted(t *testing.T) {
	p := testutil.StartPayment(t, payment.Config{DataDir: t.TempDir(), Latency: 300 * time.Millisecond}, "")
	c := testutil.PaymentClient(t, p.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.Charge(ctx, &paymentv1.ChargeRequest{IdempotencyKey: "order:o-slow", OrderId: "o-slow", AmountCents: 10, Currency: "MDL"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := c.GetCharge(context.Background(), &paymentv1.GetChargeRequest{IdempotencyKey: "order:o-slow"}); status.Code(err) != codes.NotFound {
		t.Fatalf("the abandoned charge must not have been executed, got %v", err)
	}
	if len(p.Charges()) != 0 {
		t.Fatal("no charge expected")
	}
}

// Fallback: the payment service is down. Billing does not ACK and does not
// assume a failure; it NACKs, the broker redelivers with backoff, and once
// the service is back the order is charged once.
func TestPaymentUnavailable_BillingNacksThenRecovers(t *testing.T) {
	cfg := testutil.FastConfig(t.TempDir())
	cfg.MaxAttempts, cfg.RetryBase = 10, 50*time.Millisecond
	b := testutil.StartBroker(t, cfg, "")
	b.Declare(envelope.TopicOrders, "billing")
	addr := testutil.FreeAddr(t) // nothing listens yet
	out := &outcomes{}
	policy := rpcx.Policy{MaxAttempts: 2, BaseDelay: 10 * time.Millisecond, PerAttemptTimeout: 100 * time.Millisecond}
	conn := testutil.Dial(t, b.Addr)
	testutil.Subscribe(t, conn, "billing", "billing-1", billingProcessor(t, addr, policy, out).Handle)
	testutil.Publish(t, conn, testutil.Order(t, "corr-payment-down", 700, "EUR"))
	testutil.WaitFor(t, 3*time.Second, "outcome unknown while the service is down", func() bool { return out.has("unknown") })

	p := testutil.StartPayment(t, payment.Config{DataDir: t.TempDir()}, addr)
	testutil.WaitFor(t, 10*time.Second, "charged after recovery", func() bool { return out.has("approved") })
	testutil.WaitFor(t, 2*time.Second, "acked", func() bool { return testutil.Idle(b, "orders.billing") })
	if len(p.Charges()) != 1 || b.Stats()["orders.billing"].DLQ != 0 {
		t.Fatalf("one charge and no DLQ expected, got %d charges", len(p.Charges()))
	}
}

// No infinite retry: if the payment service never comes back, the message
// ends in the DLQ after MaxAttempts deliveries.
func TestPaymentDownForGood_MessageEndsInDLQ(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "") // MaxAttempts 3
	b.Declare(envelope.TopicOrders, "billing")
	out := &outcomes{}
	policy := rpcx.Policy{MaxAttempts: 1, PerAttemptTimeout: 50 * time.Millisecond}
	conn := testutil.Dial(t, b.Addr)
	testutil.Subscribe(t, conn, "billing", "billing-1", billingProcessor(t, testutil.FreeAddr(t), policy, out).Handle)
	testutil.Publish(t, conn, testutil.Order(t, "corr-payment-gone", 700, "EUR"))
	testutil.WaitFor(t, 5*time.Second, "DLQ", func() bool { return b.Stats()["orders.billing"].DLQ == 1 })
	d := b.DLQ("orders.billing")[0]
	if d.Attempts != 3 || d.Permanent || !strings.Contains(d.Reason, "payment outcome unknown") {
		t.Fatalf("unexpected DLQ entry %+v", d)
	}
}

// Two billing replicas get the same order (redelivery after an ack timeout
// while the first replica is still waiting for the payment service). The
// idempotency key makes the concurrent charges one charge.
func TestTwoBillingReplicasChargeTheSameOrderOnce(t *testing.T) {
	cfg := testutil.FastConfig(t.TempDir())
	cfg.AckTimeout = 100 * time.Millisecond
	b := testutil.StartBroker(t, cfg, "")
	b.Declare(envelope.TopicOrders, "billing")
	p := testutil.StartPayment(t, payment.Config{DataDir: t.TempDir(), Latency: 250 * time.Millisecond}, "")
	out := &outcomes{}
	policy := rpcx.Policy{MaxAttempts: 2, BaseDelay: 10 * time.Millisecond, PerAttemptTimeout: time.Second}
	conn := testutil.Dial(t, b.Addr)
	testutil.Subscribe(t, conn, "billing", "billing-a", billingProcessor(t, p.Addr, policy, out).Handle)
	testutil.WaitFor(t, 2*time.Second, "a subscribed", func() bool { return b.Stats()["orders.billing"].Consumers == 1 })
	testutil.Subscribe(t, conn, "billing", "billing-b", billingProcessor(t, p.Addr, policy, out).Handle)
	testutil.Publish(t, conn, testutil.Order(t, "corr-replicas", 900, "USD"))
	testutil.WaitFor(t, 5*time.Second, "settled", func() bool { return out.has("approved") && testutil.Idle(b, "orders.billing") })
	time.Sleep(400 * time.Millisecond)
	if n := len(p.Charges()); n != 1 {
		t.Fatalf("concurrent deliveries of one order must produce one charge, got %d", n)
	}
}
