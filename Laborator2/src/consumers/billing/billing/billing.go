// Package billing charges every order at the external payment service.
//
// The critical scenario of the lab lives here: the charge is executed at the
// payment service but its answer is lost (deadline exceeded, connection
// reset). Billing cannot tell "not charged" from "charged, answer lost". It
// never guesses; it resolves the ambiguity:
//
//  1. every call carries a deadline and the idempotency key "order:<order_id>",
//     so repeating the call can never charge twice;
//  2. after an unknown outcome it asks GetCharge: found = it was executed
//     (use that result), NOT_FOUND = it was not (safe to charge again);
//  3. the retry policy is bounded; if the outcome is still unknown, billing
//     NACKs the message (fallback): the broker redelivers it later with
//     backoff, the same key is used again, and after MaxAttempts it ends in
//     the DLQ instead of looping forever.
package billing

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentv1 "pad-lab2/gen/payment/v1"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
)

// Outcome of one delivery, reported to OnOutcome (tests, metrics).
type Outcome struct {
	MessageID string
	OrderID   string
	Result    string // approved | declined | rejected | unknown
	ChargeID  string
	Replayed  bool
}

type Processor struct {
	Payment paymentv1.PaymentClient
	// Policy of the Charge call: attempts, per-attempt deadline, backoff.
	Policy rpcx.Policy
	// LookupTimeout is the deadline of a GetCharge reconciliation call.
	LookupTimeout time.Duration
	Currencies    map[string]bool
	Log           *slog.Logger
	OnOutcome     func(Outcome)
}

// DefaultPolicy: 3 attempts, 800ms deadline each.
var DefaultPolicy = rpcx.Policy{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second, PerAttemptTimeout: 800 * time.Millisecond}

var DefaultCurrencies = map[string]bool{"MDL": true, "EUR": true, "USD": true}

// IdempotencyKey of the charge of an order: one order, at most one charge,
// whatever the number of deliveries, retries or billing replicas.
func IdempotencyKey(orderID string) string { return "order:" + orderID }

func (p *Processor) Handle(ctx context.Context, d client.Delivery) error {
	m := d.Message
	log := p.Log.With("correlation_id", m.CorrelationId, "message_id", m.MessageId, "attempt", d.Attempt)
	o, err := envelope.DecodeOrderCreated(m)
	if err != nil {
		log.Error("message rejected, permanent error", "event", "message_rejected", "error", err)
		p.report(Outcome{MessageID: m.MessageId, Result: "rejected"})
		return client.Permanent(err)
	}
	if !p.Currencies[o.Currency] {
		err := fmt.Errorf("currency %q is not supported", o.Currency)
		log.Warn("order rejected, permanent error", "event", "message_rejected", "order_id", o.OrderId, "error", err)
		p.report(Outcome{MessageID: m.MessageId, OrderID: o.OrderId, Result: "rejected"})
		return client.Permanent(err)
	}
	req := &paymentv1.ChargeRequest{IdempotencyKey: IdempotencyKey(o.OrderId), OrderId: o.OrderId, AmountCents: o.AmountCents, Currency: o.Currency}
	log = log.With("order_id", o.OrderId, "idempotency_key", req.IdempotencyKey)

	res, err := p.charge(ctx, req, log)
	switch {
	case err == nil:
	case status.Code(err) == codes.InvalidArgument || status.Code(err) == codes.FailedPrecondition:
		log.Error("charge refused by the payment service, permanent error", "event", "charge_refused", "result", status.Code(err).String(), "error", status.Convert(err).Message())
		p.report(Outcome{MessageID: m.MessageId, OrderID: o.OrderId, Result: "rejected"})
		return client.Permanent(err)
	default:
		// Still unknown (or the service is down): do not ACK, do not assume
		// failure. The broker redelivers later; the key prevents a double charge.
		log.Warn("charge outcome still unknown, NACK for a later redelivery", "event", "charge_outcome_unknown", "result", status.Code(err).String(), "error", err.Error())
		p.report(Outcome{MessageID: m.MessageId, OrderID: o.OrderId, Result: "unknown"})
		return fmt.Errorf("payment outcome unknown: %w", err)
	}

	result := "approved"
	if res.Status == paymentv1.ChargeStatus_CHARGE_STATUS_DECLINED {
		result = "declined" // a business outcome: recorded and acknowledged, not retried
	}
	log.Info("order charged", "event", "charge_"+result, "charge_id", res.ChargeId, "replayed", res.Replayed, "decline_reason", res.DeclineReason)
	p.report(Outcome{MessageID: m.MessageId, OrderID: o.OrderId, Result: result, ChargeID: res.ChargeId, Replayed: res.Replayed})
	return nil
}

// charge calls Charge under the retry policy. After an ambiguous failure it
// reconciles with GetCharge before trying again.
func (p *Processor) charge(ctx context.Context, req *paymentv1.ChargeRequest, log *slog.Logger) (*paymentv1.ChargeResponse, error) {
	var res *paymentv1.ChargeResponse
	err := rpcx.Do(ctx, p.Policy, log, "payment.Charge", rpcx.Retryable, func(actx context.Context, attempt int) error {
		r, err := p.Payment.Charge(actx, req)
		if err == nil {
			res = r
			return nil
		}
		if !rpcx.Ambiguous(err) || ctx.Err() != nil {
			return err
		}
		log.Warn("charge outcome unknown: it may have been executed, reconciling with GetCharge", "event", "charge_ambiguous",
			"call_attempt", attempt, "result", status.Code(err).String())
		found, lerr := p.lookup(ctx, req.IdempotencyKey)
		switch {
		case lerr == nil:
			log.Info("reconciled: the charge was executed, its answer had been lost", "event", "charge_reconciled", "charge_id", found.ChargeId)
			res = found
			return nil
		case status.Code(lerr) == codes.NotFound:
			log.Info("reconciled: the charge was not executed, charging again with the same key", "event", "charge_not_executed")
		default:
			log.Warn("reconciliation failed, outcome still unknown", "event", "charge_lookup_failed", "result", status.Code(lerr).String())
		}
		return err
	})
	return res, err
}

func (p *Processor) lookup(ctx context.Context, key string) (*paymentv1.ChargeResponse, error) {
	t := p.LookupTimeout
	if t <= 0 {
		t = 500 * time.Millisecond
	}
	lctx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	return p.Payment.GetCharge(lctx, &paymentv1.GetChargeRequest{IdempotencyKey: key})
}

func (p *Processor) report(o Outcome) {
	if p.OnOutcome != nil {
		p.OnOutcome(o)
	}
}
