package common

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
)

// Crash stages of the failure-injection hook.
const (
	StageBeforeEffect = "before_effect"
	StageAfterEffect  = "after_effect"
)

// Inventory reserves stock for every order: a local effect, made idempotent
// by the effect store (ACK after the effect, at-least-once delivery).
type Inventory struct {
	Store *EffectStore
	Dedup bool // skip messages whose effect was already applied
	// CrashAt is a failure-injection hook: returning true at a stage makes the
	// processor "crash" (end the session without ACK).
	CrashAt func(stage string, d client.Delivery) bool
	Delay   time.Duration // simulated work time
	Log     *slog.Logger
}

func (p *Inventory) Handle(ctx context.Context, d client.Delivery) error {
	m := d.Message
	log := p.Log.With("correlation_id", m.CorrelationId, "message_id", m.MessageId, "attempt", d.Attempt)
	o, err := envelope.DecodeOrderCreated(m)
	if err != nil {
		log.Error("message rejected, permanent error", "event", "message_rejected", "error", err)
		return client.Permanent(err)
	}
	if p.Dedup && p.Store.Has(m.MessageId) { // fast path; ApplyOnce below closes the race
		log.Info("duplicate delivery, effect skipped, will ACK", "event", "duplicate_skipped", "order_id", o.OrderId)
		return nil
	}
	if p.crash(StageBeforeEffect, d) {
		log.Error("SIMULATED CRASH before effect", "event", "simulated_crash", "stage", StageBeforeEffect, "order_id", o.OrderId)
		return client.ErrCrash
	}
	if p.Delay > 0 {
		select {
		case <-time.After(p.Delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	effect := Effect{MessageID: m.MessageId, CorrelationID: m.CorrelationId, Consumer: "inventory", Action: "reserve_stock",
		OrderID: o.OrderId, AmountCents: o.AmountCents, At: time.Now().UTC()}
	applied, err := true, error(nil)
	if p.Dedup {
		applied, err = p.Store.ApplyOnce(effect) // check + write atomically
	} else {
		err = p.Store.Apply(effect)
	}
	if err != nil {
		log.Error("could not apply effect, will be retried", "event", "effect_failed", "error", err)
		return errors.Join(errors.New("local effect failed"), err)
	}
	if !applied {
		log.Info("duplicate delivery, effect applied meanwhile by another delivery, will ACK", "event", "duplicate_skipped", "order_id", o.OrderId)
		return nil
	}
	log.Info("local effect applied", "event", "effect_applied", "action", "reserve_stock", "order_id", o.OrderId)
	if p.crash(StageAfterEffect, d) {
		log.Error("SIMULATED CRASH after effect, before ACK", "event", "simulated_crash", "stage", StageAfterEffect, "order_id", o.OrderId)
		return client.ErrCrash
	}
	return nil
}

func (p *Inventory) crash(stage string, d client.Delivery) bool {
	return p.CrashAt != nil && p.CrashAt(stage, d)
}
