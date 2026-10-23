package rpcx

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Policy is an explicit, bounded retry policy.
type Policy struct {
	MaxAttempts       int           // total attempts, >= 1
	BaseDelay         time.Duration // delay before attempt 2 is drawn from [0, BaseDelay]
	MaxDelay          time.Duration // cap of the delay window
	PerAttemptTimeout time.Duration // deadline of each attempt (0: only the parent deadline)
}

// Backoff returns the delay before attempt n+1 (n >= 1): "full jitter", a
// random value in [0, min(MaxDelay, BaseDelay*2^(n-1))]. The randomness keeps
// many clients from retrying at the same moment after a common failure.
func (p Policy) Backoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 16 {
		n = 16
	}
	window := p.BaseDelay << (n - 1)
	if p.MaxDelay > 0 && window > p.MaxDelay {
		window = p.MaxDelay
	}
	if window <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(window) + 1))
}

// Retryable reports whether a gRPC error may be retried. It only says the
// failure can be transient; the caller must also know that repeating the
// operation is safe (it is idempotent), otherwise it must not retry.
//
//	Unavailable, DeadlineExceeded, ResourceExhausted, Aborted -> retry
//	everything else (InvalidArgument, FailedPrecondition, AlreadyExists,
//	NotFound, Unauthenticated, ...) -> do not retry
func Retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	}
	return false
}

// Ambiguous reports whether the outcome of a call is unknown: the request may
// have been executed by the server even though no answer arrived.
func Ambiguous(err error) bool {
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Unavailable, codes.Canceled:
		return true
	}
	return false
}

// ErrAttemptsExhausted wraps the last error after the last attempt.
var ErrAttemptsExhausted = errors.New("retry attempts exhausted")

// Do runs fn until it succeeds, returns a non-retryable error, the attempts
// are exhausted or ctx is done. Each attempt gets its own deadline.
func Do(ctx context.Context, p Policy, log *slog.Logger, op string, retryable func(error) bool, fn func(ctx context.Context, attempt int) error) error {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	var err error
	for attempt := 1; ; attempt++ {
		actx, cancel := ctx, context.CancelFunc(func() {})
		if p.PerAttemptTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, p.PerAttemptTimeout)
		}
		err = fn(actx, attempt)
		cancel()
		if err == nil {
			return nil
		}
		if !retryable(err) || ctx.Err() != nil {
			return err
		}
		if attempt >= p.MaxAttempts {
			return errors.Join(ErrAttemptsExhausted, err)
		}
		delay := p.Backoff(attempt)
		if log != nil {
			log.Warn("attempt failed, retrying", "event", "retry_scheduled", "operation", op, "attempt", attempt,
				"max_attempts", p.MaxAttempts, "retry_in_ms", delay.Milliseconds(), "result", status.Code(err).String(),
				"error", status.Convert(err).Message(), "correlation_id", CorrelationID(ctx))
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return err
		}
	}
}
