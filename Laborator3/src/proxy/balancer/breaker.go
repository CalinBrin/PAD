package balancer

import (
	"sync"
	"time"
)

// Breaker is the passive failure detector of one backend (circuit breaker).
//
//	closed    --Threshold consecutive failures-->  open
//	open      --Cooldown elapsed-->                half-open (one trial request)
//	half-open --trial succeeds-->                  closed
//	half-open --trial fails-->                     open
//
// It reacts to the real traffic between two active health checks.
type Breaker struct {
	Threshold int
	Cooldown  time.Duration
	Now       func() time.Time

	mu       sync.Mutex
	state    string
	failures int
	openedAt time.Time
	probing  bool
}

const (
	Closed   = "closed"
	Open     = "open"
	HalfOpen = "half-open"
)

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 3
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Second
	}
	return &Breaker{Threshold: threshold, Cooldown: cooldown, Now: time.Now, state: Closed}
}

// Available reports whether a request may be sent now (without reserving it).
func (b *Breaker) Available() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		return b.Now().Sub(b.openedAt) >= b.Cooldown
	default: // half-open: one trial at a time
		return !b.probing
	}
}

// Acquire reserves the right to send a request. In half-open (or open with an
// elapsed cooldown) only one trial request is admitted.
func (b *Breaker) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		if b.Now().Sub(b.openedAt) < b.Cooldown {
			return false
		}
		b.state = HalfOpen
		fallthrough
	default:
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

// Success records a successful request; it returns true when the breaker closed.
func (b *Breaker) Success() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	if b.state != Closed {
		b.state = Closed
		return true
	}
	return false
}

// Failure records a failed request; it returns true when the breaker opened.
func (b *Breaker) Failure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	b.failures++
	if b.state == HalfOpen || (b.state == Closed && b.failures >= b.Threshold) {
		b.state = Open
		b.openedAt = b.Now()
		return true
	}
	return false
}

// Release gives back an admitted request that says nothing about the backend
// (the client went away before the answer).
func (b *Breaker) Release() {
	b.mu.Lock()
	b.probing = false
	b.mu.Unlock()
}

// State returns closed, open or half-open.
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
