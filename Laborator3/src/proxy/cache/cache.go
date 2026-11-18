// Package cache is the response cache of the proxy, kept in Redis so that
// every proxy replica shares it and it survives a proxy restart.
//
// Invalidation uses generation counters (ADR 0002). A cached answer depends
// on a few counters (for GET /employees/a: gen:employees:all and
// gen:employees:item:a; for a list page: gen:employees:all and
// gen:employees:lists). The counters are read, together with the entry, in
// one MGET BEFORE the warehouse is asked, and stored inside the entry.
// An entry is used only while its counters are unchanged. A write increments
// the counters of what it touches, before and after it is forwarded. So an
// answer computed from old data (a slow GET that started before a PUT) carries
// old counters and is never served, whenever it is written.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrUnavailable: Redis cannot be reached; the proxy works without cache.
var ErrUnavailable = errors.New("cache unavailable")

// Entry is a stored upstream answer.
type Entry struct {
	Status   int               `json:"status"`
	Header   map[string]string `json:"header"`
	Body     []byte            `json:"body"`
	StoredAt time.Time         `json:"stored_at"`
	Gens     []int64           `json:"gens"` // the counters read before the upstream request
}

// Redis implements the cache.
//
// Fail fast: after an error Redis is marked down and no request calls it any
// more (no request waits for a Redis timeout); one background probe pings it
// every Backoff and marks it up again.
type Redis struct {
	c       *redis.Client
	Backoff time.Duration
	down    atomic.Bool
	probing atomic.Bool
	errors  atomic.Int64

	// invalidations that could not be written: they are written before the
	// cache is used again
	mu      sync.Mutex
	pending map[string]bool
	// OnUp is called when Redis is reachable again (logging).
	OnUp func()
}

// NewRedis connects lazily to addr (host:port).
func NewRedis(addr string, timeout time.Duration) *Redis {
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	c := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		PoolSize:     64,
		MaxRetries:   0,
	})
	return &Redis{c: c, Backoff: 500 * time.Millisecond, pending: map[string]bool{}}
}

// Client exposes the Redis client (the client registry uses the same server).
func (r *Redis) Client() *redis.Client { return r.c }

func (r *Redis) Errors() int64 { return r.errors.Load() }

// Available reports whether Redis is considered reachable.
func (r *Redis) Available() bool { return !r.down.Load() }

func (r *Redis) failed(err error) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return err
	}
	r.errors.Add(1)
	r.down.Store(true)
	if r.probing.CompareAndSwap(false, true) {
		go r.probe()
	}
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}

func (r *Redis) probe() {
	defer r.probing.Store(false)
	for {
		time.Sleep(r.Backoff)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := r.c.Ping(ctx).Err()
		cancel()
		if err == nil {
			r.down.Store(false)
			if r.OnUp != nil {
				r.OnUp()
			}
			return
		}
	}
}

func (r *Redis) check() error {
	if r.down.Load() {
		return ErrUnavailable
	}
	return nil
}

// flushPending writes the invalidations lost while Redis was unreachable.
func (r *Redis) flushPending(ctx context.Context) error {
	r.mu.Lock()
	keys := make([]string, 0, len(r.pending))
	for k := range r.pending {
		keys = append(keys, k)
	}
	r.mu.Unlock()
	if len(keys) == 0 {
		return nil
	}
	return r.Bump(ctx, keys...)
}

// Lookup reads the entry and the counters it depends on in one round trip.
// It returns the entry only if it is still valid, and always the current
// counters (to be stored in the entry built from the upstream answer).
func (r *Redis) Lookup(ctx context.Context, key string, deps []string) (*Entry, []int64, error) {
	if err := r.check(); err != nil {
		return nil, nil, err
	}
	if err := r.flushPending(ctx); err != nil {
		return nil, nil, err
	}
	vals, err := r.c.MGet(ctx, append([]string{"cache:" + key}, deps...)...).Result()
	if err != nil {
		return nil, nil, r.failed(err)
	}
	gens := make([]int64, len(deps))
	for i, v := range vals[1:] {
		if s, ok := v.(string); ok {
			fmt.Sscan(s, &gens[i])
		}
	}
	s, ok := vals[0].(string)
	if !ok {
		return nil, gens, nil
	}
	var e Entry
	if json.Unmarshal([]byte(s), &e) != nil || !slices.Equal(e.Gens, gens) {
		return nil, gens, nil // absent, unreadable or invalidated since it was computed
	}
	return &e, gens, nil
}

// Store writes the entry for ttl.
func (r *Redis) Store(ctx context.Context, key string, e Entry, ttl time.Duration) error {
	if err := r.check(); err != nil {
		return err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return r.failed(r.c.Set(ctx, "cache:"+key, b, ttl).Err())
}

// Bump increments the counters (one pipelined round trip). The counters have
// no TTL: with maxmemory-policy volatile-lru Redis never evicts them.
// If Redis cannot be reached, the keys are kept and bumped before the cache
// is used again.
func (r *Redis) Bump(ctx context.Context, keys ...string) error {
	if r.down.Load() { // do not wait for a timeout: remember them
		r.mu.Lock()
		for _, k := range keys {
			r.pending[k] = true
		}
		r.mu.Unlock()
		return ErrUnavailable
	}
	_, err := r.c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, k := range keys {
			p.Incr(ctx, k)
		}
		return nil
	})
	r.mu.Lock()
	for _, k := range keys {
		if err != nil {
			r.pending[k] = true
		} else {
			delete(r.pending, k)
		}
	}
	r.mu.Unlock()
	return r.failed(err)
}

// Ping checks the connection.
func (r *Redis) Ping(ctx context.Context) error {
	if err := r.c.Ping(ctx).Err(); err != nil {
		return r.failed(err)
	}
	r.down.Store(false)
	return nil
}

func (r *Redis) Close() error { return r.c.Close() }
