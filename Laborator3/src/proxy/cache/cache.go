// Package cache is the response cache of the proxy, kept in Redis so that
// every proxy replica shares it and it survives a proxy restart.
//
// Invalidation uses generations (ADR 0002). Each namespace ("employees") has a
// counter gen:<ns>. A cache key contains the generation read when the request
// started; a write increments the counter before and after it is forwarded.
// An answer computed from old data is therefore stored under an old
// generation that no reader uses any more: a slow GET that started before a
// PUT cannot put the old value back in the cache.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Gen      int64             `json:"gen"`
}

// Redis implements the cache. After an error it stops calling Redis for
// Backoff (fail fast), so an unreachable Redis does not add a timeout to every
// request.
type Redis struct {
	c       *redis.Client
	Backoff time.Duration
	down    atomic.Int64 // unix nanos until which Redis is considered down
	errors  atomic.Int64
	// set when an invalidation could not be written: the generation must be
	// bumped before the cache is used again
	pendingBump atomic.Bool
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
	return &Redis{c: c, Backoff: time.Second}
}

// Client exposes the Redis client (the client registry uses the same server).
func (r *Redis) Client() *redis.Client { return r.c }

func (r *Redis) Errors() int64 { return r.errors.Load() }

// Available reports whether Redis is considered reachable.
func (r *Redis) Available() bool { return time.Now().UnixNano() >= r.down.Load() }

func (r *Redis) failed(err error) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return err
	}
	r.errors.Add(1)
	r.down.Store(time.Now().Add(r.Backoff).UnixNano())
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}

func (r *Redis) check() error {
	if !r.Available() {
		return ErrUnavailable
	}
	return nil
}

func genKey(ns string) string { return "gen:" + ns }

// Generation returns the current generation of the namespace. If an earlier
// invalidation was lost, it is written first; until then the cache is not used.
func (r *Redis) Generation(ctx context.Context, ns string) (int64, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	if r.pendingBump.Load() {
		if _, err := r.Bump(ctx, ns); err != nil {
			return 0, err
		}
	}
	g, err := r.c.Get(ctx, genKey(ns)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, r.failed(err)
	}
	return g, nil
}

// Bump increments the generation: every entry of the namespace becomes
// unreachable at once (O(1), no key scan).
func (r *Redis) Bump(ctx context.Context, ns string) (int64, error) {
	g, err := r.c.Incr(ctx, genKey(ns)).Result()
	if err != nil {
		r.pendingBump.Store(true)
		return 0, r.failed(err)
	}
	r.pendingBump.Store(false)
	return g, nil
}

// Get returns the entry stored under key.
func (r *Redis) Get(ctx context.Context, key string) (Entry, bool, error) {
	if err := r.check(); err != nil {
		return Entry{}, false, err
	}
	b, err := r.c.Get(ctx, "cache:"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, r.failed(err)
	}
	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		return Entry{}, false, nil
	}
	return e, true, nil
}

// Set stores the entry for ttl.
func (r *Redis) Set(ctx context.Context, key string, e Entry, ttl time.Duration) error {
	if err := r.check(); err != nil {
		return err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return r.failed(r.c.Set(ctx, "cache:"+key, b, ttl).Err())
}

// Ping checks the connection (and clears the fail-fast window on success).
func (r *Redis) Ping(ctx context.Context) error {
	if err := r.c.Ping(ctx).Err(); err != nil {
		return r.failed(err)
	}
	r.down.Store(0)
	return nil
}

func (r *Redis) Close() error { return r.c.Close() }
