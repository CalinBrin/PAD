package proxy

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Registry is the "smart proxy" bookkeeping (EIP Smart Proxy, ADR 0004):
// for every client connection and every request it records who asked, on
// which connection, which replica answered, with which result. The records
// live in Redis (shared by all proxy replicas, they survive a proxy restart):
//
//	client:<ip>        hash   first_seen, last_seen, requests, conns_total, conns_rejected, last_path
//	conns:<proxy-id>   hash   <ip> -> connections open now on this proxy replica
//	req:<corr-id>      hash   client, client_id, remote_addr, method, path, status, cache, backend, attempts, duration_ms (10 min)
//	clients            set    every client ip
//
// Writes are asynchronous (buffered channel, pipelined by a background
// goroutine) so Redis is never on the request path; if Redis is slow or down
// the events are dropped and counted, and an in-memory copy remains.
type Registry struct {
	proxyID string
	rdb     *redis.Client
	log     *slog.Logger
	events  chan func(redis.Pipeliner)
	dropped atomic.Int64

	mu      sync.Mutex
	clients map[string]*ClientInfo
}

// ClientInfo is what this proxy replica knows about one client.
type ClientInfo struct {
	Client        string    `json:"client"`
	OpenConns     int64     `json:"open_conns"`
	ConnsTotal    int64     `json:"conns_total"`
	ConnsRejected int64     `json:"conns_rejected"`
	Requests      int64     `json:"requests"`
	FirstSeen     time.Time `json:"first_seen"`
	LastSeen      time.Time `json:"last_seen"`
	LastPath      string    `json:"last_path"`
}

// RequestRecord is the start of a request.
type RequestRecord struct {
	CorrelationID, Client, ClientID, RemoteAddr, Method, Path string
	Started                                                   time.Time
}

const requestTTL = 10 * time.Minute

func NewRegistry(proxyID string, rdb *redis.Client, log *slog.Logger) *Registry {
	return &Registry{proxyID: proxyID, rdb: rdb, log: log,
		events: make(chan func(redis.Pipeliner), 10000), clients: map[string]*ClientInfo{}}
}

func (r *Registry) Dropped() int64 { return r.dropped.Load() }

func (r *Registry) client(ip string) *ClientInfo {
	c := r.clients[ip]
	if c == nil {
		now := time.Now().UTC()
		c = &ClientInfo{Client: ip, FirstSeen: now, LastSeen: now}
		r.clients[ip] = c
	}
	return c
}

func (r *Registry) emit(f func(redis.Pipeliner)) {
	if r.rdb == nil {
		return
	}
	select {
	case r.events <- f:
	default:
		r.dropped.Add(1)
	}
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// ConnOpened is called by the listener for every accepted client connection.
func (r *Registry) ConnOpened(ip string) {
	r.mu.Lock()
	c := r.client(ip)
	c.OpenConns++
	c.ConnsTotal++
	c.LastSeen = time.Now().UTC()
	r.mu.Unlock()
	now := ts(time.Now())
	r.emit(func(p redis.Pipeliner) {
		p.SAdd(context.Background(), "clients", ip)
		p.HSetNX(context.Background(), "client:"+ip, "first_seen", now)
		p.HSet(context.Background(), "client:"+ip, "last_seen", now)
		p.HIncrBy(context.Background(), "client:"+ip, "conns_total", 1)
		p.HIncrBy(context.Background(), "conns:"+r.proxyID, ip, 1)
	})
}

// ConnClosed is called when a client connection is closed (by either side).
func (r *Registry) ConnClosed(ip string) {
	r.mu.Lock()
	r.client(ip).OpenConns--
	r.mu.Unlock()
	r.emit(func(p redis.Pipeliner) { p.HIncrBy(context.Background(), "conns:"+r.proxyID, ip, -1) })
}

// ConnRejected: the client already holds the maximum number of connections.
func (r *Registry) ConnRejected(ip string) {
	r.mu.Lock()
	r.client(ip).ConnsRejected++
	r.mu.Unlock()
	r.emit(func(p redis.Pipeliner) { p.HIncrBy(context.Background(), "client:"+ip, "conns_rejected", 1) })
}

func (r *Registry) RequestStarted(rec RequestRecord) {
	r.mu.Lock()
	c := r.client(rec.Client)
	c.Requests++
	c.LastSeen = rec.Started.UTC()
	c.LastPath = rec.Method + " " + rec.Path
	r.mu.Unlock()
	r.emit(func(p redis.Pipeliner) {
		ctx := context.Background()
		p.HIncrBy(ctx, "client:"+rec.Client, "requests", 1)
		p.HSet(ctx, "client:"+rec.Client, "last_seen", ts(rec.Started), "last_path", rec.Method+" "+rec.Path)
		if rec.CorrelationID != "" {
			k := "req:" + rec.CorrelationID
			p.HSet(ctx, k, "client", rec.Client, "client_id", rec.ClientID, "remote_addr", rec.RemoteAddr,
				"method", rec.Method, "path", rec.Path, "started", ts(rec.Started), "proxy", r.proxyID, "state", "in_progress")
			p.Expire(ctx, k, requestTTL)
		}
	})
}

func (r *Registry) RequestFinished(corr, client string, out outcome, d time.Duration) {
	if corr == "" {
		return
	}
	r.emit(func(p redis.Pipeliner) {
		ctx := context.Background()
		k := "req:" + corr
		p.HSet(ctx, k, "state", "done", "status", out.status, "cache", out.cache, "backend", out.backend,
			"attempts", out.attempts, "duration_ms", d.Milliseconds())
		p.Expire(ctx, k, requestTTL)
		p.Expire(ctx, "client:"+client, 24*time.Hour)
	})
}

// Run writes the events to Redis in pipelines until ctx is done.
// At start it clears the connection counters of this proxy replica: the
// connections of a previous run of the same replica no longer exist.
func (r *Registry) Run(ctx context.Context) {
	if r.rdb == nil {
		return
	}
	_ = r.rdb.Del(ctx, "conns:"+r.proxyID).Err()
	batch := make([]func(redis.Pipeliner), 0, 256)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		fctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := r.rdb.Pipelined(fctx, func(p redis.Pipeliner) error {
			for _, f := range batch {
				f(p)
			}
			return nil
		})
		cancel()
		if err != nil {
			r.dropped.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case f := <-r.events:
			batch = append(batch, f)
			if len(batch) >= 256 {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

// Local returns what this replica knows (used when Redis is unreachable).
func (r *Registry) Local() []ClientInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ClientInfo, 0, len(r.clients))
	for _, c := range r.clients {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Client < out[j].Client })
	return out
}

// Shared returns the clients recorded in Redis by every proxy replica.
func (r *Registry) Shared(ctx context.Context) ([]ClientInfo, error) {
	if r.rdb == nil {
		return r.Local(), nil
	}
	ips, err := r.rdb.SMembers(ctx, "clients").Result()
	if err != nil {
		return nil, err
	}
	open := map[string]int64{}
	iter := r.rdb.Scan(ctx, 0, "conns:*", 100).Iterator()
	for iter.Next(ctx) {
		m, err := r.rdb.HGetAll(ctx, iter.Val()).Result()
		if err != nil {
			return nil, err
		}
		for ip, v := range m {
			n, _ := strconv.ParseInt(v, 10, 64)
			open[ip] += n
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	out := []ClientInfo{}
	for _, ip := range ips {
		m, err := r.rdb.HGetAll(ctx, "client:"+ip).Result()
		if err != nil {
			return nil, err
		}
		if len(m) == 0 {
			continue
		}
		c := ClientInfo{Client: ip, OpenConns: open[ip], LastPath: m["last_path"]}
		c.Requests, _ = strconv.ParseInt(m["requests"], 10, 64)
		c.ConnsTotal, _ = strconv.ParseInt(m["conns_total"], 10, 64)
		c.ConnsRejected, _ = strconv.ParseInt(m["conns_rejected"], 10, 64)
		c.FirstSeen, _ = time.Parse(time.RFC3339Nano, m["first_seen"])
		c.LastSeen, _ = time.Parse(time.RFC3339Nano, m["last_seen"])
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Client < out[j].Client })
	return out, nil
}

// Request returns the record of one request (by correlation ID).
func (r *Registry) Request(ctx context.Context, corr string) (map[string]string, error) {
	if r.rdb == nil {
		return nil, redis.Nil
	}
	m, err := r.rdb.HGetAll(ctx, "req:"+corr).Result()
	if err == nil && len(m) == 0 {
		return nil, redis.Nil
	}
	return m, err
}

// LimitListener wraps the client listener: it records every connection in the
// registry and refuses a new connection from a client that already holds
// MaxPerClient open connections (one client cannot take every slot).
type LimitListener struct {
	net.Listener
	MaxPerClient int
	Reg          *Registry

	mu    sync.Mutex
	perIP map[string]int
	open  atomic.Int64
}

func (l *LimitListener) OpenConns() int64 { return l.open.Load() }

func (l *LimitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		ip := c.RemoteAddr().String()
		if h, _, err := net.SplitHostPort(ip); err == nil {
			ip = h
		}
		l.mu.Lock()
		if l.perIP == nil {
			l.perIP = map[string]int{}
		}
		if l.MaxPerClient > 0 && l.perIP[ip] >= l.MaxPerClient {
			l.mu.Unlock()
			_ = c.Close()
			if l.Reg != nil {
				l.Reg.ConnRejected(ip)
			}
			continue
		}
		l.perIP[ip]++
		l.mu.Unlock()
		l.open.Add(1)
		if l.Reg != nil {
			l.Reg.ConnOpened(ip)
		}
		return &trackedConn{Conn: c, onClose: func() {
			l.mu.Lock()
			l.perIP[ip]--
			if l.perIP[ip] <= 0 {
				delete(l.perIP, ip)
			}
			l.mu.Unlock()
			l.open.Add(-1)
			if l.Reg != nil {
				l.Reg.ConnClosed(ip)
			}
		}}, nil
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}
