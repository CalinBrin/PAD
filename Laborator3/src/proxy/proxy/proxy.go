// Package proxy is the reverse proxy in front of the warehouse replicas:
// caching (Redis, generations), load balancing (balancer.Pool), retry only
// where it is safe, and the smart-proxy bookkeeping of client connections
// and requests (Registry).
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/proxy/balancer"
	"pad-lab3/src/proxy/cache"
)

// Config of the proxy.
type Config struct {
	UpstreamTimeout time.Duration // deadline of one attempt
	RequestTimeout  time.Duration // deadline of the whole request, retries included
	MaxAttempts     int           // attempts for the requests that may be retried
	RetryBackoff    time.Duration // base of the jittered backoff between attempts
	CacheTTL        time.Duration // upper bound; the warehouse max-age may shorten it
	WriteGrace      time.Duration // delay of the extra invalidation after an ambiguous write
	CacheEnabled    bool
}

func (c *Config) defaults() {
	if c.UpstreamTimeout <= 0 {
		c.UpstreamTimeout = 2 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 6 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.CacheTTL <= 0 {
		c.CacheTTL = 30 * time.Second
	}
	if c.WriteGrace <= 0 {
		c.WriteGrace = 3 * time.Second
	}
}

// Stats are the counters exposed on /proxy/stats.
type Stats struct {
	Requests, Hits, Misses, Bypass                       atomic.Int64
	Invalidations, DelayedInvalidations, InvalidationErr atomic.Int64
	Upstream, Retries, UpstreamErrors, NoBackend         atomic.Int64
	ConnsNew, ConnsReused                                atomic.Int64
	BreakerOpened                                        atomic.Int64
}

// Proxy implements http.Handler.
type Proxy struct {
	cfg     Config
	pool    *balancer.Pool
	cache   *cache.Redis // nil: no cache configured
	reg     *Registry    // nil: no client bookkeeping
	client  *http.Client
	log     *slog.Logger
	cacheOn atomic.Bool
	Stats   Stats
}

// NewTransport is the connection pool towards the warehouse replicas:
// keep-alive connections are reused across client requests.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   false,
	}
}

func New(cfg Config, pool *balancer.Pool, c *cache.Redis, reg *Registry, log *slog.Logger) *Proxy {
	cfg.defaults()
	if log == nil {
		log = logx.Discard()
	}
	p := &Proxy{cfg: cfg, pool: pool, cache: c, reg: reg, log: log,
		client: &http.Client{Transport: NewTransport(), CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}}
	p.cacheOn.Store(cfg.CacheEnabled && c != nil)
	return p
}

// SetCacheEnabled switches the cache at run time (benchmarks with / without cache).
func (p *Proxy) SetCacheEnabled(v bool) { p.cacheOn.Store(v && p.cache != nil) }
func (p *Proxy) CacheEnabled() bool     { return p.cacheOn.Load() }
func (p *Proxy) Pool() *balancer.Pool   { return p.pool }

// namespace returns the cache namespace of a path ("" for paths that are
// neither cached nor invalidating). The change feed is never cached.
func namespace(path string) string {
	if path == "/employees" || path == "/employee" ||
		strings.HasPrefix(path, "/employees/") || strings.HasPrefix(path, "/employee/") {
		return "employees"
	}
	return ""
}

// idempotent: may the request be sent again after a failure whose outcome is unknown?
// GET/HEAD/PUT/DELETE are idempotent by HTTP semantics (and the warehouse
// keeps them so: PUT with the same body is a no-op). POST is retried only
// with an Idempotency-Key, which the warehouse deduplicates.
func idempotent(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	case http.MethodPost:
		return r.Header.Get("Idempotency-Key") != ""
	}
	return false
}

var errNoBackend = errors.New("no healthy backend")

// result is the answer of the upstream (or why there is none).
type result struct {
	status   int
	header   http.Header
	body     []byte
	backend  string
	attempts int
	err      error
}

func (r result) ambiguous() bool { return r.err != nil || r.status >= 500 }

type outcome struct {
	cache    string
	backend  string
	attempts int
	status   int
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	p.Stats.Requests.Add(1)
	corr := logx.CorrelationID(r.Context())
	ctx, cancel := context.WithTimeout(r.Context(), p.cfg.RequestTimeout)
	defer cancel()

	body, err := io.ReadAll(io.LimitReader(r.Body, httpx.MaxBody+1))
	if err != nil || len(body) > httpx.MaxBody {
		httpx.WriteError(w, r, http.StatusRequestEntityTooLarge, "body_too_large", "request body limited to 1 MiB")
		return
	}
	client := clientIP(r)
	if p.reg != nil {
		p.reg.RequestStarted(RequestRecord{CorrelationID: corr, Client: client, ClientID: r.Header.Get("X-Client-ID"),
			RemoteAddr: r.RemoteAddr, Method: r.Method, Path: r.URL.RequestURI(), Started: start})
	}
	var out outcome
	ns := namespace(r.URL.Path)
	switch {
	case ns != "" && r.Method == http.MethodGet:
		out = p.serveCached(ctx, w, r, body, ns)
	case ns != "" && r.Method != http.MethodHead && r.Method != http.MethodOptions:
		out = p.serveWrite(ctx, w, r, body, ns)
	default:
		res := p.forward(ctx, r, body)
		out = p.writeResult(w, r, res, "BYPASS")
	}
	dur := time.Since(start)
	p.log.Info("proxied", "event", "proxied", "method", r.Method, "path", r.URL.RequestURI(), "status", out.status,
		"cache", out.cache, "backend", out.backend, "attempts", out.attempts, "duration_ms", dur.Milliseconds(),
		"client", client, "correlation_id", corr)
	if p.reg != nil {
		p.reg.RequestFinished(corr, client, out, dur)
	}
}

// serveCached answers a GET from the cache when possible.
func (p *Proxy) serveCached(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, ns string) outcome {
	f, ok := httpx.Negotiate(r)
	cc := strings.ToLower(r.Header.Get("Cache-Control"))
	noStore := strings.Contains(cc, "no-store")
	noCache := strings.Contains(cc, "no-cache") || strings.Contains(strings.ToLower(r.Header.Get("Pragma")), "no-cache")
	if !p.cacheOn.Load() || !ok || noStore {
		p.Stats.Bypass.Add(1)
		return p.writeResult(w, r, p.forward(ctx, r, body), "BYPASS")
	}
	key := cacheKey(ns, r, f)
	// The entry and its generation counters are read in one round trip,
	// BEFORE the upstream request: if a write happens meanwhile, the answer
	// is stored with counters that are already old, so it is never served.
	e, gens, err := p.cache.Lookup(ctx, key, dependencies(ns, r))
	if err != nil {
		p.Stats.Bypass.Add(1)
		return p.writeResult(w, r, p.forward(ctx, r, body), "BYPASS")
	}
	if e != nil && !noCache {
		p.Stats.Hits.Add(1)
		return p.writeEntry(w, r, *e)
	}
	p.Stats.Misses.Add(1)
	res := p.forward(ctx, r, body)
	if res.err == nil && res.status == http.StatusOK {
		if ttl := p.ttl(res.header); ttl > 0 {
			ne := cache.Entry{Status: res.status, Header: keepHeaders(res.header), Body: res.body,
				StoredAt: time.Now().UTC(), Gens: gens}
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 300*time.Millisecond)
			_ = p.cache.Store(sctx, key, ne, ttl)
			cancel()
		}
	}
	return p.writeResult(w, r, res, "MISS")
}

// serveWrite forwards a PUT/POST/DELETE and invalidates what it touches:
// before forwarding, after it (whatever the outcome: success, error or
// timeout) and, if the outcome is unknown, once more after WriteGrace, when a
// write still in flight in the warehouse has certainly finished.
func (p *Proxy) serveWrite(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, ns string) outcome {
	keys := invalidations(ns, r, body)
	if p.cache != nil {
		p.bump(ctx, keys, "before_write", r)
	}
	res := p.forward(ctx, r, body)
	if p.cache != nil {
		p.bump(context.WithoutCancel(ctx), keys, "after_write", r)
		if res.ambiguous() {
			p.Stats.DelayedInvalidations.Add(1)
			corr := logx.CorrelationID(ctx)
			time.AfterFunc(p.cfg.WriteGrace, func() {
				p.bump(logx.WithCorrelationID(context.Background(), corr), keys, "delayed_after_ambiguous_write", r)
			})
		}
	}
	return p.writeResult(w, r, res, "INVALIDATE")
}

func (p *Proxy) bump(ctx context.Context, keys []string, reason string, r *http.Request) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := p.cache.Bump(ctx, keys...); err != nil {
		p.Stats.InvalidationErr.Add(1)
		p.log.Warn("cache invalidation failed, will be written before the cache is used again",
			"event", "invalidation_failed", "keys", keys, "reason", reason, "error", err,
			"correlation_id", logx.CorrelationID(ctx))
		return
	}
	p.Stats.Invalidations.Add(1)
	p.log.Info("cache invalidated", "event", "cache_invalidated", "keys", keys,
		"reason", reason, "method", r.Method, "path", r.URL.Path, "correlation_id", logx.CorrelationID(ctx))
}

// itemID returns the employee a request is about ("" for lists, or when the
// id is only in the body): /employees/{id}, /employee?id=, /employee/?id=.
func itemID(r *http.Request) string {
	if id, ok := strings.CutPrefix(r.URL.Path, "/employees/"); ok && id != "" && !strings.Contains(id, "/") {
		return id
	}
	if r.URL.Path == "/employee" || r.URL.Path == "/employee/" {
		return r.URL.Query().Get("id")
	}
	return ""
}

func genAll(ns string) string      { return "gen:" + ns + ":all" }
func genLists(ns string) string    { return "gen:" + ns + ":lists" }
func genItem(ns, id string) string { return "gen:" + ns + ":item:" + id }

// dependencies: the counters a cached GET depends on.
func dependencies(ns string, r *http.Request) []string {
	if id := itemID(r); id != "" {
		return []string{genAll(ns), genItem(ns, id)}
	}
	return []string{genAll(ns), genLists(ns)}
}

// invalidations: the counters a write increments. A write to one employee
// invalidates that employee and the list pages (which may contain it), not
// the other employees. If the proxy cannot tell which employee (PUT /employee
// with an unreadable body), everything is invalidated.
func invalidations(ns string, r *http.Request, body []byte) []string {
	id := itemID(r)
	if id == "" && len(body) > 0 {
		if f, err := httpx.FormatOfContentType(r.Header.Get("Content-Type")); err == nil {
			var e model.Employee
			if httpx.DecodeBytesLoose(f, body, &e) == nil && model.ValidID(e.ID) {
				id = e.ID
			}
		}
	}
	if id == "" {
		return []string{genAll(ns)}
	}
	return []string{genItem(ns, id), genLists(ns)}
}

// cacheKey: path, canonical query (sorted, without format) and the
// negotiated format, so JSON and XML are separate entries whatever the
// Accept spelling. The validity is given by the counters inside the entry.
func cacheKey(ns string, r *http.Request, f httpx.Format) string {
	q := r.URL.Query()
	q.Del("format")
	return fmt.Sprintf("%s:GET:%s?%s:%s", ns, r.URL.Path, q.Encode(), f)
}

// ttl = min(CacheTTL, max-age of the answer); 0 when the answer forbids caching.
func (p *Proxy) ttl(h http.Header) time.Duration {
	cc := strings.ToLower(h.Get("Cache-Control"))
	if strings.Contains(cc, "no-store") || strings.Contains(cc, "private") || strings.Contains(cc, "no-cache") {
		return 0
	}
	ttl := p.cfg.CacheTTL
	for _, part := range strings.Split(cc, ",") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(part), "max-age="); ok {
			if n, err := strconv.Atoi(v); err == nil && time.Duration(n)*time.Second < ttl {
				ttl = time.Duration(n) * time.Second
			}
		}
	}
	return ttl
}

var cachedHeaders = []string{"Content-Type", "ETag", "Last-Modified", "Cache-Control", "Vary", "X-Served-By"}

func keepHeaders(h http.Header) map[string]string {
	m := map[string]string{}
	for _, k := range cachedHeaders {
		if v := h.Get(k); v != "" {
			m[k] = v
		}
	}
	return m
}

func (p *Proxy) writeEntry(w http.ResponseWriter, r *http.Request, e cache.Entry) outcome {
	h := w.Header()
	for k, v := range e.Header {
		h.Set(k, v)
	}
	h.Set("X-Cache", "HIT")
	h.Set("Age", strconv.Itoa(int(time.Since(e.StoredAt).Seconds())))
	h.Set("Via", "1.1 pad-proxy")
	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == e.Header["ETag"] {
		w.WriteHeader(http.StatusNotModified)
		return outcome{cache: "HIT", status: http.StatusNotModified}
	}
	h.Set("Content-Length", strconv.Itoa(len(e.Body)))
	w.WriteHeader(e.Status)
	_, _ = w.Write(e.Body)
	return outcome{cache: "HIT", status: e.Status}
}

// forward sends the request to a backend, retrying on another backend when
// that is safe (idempotent request) and useful (no answer, 502, 503, 504).
func (p *Proxy) forward(ctx context.Context, r *http.Request, body []byte) result {
	max := p.cfg.MaxAttempts
	if !idempotent(r) {
		max = 1
	}
	tried := map[*balancer.Backend]bool{}
	last := result{err: errNoBackend}
	for attempt := 1; attempt <= max; attempt++ {
		if attempt > 1 {
			p.Stats.Retries.Add(1)
			window := p.cfg.RetryBackoff << (attempt - 2)
			select {
			case <-ctx.Done():
				return last
			case <-time.After(time.Duration(rand.Int64N(int64(window) + 1))):
			}
		}
		b := p.pool.Pick(tried)
		if b == nil && len(tried) > 0 { // every backend tried: allow them again
			tried = map[*balancer.Backend]bool{}
			b = p.pool.Pick(tried)
		}
		if b == nil {
			p.Stats.NoBackend.Add(1)
			if last.err == nil {
				return last
			}
			last = result{err: errNoBackend, attempts: attempt}
			continue
		}
		tried[b] = true
		res := p.attempt(ctx, b, r, body)
		res.attempts = attempt
		last = res
		// Only the signals that the replica itself is unhealthy count against
		// it: no answer, 502, 503, 504. A 500 is an error of this request
		// (the replica answered): it neither opens the breaker nor is retried.
		nodeFailure := res.err != nil || res.status == http.StatusBadGateway ||
			res.status == http.StatusServiceUnavailable || res.status == http.StatusGatewayTimeout
		switch {
		case res.err != nil && errors.Is(ctx.Err(), context.Canceled):
			b.Breaker.Release() // the client left: says nothing about the backend
			return res
		case !nodeFailure:
			if res.status >= 500 {
				b.Breaker.Release()
			} else if b.Breaker.Success() {
				p.log.Info("circuit breaker closed", "event", "breaker_closed", "backend", b.Name)
			}
			return res
		}
		b.CountFailure()
		p.Stats.UpstreamErrors.Add(1)
		if b.Breaker.Failure() {
			p.Stats.BreakerOpened.Add(1)
			p.log.Warn("circuit breaker opened", "event", "breaker_open", "backend", b.Name,
				"correlation_id", logx.CorrelationID(ctx))
		}
		errText := ""
		if res.err != nil {
			errText = res.err.Error()
		}
		p.log.Warn("upstream attempt failed", "event", "upstream_failed", "backend", b.Name, "attempt", attempt,
			"status", res.status, "error", errText, "will_retry", attempt < max,
			"method", r.Method, "path", r.URL.Path, "correlation_id", logx.CorrelationID(ctx))
	}
	return last
}

var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Proxy-Connection"}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
	for _, f := range strings.Split(src.Get("Connection"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			dst.Del(f)
		}
	}
	for _, k := range hopByHop {
		dst.Del(k)
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// attempt sends one request to one backend, within UpstreamTimeout.
func (p *Proxy) attempt(ctx context.Context, b *balancer.Backend, r *http.Request, body []byte) result {
	actx, cancel := context.WithTimeout(ctx, p.cfg.UpstreamTimeout)
	defer cancel()
	done := b.Start()
	defer done()
	p.Stats.Upstream.Add(1)
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if info.Reused {
			p.Stats.ConnsReused.Add(1)
		} else {
			p.Stats.ConnsNew.Add(1)
		}
	}}
	u := *b.URL
	u.Path = strings.TrimSuffix(b.URL.Path, "/") + r.URL.Path
	u.RawPath = ""
	u.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(actx, trace), r.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		return result{err: err, backend: b.Name}
	}
	copyHeader(req.Header, r.Header)
	req.ContentLength = int64(len(body))
	req.Host = b.URL.Host
	if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		req.Header.Set("X-Forwarded-For", prior+", "+clientIP(r))
	} else {
		req.Header.Set("X-Forwarded-For", clientIP(r))
	}
	req.Header.Set("X-Forwarded-Host", r.Host)
	req.Header.Set("Via", "1.1 pad-proxy")
	req.Header.Set(logx.CorrelationHeader, logx.CorrelationID(ctx))
	resp, err := p.client.Do(req)
	if err != nil {
		return result{err: err, backend: b.Name}
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return result{err: err, backend: b.Name}
	}
	return result{status: resp.StatusCode, header: resp.Header, body: rb, backend: b.Name}
}

func (p *Proxy) writeResult(w http.ResponseWriter, r *http.Request, res result, cacheStatus string) outcome {
	out := outcome{cache: cacheStatus, backend: res.backend, attempts: res.attempts}
	h := w.Header()
	h.Set("X-Cache", cacheStatus)
	h.Set("Via", "1.1 pad-proxy")
	if res.attempts > 0 {
		h.Set("X-Attempts", strconv.Itoa(res.attempts))
	}
	if res.backend != "" {
		h.Set("X-Upstream", res.backend)
	}
	if res.err != nil {
		status, code := http.StatusBadGateway, "bad_gateway"
		switch {
		case errors.Is(res.err, errNoBackend):
			status, code = http.StatusServiceUnavailable, "no_backend"
		case errors.Is(res.err, context.DeadlineExceeded) || isTimeout(res.err):
			status, code = http.StatusGatewayTimeout, "upstream_timeout"
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			// a write without answer may have been applied
			h.Set("X-Outcome", "unknown")
		}
		out.status = status
		httpx.WriteError(w, r, status, code, res.err.Error())
		return out
	}
	copyHeader(h, res.header)
	h.Set("X-Cache", cacheStatus) // the upstream cannot override these
	h.Set("Via", "1.1 pad-proxy")
	h.Set("Content-Length", strconv.Itoa(len(res.body)))
	w.WriteHeader(res.status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(res.body)
	}
	out.status = res.status
	return out
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
