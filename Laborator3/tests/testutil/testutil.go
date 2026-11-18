// Package testutil starts a real system in one process: warehouse replicas
// (real HTTP servers, sharing one store as they share Cassandra), a real
// Redis protocol server (miniredis) and the proxy with its listener, cache,
// balancer and client registry.
package testutil

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/datawarehouse/dw"
	"pad-lab3/src/datawarehouse/store"
	"pad-lab3/src/proxy/balancer"
	"pad-lab3/src/proxy/cache"
	"pad-lab3/src/proxy/proxy"
)

// DW is one warehouse replica.
type DW struct {
	Name   string
	Server *httptest.Server
	Faults *dw.Faults
}

// Options of the cluster.
type Options struct {
	Replicas          int
	NoRedis           bool
	Proxy             proxy.Config
	Algorithm         string
	BreakerThreshold  int
	BreakerCooldown   time.Duration
	MaxConnsPerClient int
	// Store wraps the shared store (to inject storage faults); nil keeps it.
	Store func(store.Store) store.Store
}

// Cluster is the running system.
type Cluster struct {
	Mem     *store.Memory
	DWs     []*DW
	Redis   *miniredis.Miniredis
	Cache   *cache.Redis
	Pool    *balancer.Pool
	Health  *balancer.HealthChecker
	Proxy   *proxy.Proxy
	Reg     *proxy.Registry
	Limit   *proxy.LimitListener
	Server  *httptest.Server
	Admin   *httptest.Server
	cancel  context.CancelFunc
	regDone chan struct{}
}

// Start builds the cluster and stops it at the end of the test.
func Start(t testing.TB, o Options) *Cluster {
	t.Helper()
	if o.Replicas == 0 {
		o.Replicas = 3
	}
	if o.BreakerThreshold == 0 {
		o.BreakerThreshold = 3
	}
	if o.BreakerCooldown == 0 {
		o.BreakerCooldown = 300 * time.Millisecond
	}
	if o.Proxy.RetryBackoff == 0 {
		o.Proxy.RetryBackoff = 5 * time.Millisecond
	}
	c := &Cluster{Mem: store.NewMemory()}
	var shared store.Store = c.Mem
	if o.Store != nil {
		shared = o.Store(shared)
	}
	svc := dw.NewService(shared, dw.Config{FeedOverlap: 5 * time.Second})
	var specs []string
	for i := 1; i <= o.Replicas; i++ {
		name := "dw-" + string(rune('0'+i))
		f := &dw.Faults{}
		ctrl := dw.NewController(svc, dw.ControllerConfig{Node: name, CacheMaxAge: 30, StoreTimeout: 2 * time.Second}, f, logx.Discard())
		srv := httptest.NewServer(ctrl.Handler())
		c.DWs = append(c.DWs, &DW{Name: name, Server: srv, Faults: f})
		specs = append(specs, name+"="+srv.URL)
	}
	backends, err := balancer.ParseBackends(specs, func() *balancer.Breaker {
		return balancer.NewBreaker(o.BreakerThreshold, o.BreakerCooldown)
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Pool = balancer.NewPool(backends, o.Algorithm)
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	if !o.NoRedis {
		c.Redis = miniredis.RunT(t)
		c.Cache = cache.NewRedis(c.Redis.Addr(), 200*time.Millisecond)
		c.Cache.Backoff = 50 * time.Millisecond
		c.Reg = proxy.NewRegistry("proxy-test", c.Cache.Client(), logx.Discard())
		c.regDone = make(chan struct{})
		go func() { c.Reg.Run(ctx); close(c.regDone) }()
		if !o.Proxy.CacheEnabled {
			o.Proxy.CacheEnabled = true
		}
	} else {
		c.Reg = proxy.NewRegistry("proxy-test", nil, logx.Discard())
	}
	c.Proxy = proxy.New(o.Proxy, c.Pool, c.Cache, c.Reg, logx.Discard())
	c.Health = &balancer.HealthChecker{Pool: c.Pool, Timeout: 300 * time.Millisecond, Fall: 2, Rise: 2}
	c.Server = httptest.NewUnstartedServer(httpx.Middleware(logx.Discard(), c.Proxy))
	c.Limit = &proxy.LimitListener{Listener: c.Server.Listener, MaxPerClient: o.MaxConnsPerClient, Reg: c.Reg}
	c.Server.Listener = c.Limit
	c.Server.Start()
	c.Admin = httptest.NewServer(c.Proxy.AdminHandler(c.Limit))
	t.Cleanup(c.Close)
	return c
}

// Close stops everything.
func (c *Cluster) Close() {
	c.Server.Close()
	c.Admin.Close()
	for _, d := range c.DWs {
		d.Server.Close()
	}
	c.cancel()
	if c.regDone != nil {
		<-c.regDone
	}
	if c.Cache != nil {
		_ = c.Cache.Close()
	}
}

// FlushRegistry waits until the asynchronous registry writes reached Redis.
func (c *Cluster) FlushRegistry() { time.Sleep(250 * time.Millisecond) }

// Client returns a keep-alive client of the proxy.
func (c *Cluster) Client(f httpx.Format) *httpx.Client {
	cl := httpx.NewClient(c.Server.URL, f, 10*time.Second)
	cl.From = "test@local"
	return cl
}

// Do sends a request through the proxy in JSON.
func (c *Cluster) Do(t testing.TB, method, path string, body any, hdr map[string]string) *httpx.Response {
	t.Helper()
	r, err := c.Client(httpx.JSON).Do(context.Background(), method, path, body, hdr)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return r
}

// Raw sends a request with a raw body and headers (contract tests).
func Raw(t testing.TB, base, method, path, body string, hdr map[string]string) *httpx.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, base+path, rd)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return &httpx.Response{Status: resp.StatusCode, Header: resp.Header, Body: b}
}

// Employee decodes a JSON employee.
func Employee(t testing.TB, r *httpx.Response) model.Employee {
	t.Helper()
	var e model.Employee
	if err := httpx.DecodeBytesLoose(httpx.JSON, r.Body, &e); err != nil {
		t.Fatalf("decode employee: %v (%s)", err, r.Body)
	}
	return e
}

// Seed PUTs one employee and fails the test on error.
func (c *Cluster) Seed(t testing.TB, id string, salary int64) model.Employee {
	t.Helper()
	r := c.Do(t, http.MethodPut, "/employees/"+id, model.Employee{ID: id, Name: "Emp " + id, Department: "IT", Salary: salary}, nil)
	if r.Status != http.StatusCreated && r.Status != http.StatusOK {
		t.Fatalf("seed %s: HTTP %d %s", id, r.Status, r.Body)
	}
	return Employee(t, r)
}

// Served returns how many data requests each replica served.
func (c *Cluster) Served() map[string]int64 {
	m := map[string]int64{}
	for _, d := range c.DWs {
		m[d.Name] = d.Faults.Served()
	}
	return m
}

// SlowStore delays CompareAndSet without honouring the context: the write
// lands in the database after the caller gave up (like a Cassandra write
// that was already sent when the request was cancelled).
type SlowStore struct {
	store.Store
	mu    sync.Mutex
	delay time.Duration
}

func (s *SlowStore) SetCASDelay(d time.Duration) { s.mu.Lock(); s.delay = d; s.mu.Unlock() }

func (s *SlowStore) CompareAndSet(ctx context.Context, e model.Employee, expected int64) error {
	s.mu.Lock()
	d := s.delay
	s.mu.Unlock()
	time.Sleep(d)
	return s.Store.CompareAndSet(context.WithoutCancel(ctx), e, expected)
}

// DialRaw opens a raw TCP connection to the proxy.
func (c *Cluster) DialRaw(t testing.TB) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", c.Server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
