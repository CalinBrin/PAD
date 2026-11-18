// Failure tests. Each one reproduces a fault and checks the recovery.
package failure

import (
	"strings"
	"sync"
	"testing"
	"time"

	"pad-lab3/src/datawarehouse/store"
	"pad-lab3/src/proxy/proxy"
	"pad-lab3/tests/testutil"
)

func salaryOf(t *testing.T, c *testutil.Cluster, hdr map[string]string) (int64, string) {
	r := c.Do(t, "GET", "/employees/a", nil, hdr)
	if r.Status != 200 {
		t.Fatalf("GET: %d %s", r.Status, r.Body)
	}
	return testutil.Employee(t, r).Salary, r.Header.Get("X-Cache")
}

// Remark 1a. A GET that started before a PUT reads the old value; its answer
// arrives after the PUT invalidated the cache. Without generations the old
// value would be stored and served until the TTL. With generations it is
// stored under the old generation, which nobody reads.
func TestSlowGetStartedBeforePut_DoesNotCacheStaleValue(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2, Proxy: proxy.Config{CacheTTL: time.Minute}})
	c.Seed(t, "a", 100)                      // round robin: the next request goes to dw-2, then dw-1
	c.Do(t, "GET", "/employees/b", nil, nil) // dw-2 (404), so the slow GET lands on dw-1
	c.DWs[0].Faults.SetReadDelay(400 * time.Millisecond)

	var wg sync.WaitGroup
	var slowSalary int64
	wg.Add(1)
	go func() { // reads 100 on dw-1, answers 400 ms later
		defer wg.Done()
		slowSalary, _ = salaryOf(t, c, nil)
	}()
	time.Sleep(100 * time.Millisecond)
	if r := c.Do(t, "PUT", "/employees/a", map[string]any{"id": "a", "name": "Emp a", "department": "IT", "salary": 200}, nil); r.Status != 200 {
		t.Fatalf("PUT: %d %s", r.Status, r.Body)
	}
	wg.Wait()
	c.DWs[0].Faults.SetReadDelay(0)
	if slowSalary != 100 {
		t.Fatalf("the race did not happen: the slow GET read %d", slowSalary)
	}
	// the old answer was written to Redis, under an old generation
	gen, _ := c.Redis.Get("gen:employees")
	stale := false
	for _, k := range c.Redis.Keys() {
		if strings.HasPrefix(k, "cache:employees:") && strings.Contains(k, ":GET:/employees/a?") &&
			!strings.HasPrefix(k, "cache:employees:"+gen+":") {
			v, _ := c.Redis.Get(k)
			stale = stale || strings.Contains(v, "eyJpZCI6ImEi") // base64 of {"id":"a"
		}
	}
	if !stale {
		t.Fatalf("expected the stale answer under a generation older than %s, keys=%v", gen, c.Redis.Keys())
	}
	for i := 0; i < 3; i++ {
		if s, x := salaryOf(t, c, nil); s != 200 {
			t.Fatalf("stale value served from the cache: salary=%d X-Cache=%s", s, x)
		}
	}
}

// Remark 1b. A PUT whose answer is lost (timeout at the proxy) but which the
// warehouse applied must still invalidate the cache.
func TestPutTimesOutButApplied_CacheStillInvalidated(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 1,
		Proxy: proxy.Config{UpstreamTimeout: 200 * time.Millisecond, MaxAttempts: 1, CacheTTL: time.Minute}})
	c.Seed(t, "a", 100)
	salaryOf(t, c, nil)
	if _, x := salaryOf(t, c, nil); x != "HIT" {
		t.Fatal("expected a cached value")
	}
	c.DWs[0].Faults.SetWriteDelay(600 * time.Millisecond) // committed, answer late
	r := c.Do(t, "PUT", "/employees/a", map[string]any{"id": "a", "name": "Emp a", "department": "IT", "salary": 200}, nil)
	if r.Status != 504 || r.Header.Get("X-Outcome") != "unknown" {
		t.Fatalf("expected 504 with an unknown outcome, got %d %v", r.Status, r.Header)
	}
	if s, x := salaryOf(t, c, nil); s != 200 || x != "MISS" {
		t.Fatalf("after the timed-out PUT: salary=%d X-Cache=%s", s, x)
	}
}

// Remark 1b, harder case: the write lands in the database AFTER the proxy
// gave up and invalidated (the database write was already in flight). A read
// in between may cache the old value; the delayed invalidation (WriteGrace)
// removes it. The staleness is bounded by WriteGrace, not by the TTL.
func TestWriteCommittedAfterProxyGaveUp_DelayedInvalidationBoundsStaleness(t *testing.T) {
	var slow *testutil.SlowStore
	c := testutil.Start(t, testutil.Options{Replicas: 1,
		Store: func(s store.Store) store.Store { slow = &testutil.SlowStore{Store: s}; return slow },
		Proxy: proxy.Config{UpstreamTimeout: 100 * time.Millisecond, MaxAttempts: 1, CacheTTL: time.Minute,
			WriteGrace: 700 * time.Millisecond}})
	c.Seed(t, "a", 100)
	slow.SetCASDelay(300 * time.Millisecond)
	start := time.Now()
	r := c.Do(t, "PUT", "/employees/a", map[string]any{"id": "a", "name": "Emp a", "department": "IT", "salary": 200}, nil)
	if r.Status != 504 {
		t.Fatalf("expected a timeout, got %d", r.Status)
	}
	// before the commit (t < 300 ms): a read caches the old value under the new generation
	if s, _ := salaryOf(t, c, nil); s != 100 {
		t.Fatalf("read before the commit: %d", s)
	}
	time.Sleep(time.Until(start.Add(450 * time.Millisecond)))
	s, x := salaryOf(t, c, nil)
	t.Logf("between commit and delayed invalidation: salary=%d X-Cache=%s (stale window)", s, x)
	time.Sleep(time.Until(start.Add(900 * time.Millisecond)))
	if s, x := salaryOf(t, c, nil); s != 200 {
		t.Fatalf("after WriteGrace the value must be fresh: salary=%d X-Cache=%s", s, x)
	}
	if c.Proxy.Stats.DelayedInvalidations.Load() != 1 {
		t.Fatal("expected one delayed invalidation")
	}
}

// Redis unreachable: the proxy keeps serving (without cache). A write made
// while Redis is unreachable could not invalidate; when Redis is back (with
// its data: a network partition, not a restart) the generation is bumped
// before the cache is used again, so the old entry is not served.
func TestRedisDown_ProxyServesWithoutCache_AndLostInvalidationIsReplayed(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2})
	c.Seed(t, "a", 100)
	salaryOf(t, c, nil)
	if _, x := salaryOf(t, c, nil); x != "HIT" {
		t.Fatal("expected a cached value")
	}
	c.Redis.Close()
	start := time.Now()
	s, x := salaryOf(t, c, nil)
	if s != 100 || x != "BYPASS" {
		t.Fatalf("Redis down: salary=%d X-Cache=%s", s, x)
	}
	if r := c.Do(t, "PUT", "/employees/a", map[string]any{"id": "a", "name": "Emp a", "department": "IT", "salary": 200}, nil); r.Status != 200 {
		t.Fatalf("writes must work without Redis: %d", r.Status)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Redis down must not slow the requests down: %v", time.Since(start))
	}
	if c.Proxy.Stats.InvalidationErr.Load() == 0 {
		t.Fatal("expected a failed invalidation")
	}
	if err := c.Redis.Restart(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // fail-fast window (Backoff = 50 ms in tests)
	if s, x := salaryOf(t, c, nil); s != 200 {
		t.Fatalf("old entry served after Redis came back: salary=%d X-Cache=%s", s, x)
	}
}
