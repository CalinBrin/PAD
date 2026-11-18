package failure

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/datawarehouse/store"
	"pad-lab3/src/proxy/balancer"
	"pad-lab3/src/proxy/proxy"
	"pad-lab3/tests/testutil"
)

// A replica answering 503: an idempotent GET is retried on another replica.
func TestGetRetriedOnAnotherReplica(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2})
	c.Seed(t, "a", 1)
	c.Proxy.SetCacheEnabled(false)
	c.DWs[0].Faults.SetFail(true)
	for i := 0; i < 6; i++ {
		r := c.Do(t, "GET", "/employees/a", nil, nil)
		if r.Status != 200 || r.Header.Get("X-Upstream") != "dw-2" {
			t.Fatalf("GET %d: %d via %s", i, r.Status, r.Header.Get("X-Upstream"))
		}
	}
}

// A POST without Idempotency-Key is never retried (it could be applied
// twice); with a key it is retried and the warehouse deduplicates.
func TestPostRetriedOnlyWithIdempotencyKey(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2, BreakerThreshold: 100})
	c.Seed(t, "a", 1) // dw-1; next pick: dw-2
	c.DWs[1].Faults.SetFail(true)
	s := int64(5)
	r := c.Do(t, "POST", "/employees/a", model.EmployeePatch{Salary: &s}, nil)
	if r.Status != 503 || r.Header.Get("X-Attempts") != "1" {
		t.Fatalf("POST without key: %d attempts=%s", r.Status, r.Header.Get("X-Attempts"))
	}
	c.Do(t, "GET", "/employees/zz", nil, nil) // move round robin back to dw-2
	r = c.Do(t, "POST", "/employees/a", model.EmployeePatch{Salary: &s}, map[string]string{"Idempotency-Key": "p-1"})
	if r.Status != 200 || r.Header.Get("X-Attempts") != "2" || r.Header.Get("X-Upstream") != "dw-1" {
		t.Fatalf("POST with key: %d attempts=%s via %s", r.Status, r.Header.Get("X-Attempts"), r.Header.Get("X-Upstream"))
	}
}

// The answer of a PUT is lost after the commit; the retry on another replica
// is a no-op (same content): the version is not incremented twice.
func TestPutRetryAfterLostAnswerIsNoOp(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2,
		Proxy: proxy.Config{UpstreamTimeout: 200 * time.Millisecond, MaxAttempts: 2}})
	c.Seed(t, "a", 1) // dw-1; next: dw-2
	c.DWs[1].Faults.SetWriteDelay(500 * time.Millisecond)
	r := c.Do(t, "PUT", "/employees/a", map[string]any{"id": "a", "name": "Emp a", "department": "IT", "salary": 2}, nil)
	e := testutil.Employee(t, r)
	if r.Status != 200 || r.Header.Get("X-Attempts") != "2" || r.Header.Get("X-Changed") != "false" || e.Version != 2 {
		t.Fatalf("retried PUT: %d attempts=%s changed=%s version=%d", r.Status, r.Header.Get("X-Attempts"), r.Header.Get("X-Changed"), e.Version)
	}
}

// Remark 2. The change feed is a pure read (a cursor, no lease): retrying it
// after a timeout loses nothing and changes nothing.
func TestChangeFeedRetryHasNoSideEffect(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2,
		Proxy: proxy.Config{UpstreamTimeout: 150 * time.Millisecond, MaxAttempts: 3}})
	for _, id := range []string{"a", "b", "c"} {
		c.Seed(t, id, 1)
	}
	// dw-2 is slow: a request sent there first times out and is retried on dw-1
	c.DWs[1].Faults.SetLatency(400 * time.Millisecond)
	var r1 *httpx.Response
	for i := 0; i < 4 && (r1 == nil || r1.Header.Get("X-Attempts") != "2"); i++ {
		r1 = c.Do(t, "GET", "/updates/employees", nil, nil)
	}
	c.DWs[1].Faults.SetLatency(0)
	r2 := c.Do(t, "GET", "/updates/employees", nil, nil)
	var f1, f2 model.ChangeFeed
	_ = json.Unmarshal(r1.Body, &f1)
	_ = json.Unmarshal(r2.Body, &f2)
	if r1.Status != 200 || r1.Header.Get("X-Attempts") != "2" || len(f1.Items) != 3 {
		t.Fatalf("retried feed: %d attempts=%s items=%d", r1.Status, r1.Header.Get("X-Attempts"), len(f1.Items))
	}
	if len(f2.Items) != 3 || f1.Cursor != f2.Cursor {
		t.Fatalf("the feed must be repeatable: %v / %v", f1, f2)
	}
	for _, e := range f2.Items {
		if e.Version != 1 {
			t.Fatalf("reading the feed changed %s (version %d)", e.ID, e.Version)
		}
	}
}

// Remark 3. The active health check removes a replica that fails /readyz,
// even when no client request is sent, and re-adds it after recovery.
func TestActiveHealthCheckWithoutTraffic(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	c.DWs[1].Faults.SetNotReady(true)
	served := c.DWs[1].Faults.Served()
	c.Health.CheckAll(t.Context())
	c.Health.CheckAll(t.Context())
	if c.Pool.Backends()[1].Healthy() {
		t.Fatal("dw-2 must be out of rotation after 2 failed checks")
	}
	if c.DWs[1].Faults.Served() != served {
		t.Fatal("detection must not need client traffic")
	}
	c.Seed(t, "a", 1)
	c.Proxy.SetCacheEnabled(false)
	for i := 0; i < 10; i++ {
		if r := c.Do(t, "GET", "/employees/a", nil, nil); r.Header.Get("X-Upstream") == "dw-2" {
			t.Fatal("request sent to a replica out of rotation")
		}
	}
	c.DWs[1].Faults.SetNotReady(false)
	c.Health.CheckAll(t.Context())
	c.Health.CheckAll(t.Context())
	if !c.Pool.Backends()[1].Healthy() {
		t.Fatal("dw-2 must be back after 2 successful checks")
	}
}

func TestActiveHealthCheckDetectsStoppedReplica(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	c.DWs[2].Server.Close()
	c.Health.CheckAll(t.Context())
	c.Health.CheckAll(t.Context())
	if c.Pool.Backends()[2].Healthy() {
		t.Fatal("a stopped replica must leave the rotation")
	}
}

// Between two health checks (readyz still OK) the passive breaker reacts to
// the real failures and stops sending traffic to the replica.
func TestBreakerOpensBetweenHealthChecks(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2, BreakerThreshold: 3, BreakerCooldown: 300 * time.Millisecond})
	c.Seed(t, "a", 1)
	c.Proxy.SetCacheEnabled(false)
	c.DWs[0].Faults.SetFail(true)
	for i := 0; i < 12; i++ {
		if r := c.Do(t, "GET", "/employees/a", nil, nil); r.Status != 200 {
			t.Fatalf("GET %d: %d", i, r.Status)
		}
	}
	if st := c.Pool.Backends()[0].Breaker.State(); st != balancer.Open {
		t.Fatalf("breaker %s, expected open", st)
	}
	n := c.DWs[0].Faults.Served()
	for i := 0; i < 10; i++ {
		c.Do(t, "GET", "/employees/a", nil, nil)
	}
	if c.DWs[0].Faults.Served() != n {
		t.Fatal("an open breaker must stop the traffic to dw-1")
	}
	c.DWs[0].Faults.SetFail(false)
	time.Sleep(350 * time.Millisecond)
	for i := 0; i < 4; i++ {
		c.Do(t, "GET", "/employees/a", nil, nil)
	}
	if st := c.Pool.Backends()[0].Breaker.State(); st != balancer.Closed {
		t.Fatalf("after the cooldown the trial request must close the breaker, state %s", st)
	}
}

func TestAllReplicasDownGives503(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2})
	c.Seed(t, "a", 1)
	c.Mem.SetFail(true) // the database is down: every readyz fails
	c.Health.CheckAll(t.Context())
	c.Health.CheckAll(t.Context())
	r := c.Do(t, "GET", "/employees/nope", nil, nil)
	if r.Status != 503 {
		t.Fatalf("expected 503 no_backend, got %d %s", r.Status, r.Body)
	}
	// the cached entry is still served while every replica is down
	c.Mem.SetFail(false)
}

func TestCachedAnswersSurviveWhenEveryReplicaIsDown(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2})
	c.Seed(t, "a", 1)
	c.Do(t, "GET", "/employees/a", nil, nil)
	for _, d := range c.DWs {
		d.Server.Close()
	}
	r := c.Do(t, "GET", "/employees/a", nil, nil)
	if r.Status != 200 || r.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("cached answer: %d %s", r.Status, r.Header.Get("X-Cache"))
	}
	if r := c.Do(t, "GET", "/employees/b", nil, nil); r.Status != 502 && r.Status != 503 {
		t.Fatalf("uncached answer with every replica down: %d", r.Status)
	}
}

// brokenFeed makes the change-feed query fail inside the database (like the
// Cassandra SAI bug met during the Docker run) while everything else works.
type brokenFeed struct{ store.Store }

func (brokenFeed) ChangedSince(context.Context, time.Time, int) ([]model.Employee, error) {
	return nil, fmt.Errorf("%w: query rejected", store.ErrInternal)
}

// Regression: a request that always fails with 500 must not open the
// breakers of every replica (and take the whole service down).
func TestRequestErrorDoesNotTakeReplicasOutOfRotation(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3, BreakerThreshold: 2,
		Store: func(s store.Store) store.Store { return brokenFeed{s} }})
	c.Seed(t, "a", 1)
	for i := 0; i < 10; i++ {
		r := c.Do(t, "GET", "/updates/employees", nil, nil)
		if r.Status != 500 || r.Header.Get("X-Attempts") != "1" {
			t.Fatalf("feed: %d attempts=%s (500 is not retried)", r.Status, r.Header.Get("X-Attempts"))
		}
	}
	for _, b := range c.Pool.Backends() {
		if b.Breaker.State() != balancer.Closed {
			t.Fatalf("%s breaker %s after request errors", b.Name, b.Breaker.State())
		}
	}
	if r := c.Do(t, "GET", "/employees/a", nil, nil); r.Status != 200 {
		t.Fatalf("the service must still work: %d", r.Status)
	}
}
