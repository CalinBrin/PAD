// Integration tests: proxy + warehouse replicas + Redis, real HTTP.
package integration

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/proxy/balancer"
	"pad-lab3/src/source-node/source"
	"pad-lab3/tests/testutil"
)

func TestRoundRobinSpreadsRequestsEvenly(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	c.Seed(t, "a", 1)
	c.Proxy.SetCacheEnabled(false)
	before := c.Served()
	for i := 0; i < 30; i++ {
		if r := c.Do(t, "GET", "/employees/a", nil, nil); r.Status != 200 || r.Header.Get("X-Cache") != "BYPASS" {
			t.Fatalf("GET: %d %s", r.Status, r.Header.Get("X-Cache"))
		}
	}
	after := c.Served()
	for _, d := range c.DWs {
		if n := after[d.Name] - before[d.Name]; n != 10 {
			t.Fatalf("round robin: %s served %d of 30 (%v)", d.Name, n, after)
		}
	}
}

func TestCacheMissThenHitOneUpstreamCall(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	c.Seed(t, "a", 1)
	up := c.Proxy.Stats.Upstream.Load()
	r1 := c.Do(t, "GET", "/employees/a", nil, nil)
	for i := 0; i < 20; i++ {
		r := c.Do(t, "GET", "/employees/a", nil, nil)
		if r.Header.Get("X-Cache") != "HIT" || string(r.Body) != string(r1.Body) {
			t.Fatalf("expected HIT with the same body: %v", r.Header.Get("X-Cache"))
		}
	}
	if r1.Header.Get("X-Cache") != "MISS" || c.Proxy.Stats.Upstream.Load()-up != 1 {
		t.Fatalf("21 GETs must reach the warehouse once (got %d)", c.Proxy.Stats.Upstream.Load()-up)
	}
}

func TestCacheKeepsJSONAndXMLApart(t *testing.T) {
	c := testutil.Start(t, testutil.Options{})
	c.Seed(t, "a", 1)
	j := testutil.Raw(t, c.Server.URL, "GET", "/employees/a", "", map[string]string{"Accept": "application/json"})
	x := testutil.Raw(t, c.Server.URL, "GET", "/employees/a", "", map[string]string{"Accept": "text/xml"})
	x2 := testutil.Raw(t, c.Server.URL, "GET", "/employees/a?format=xml", "", nil)
	if j.Header.Get("X-Cache") != "MISS" || x.Header.Get("X-Cache") != "MISS" || x2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("json/xml entries: %s %s %s", j.Header.Get("X-Cache"), x.Header.Get("X-Cache"), x2.Header.Get("X-Cache"))
	}
	if !strings.HasPrefix(string(x2.Body), "<?xml") || !strings.HasPrefix(string(j.Body), "{") {
		t.Fatal("wrong representation served from cache")
	}
}

func TestWriteThroughProxyInvalidatesItemAndLists(t *testing.T) {
	c := testutil.Start(t, testutil.Options{})
	c.Seed(t, "a", 1)
	c.Do(t, "GET", "/employees/a", nil, nil)
	c.Do(t, "GET", "/employees?limit=5", nil, nil)
	if c.Do(t, "GET", "/employees/a", nil, nil).Header.Get("X-Cache") != "HIT" {
		t.Fatal("expected HIT before the write")
	}
	s := int64(777)
	r := c.Do(t, "POST", "/employees/a", model.EmployeePatch{Salary: &s}, map[string]string{"Idempotency-Key": "k1"})
	if r.Status != 200 || r.Header.Get("X-Cache") != "INVALIDATE" {
		t.Fatalf("POST: %d %s", r.Status, r.Header.Get("X-Cache"))
	}
	g := c.Do(t, "GET", "/employees/a", nil, nil)
	if g.Header.Get("X-Cache") != "MISS" || testutil.Employee(t, g).Salary != 777 {
		t.Fatalf("after the write: %s %s", g.Header.Get("X-Cache"), g.Body)
	}
	l := c.Do(t, "GET", "/employees?limit=5", nil, nil)
	if l.Header.Get("X-Cache") != "MISS" || !strings.Contains(string(l.Body), `"salary":777`) {
		t.Fatalf("the list must be invalidated too: %s %s", l.Header.Get("X-Cache"), l.Body)
	}
}

func TestClientCanBypassCacheWithNoCache(t *testing.T) {
	c := testutil.Start(t, testutil.Options{})
	c.Seed(t, "a", 1)
	c.Do(t, "GET", "/employees/a", nil, nil)
	r := c.Do(t, "GET", "/employees/a", nil, map[string]string{"Cache-Control": "no-cache"})
	if r.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("Cache-Control: no-cache must revalidate: %s", r.Header.Get("X-Cache"))
	}
}

func TestChangeFeedIsNeverCached(t *testing.T) {
	c := testutil.Start(t, testutil.Options{})
	c.Seed(t, "a", 1)
	for i := 0; i < 3; i++ {
		if r := c.Do(t, "GET", "/updates/employees", nil, nil); r.Header.Get("X-Cache") != "BYPASS" {
			t.Fatalf("feed cached: %s", r.Header.Get("X-Cache"))
		}
	}
}

// Etapa 1 + 2 end to end: source nodes publish (JSON and XML) through the
// proxy, a client modifies an employee with POST, the owner pulls the change.
func TestSourceNodesPublishAndPullClientModification(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	ctx := context.Background()
	js := source.New(source.Config{Name: "src-json", IDPrefix: "j", Seed: 5, Poll: time.Second},
		httpx.NewClient(c.Server.URL, httpx.JSON, 5*time.Second), logx.Discard())
	xs := source.New(source.Config{Name: "src-xml", IDPrefix: "x", Seed: 5, Poll: time.Second},
		httpx.NewClient(c.Server.URL, httpx.XML, 5*time.Second), logx.Discard())
	if err := js.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := xs.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	l := c.Do(t, "GET", "/employees?limit=100", nil, nil)
	var page model.EmployeeList
	_ = httpx.DecodeBytesLoose(httpx.JSON, l.Body, &page)
	if page.Total != 10 {
		t.Fatalf("10 employees expected, got %d", page.Total)
	}
	// first pull: nothing new for the owners (they wrote it)
	if n, err := xs.PullOnce(ctx); err != nil || n != 0 {
		t.Fatalf("first pull: %d %v", n, err)
	}
	s := int64(99999)
	if r := c.Do(t, "POST", "/employees/x-003", model.EmployeePatch{Salary: &s}, map[string]string{"Idempotency-Key": "c-1"}); r.Status != 200 {
		t.Fatalf("POST: %d %s", r.Status, r.Body)
	}
	n, err := xs.PullOnce(ctx)
	if err != nil || n != 1 || xs.Snapshot().Employees["x-003"].Salary != 99999 {
		t.Fatalf("xml source must pull the change: %d %v %+v", n, err, xs.Snapshot().Employees["x-003"])
	}
	if n, _ := js.PullOnce(ctx); n != 0 {
		t.Fatal("json source must ignore employees it does not own")
	}
	// pulling again re-sends the overlap window but applies nothing twice
	if n, _ := xs.PullOnce(ctx); n != 0 {
		t.Fatalf("second pull applied %d changes again", n)
	}
}

// Smart proxy towards the warehouse: connections are pooled and reused.
func TestUpstreamConnectionsAreReused(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	c.Seed(t, "a", 1)
	c.Proxy.SetCacheEnabled(false)
	for i := 0; i < 60; i++ {
		c.Do(t, "GET", "/employees/a", nil, nil)
	}
	v := c.Proxy.View(nil)
	if v.Upstream.ConnsNew > 6 || v.Upstream.ConnsReused < 55 {
		t.Fatalf("connections not reused: new=%d reused=%d", v.Upstream.ConnsNew, v.Upstream.ConnsReused)
	}
}

// Smart proxy towards the clients: one keep-alive connection carries many
// requests; the registry in Redis records the connection and each request.
func TestRegistryRecordsClientConnectionsAndRequests(t *testing.T) {
	c := testutil.Start(t, testutil.Options{})
	c.Seed(t, "a", 1)
	conn := c.DialRaw(t)
	defer conn.Close()
	br := bufio.NewReader(conn)
	for i := 0; i < 5; i++ {
		fmt.Fprintf(conn, "GET /employees/a HTTP/1.1\r\nHost: x\r\nX-Correlation-ID: reg-%d\r\nX-Client-ID: tester\r\n\r\n", i)
		resp, err := http.ReadResponse(br, nil)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("request %d on the same connection: %v", i, err)
		}
		_, _ = bufio.NewReader(resp.Body).WriteTo(new(strings.Builder))
		resp.Body.Close()
	}
	c.FlushRegistry()
	cs, err := c.Reg.Shared(context.Background())
	if err != nil || len(cs) != 1 {
		t.Fatalf("clients: %v %+v", err, cs)
	}
	// the seed used another keep-alive connection, idle but still open:
	// 2 connections, both open, 6 requests
	if cs[0].Requests != 6 || cs[0].ConnsTotal != 2 || cs[0].OpenConns != 2 {
		t.Fatalf("registry: %+v", cs[0])
	}
	rec, err := c.Reg.Request(context.Background(), "reg-4")
	if err != nil || rec["status"] != "200" || rec["cache"] != "HIT" || rec["client_id"] != "tester" || rec["state"] != "done" {
		t.Fatalf("request record: %v %v", err, rec)
	}
}

func TestPerClientConnectionLimit(t *testing.T) {
	c := testutil.Start(t, testutil.Options{MaxConnsPerClient: 2})
	a, b := c.DialRaw(t), c.DialRaw(t)
	defer a.Close()
	defer b.Close()
	time.Sleep(50 * time.Millisecond)
	third := c.DialRaw(t)
	defer third.Close()
	_ = third.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := third.Read(make([]byte, 1)); err == nil {
		t.Fatal("the third connection of the same client must be closed")
	}
	c.FlushRegistry()
	if cs := c.Reg.Local(); len(cs) != 1 || cs[0].ConnsRejected != 1 || cs[0].OpenConns != 2 {
		t.Fatalf("registry: %+v", cs)
	}
}

func TestLeastConnAvoidsTheSlowReplica(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3, Algorithm: balancer.LeastConn})
	c.Seed(t, "a", 1)
	c.Proxy.SetCacheEnabled(false)
	c.DWs[0].Faults.SetLatency(150 * time.Millisecond)
	before := c.Served()
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				c.Do(t, "GET", "/employees/a", nil, nil)
			}
		}()
	}
	wg.Wait()
	after := c.Served()
	slow := after["dw-1"] - before["dw-1"]
	if slow > 15 {
		t.Fatalf("least_conn sent %d of 60 requests to the slow replica (%v)", slow, after)
	}
}

func TestConcurrentWritesThroughDifferentReplicasLoseNothing(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 3})
	c.Seed(t, "a", 0)
	var wg sync.WaitGroup
	var mu sync.Mutex
	changed := 0
	for i := 1; i <= 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := int64(i)
			r := c.Do(t, "POST", "/employees/a", model.EmployeePatch{Salary: &s}, nil)
			if r.Status != 200 {
				t.Errorf("POST: %d %s", r.Status, r.Body)
			}
			if r.Header.Get("X-Changed") == "true" {
				mu.Lock()
				changed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	e := testutil.Employee(t, c.Do(t, "GET", "/employees/a", nil, map[string]string{"Cache-Control": "no-cache"}))
	if e.Version != int64(1+changed) {
		t.Fatalf("version %d after %d changes", e.Version, changed)
	}
}

// A write invalidates the employee it touches and the lists, not the others.
func TestWriteInvalidatesOnlyTheTouchedEmployee(t *testing.T) {
	c := testutil.Start(t, testutil.Options{})
	c.Seed(t, "a", 1)
	c.Seed(t, "b", 1)
	for _, p := range []string{"/employees/a", "/employees/b", "/employee?id=a", "/employees?limit=5"} {
		c.Do(t, "GET", p, nil, nil)
	}
	c.Do(t, "PUT", "/employees/b", model.Employee{ID: "b", Name: "Emp b", Department: "IT", Salary: 2}, nil)
	want := map[string]string{"/employees/a": "HIT", "/employee?id=a": "HIT", "/employees/b": "MISS", "/employees?limit=5": "MISS"}
	for p, x := range want {
		if got := c.Do(t, "GET", p, nil, nil).Header.Get("X-Cache"); got != x {
			t.Errorf("%s after PUT b: %s, want %s", p, got, x)
		}
	}
	// PUT /employee/ with the id only in the body (XML) still invalidates a
	r := testutil.Raw(t, c.Server.URL, "PUT", "/employee/", `<employee id="a"><name>Nou</name></employee>`,
		map[string]string{"Content-Type": "application/xml"})
	if r.Status != 200 {
		t.Fatalf("PUT /employee/: %d %s", r.Status, r.Body)
	}
	g := c.Do(t, "GET", "/employee?id=a", nil, nil)
	if g.Header.Get("X-Cache") != "MISS" || testutil.Employee(t, g).Name != "Nou" {
		t.Fatalf("alias entry not invalidated: %s %s", g.Header.Get("X-Cache"), g.Body)
	}
}
