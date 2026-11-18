package unit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/datawarehouse/dw"
	"pad-lab3/src/datawarehouse/store"
	"pad-lab3/src/proxy/balancer"
)

func TestNegotiate(t *testing.T) {
	cases := []struct {
		query, accept string
		want          httpx.Format
		ok            bool
	}{
		{"", "", httpx.JSON, true},
		{"", "application/json", httpx.JSON, true},
		{"", "application/xml", httpx.XML, true},
		{"", "text/xml", httpx.XML, true},
		{"", "*/*", httpx.JSON, true},
		{"", "application/xml;q=0.9, application/json;q=0.5", httpx.XML, true},
		{"", "application/json;q=0.2, application/xml", httpx.XML, true},
		{"", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", httpx.XML, true},
		{"", "text/html", httpx.JSON, false},
		{"", "application/json;q=0", httpx.JSON, false},
		{"format=xml", "application/json", httpx.XML, true},
		{"format=json", "application/xml", httpx.JSON, true},
		{"format=yaml", "", httpx.JSON, false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/employees/1?"+c.query, nil)
		if c.accept != "" {
			r.Header.Set("Accept", c.accept)
		}
		got, ok := httpx.Negotiate(r)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("query=%q accept=%q: got %s,%v want %s,%v", c.query, c.accept, got, ok, c.want, c.ok)
		}
	}
}

func TestEncodeDecodeRoundTripJSONAndXML(t *testing.T) {
	e := model.Employee{ID: "e-1", Name: "Ana Rusu", Department: "IT", Position: "Engineer",
		Email: "ana@example.md", Salary: 12000, Version: 3, UpdatedAt: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC), UpdatedBy: "src"}
	for _, f := range []httpx.Format{httpx.JSON, httpx.XML} {
		b, err := httpx.Encode(f, e)
		if err != nil {
			t.Fatal(err)
		}
		var got model.Employee
		if err := httpx.DecodeBytesLoose(f, b, &got); err != nil {
			t.Fatalf("%s: %v\n%s", f, err, b)
		}
		got.XMLName = e.XMLName
		if got != e {
			t.Fatalf("%s round trip: got %+v want %+v", f, got, e)
		}
	}
}

func TestValidate(t *testing.T) {
	bad := []model.Employee{
		{ID: "", Name: "x"},
		{ID: "a b", Name: "x"},
		{ID: "ok", Name: ""},
		{ID: "ok", Name: "x", Salary: -1},
		{ID: "ok", Name: "x", Email: "nope"},
	}
	for _, e := range bad {
		if err := e.Validate(); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("%+v: expected invalid, got %v", e, err)
		}
	}
	good := model.Employee{ID: "emp-01_x", Name: "Ion", Email: "ion@x.md", Salary: 0}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPatchIsAbsoluteSoApplyingTwiceGivesTheSameState(t *testing.T) {
	s := int64(5000)
	name := "Nou"
	p := model.EmployeePatch{Salary: &s, Name: &name}
	e := model.Employee{ID: "1", Name: "Vechi", Salary: 1}
	once := p.Apply(e)
	twice := p.Apply(once)
	if once != twice || once.Salary != 5000 || once.Name != "Nou" {
		t.Fatalf("patch not idempotent: %+v %+v", once, twice)
	}
}

// Thread safety: many goroutines write the same employee at once through the
// service; every successful change gets its own version, none is lost.
func TestConcurrentWritesGetConsecutiveVersions(t *testing.T) {
	svc := dw.NewService(store.NewMemory(), dw.Config{CASRetries: 1000})
	ctx := context.Background()
	if _, err := svc.Put(ctx, model.Employee{ID: "x", Name: "X", Salary: 0}, "t", -1); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var changed atomic.Int64
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := int64(i)
			res, err := svc.Patch(ctx, "x", model.EmployeePatch{Salary: &s}, "t", "")
			if err != nil {
				t.Error(err)
				return
			}
			if res.Changed {
				changed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	e, _ := svc.Get(ctx, "x")
	if e.Version != 1+changed.Load() {
		t.Fatalf("version %d, expected 1 + %d changes", e.Version, changed.Load())
	}
}

func TestPutSameContentIsNoOpAndIfMatchIsChecked(t *testing.T) {
	svc := dw.NewService(store.NewMemory(), dw.Config{})
	ctx := context.Background()
	in := model.Employee{ID: "a", Name: "A", Salary: 1}
	r1, _ := svc.Put(ctx, in, "s", -1)
	r2, _ := svc.Put(ctx, in, "s", -1)
	if !r1.Created || r2.Changed || r2.Employee.Version != 1 {
		t.Fatalf("second identical PUT must not change: %+v %+v", r1, r2)
	}
	in.Salary = 2
	if _, err := svc.Put(ctx, in, "s", 5); !errors.Is(err, dw.ErrPrecondition) {
		t.Fatalf("expected precondition failure, got %v", err)
	}
	r3, err := svc.Put(ctx, in, "s", 1)
	if err != nil || r3.Employee.Version != 2 {
		t.Fatalf("If-Match 1 should succeed: %v %+v", err, r3)
	}
}

func TestCancelledWriteIsNotApplied(t *testing.T) {
	svc := dw.NewService(store.NewMemory(), dw.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.Put(ctx, model.Employee{ID: "a", Name: "A"}, "s", -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := svc.Get(context.Background(), "a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a cancelled write must not be stored, got %v", err)
	}
}

func TestChangeFeedResendsOverlapWindow(t *testing.T) {
	svc := dw.NewService(store.NewMemory(), dw.Config{FeedOverlap: 5 * time.Second})
	now := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	ctx := context.Background()
	_, _ = svc.Put(ctx, model.Employee{ID: "a", Name: "A"}, "s", -1)
	items, cur, _ := svc.Changes(ctx, time.Time{})
	if len(items) != 1 || !cur.Equal(now) {
		t.Fatalf("first pull: %v %v", items, cur)
	}
	// a write stamped 2 s before the cursor (late commit / clock skew)
	svc.Now = func() time.Time { return now.Add(-2 * time.Second) }
	_, _ = svc.Put(ctx, model.Employee{ID: "b", Name: "B"}, "s", -1)
	items, _, _ = svc.Changes(ctx, cur)
	found := false
	for _, e := range items {
		found = found || e.ID == "b"
	}
	if !found {
		t.Fatalf("the overlap window must re-send b: %v", items)
	}
}

func TestBreakerTransitions(t *testing.T) {
	now := time.Unix(0, 0)
	b := balancer.NewBreaker(3, time.Second)
	b.Now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		b.Acquire()
		b.Failure()
	}
	if b.State() != balancer.Closed {
		t.Fatal("must stay closed below the threshold")
	}
	b.Acquire()
	if !b.Failure() || b.State() != balancer.Open {
		t.Fatal("must open at the threshold")
	}
	if b.Acquire() || b.Available() {
		t.Fatal("open breaker admits nothing during the cooldown")
	}
	now = now.Add(time.Second)
	if !b.Acquire() {
		t.Fatal("after the cooldown one trial is admitted")
	}
	if b.Acquire() {
		t.Fatal("only one trial at a time in half-open")
	}
	b.Failure()
	if b.State() != balancer.Open {
		t.Fatal("a failed trial reopens")
	}
	now = now.Add(time.Second)
	b.Acquire()
	b.Success()
	if b.State() != balancer.Closed {
		t.Fatal("a successful trial closes")
	}
}

func pool(t *testing.T, n int, algo string) *balancer.Pool {
	var specs []string
	for i := 1; i <= n; i++ {
		specs = append(specs, fmt.Sprintf("dw-%d=http://dw-%d:8000", i, i))
	}
	bs, err := balancer.ParseBackends(specs, func() *balancer.Breaker { return balancer.NewBreaker(3, time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	return balancer.NewPool(bs, algo)
}

func TestRoundRobinIsEvenAndSkipsUnhealthy(t *testing.T) {
	p := pool(t, 3, balancer.RoundRobin)
	count := map[string]int{}
	for i := 0; i < 30; i++ {
		count[p.Pick(nil).Name]++
	}
	if count["dw-1"] != 10 || count["dw-2"] != 10 || count["dw-3"] != 10 {
		t.Fatalf("round robin not even: %v", count)
	}
	p.Backends()[1].SetHealthy(false)
	for i := 0; i < 10; i++ {
		if p.Pick(nil).Name == "dw-2" {
			t.Fatal("unhealthy backend picked")
		}
	}
	p.Backends()[0].SetHealthy(false)
	p.Backends()[2].SetHealthy(false)
	if p.Pick(nil) != nil {
		t.Fatal("no backend must be returned when all are down")
	}
}

func TestLeastConnPrefersTheLeastBusy(t *testing.T) {
	p := pool(t, 3, balancer.LeastConn)
	b := p.Backends()
	d1, d2 := b[0].Start(), b[1].Start()
	defer d1()
	defer d2()
	for i := 0; i < 5; i++ {
		if got := p.Pick(nil).Name; got != "dw-3" {
			t.Fatalf("least_conn picked %s", got)
		}
	}
}

func TestHealthCheckerFallAndRise(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()
	bs, _ := balancer.ParseBackends([]string{"dw-1=" + srv.URL}, func() *balancer.Breaker { return balancer.NewBreaker(3, time.Second) })
	p := balancer.NewPool(bs, "")
	h := &balancer.HealthChecker{Pool: p, Timeout: time.Second, Fall: 2, Rise: 2, Client: srv.Client()}
	ready.Store(false)
	h.CheckAll(context.Background())
	if !bs[0].Healthy() {
		t.Fatal("one failed check must not remove the backend (Fall=2)")
	}
	h.CheckAll(context.Background())
	if bs[0].Healthy() {
		t.Fatal("two failed checks must remove the backend")
	}
	ready.Store(true)
	h.CheckAll(context.Background())
	if bs[0].Healthy() {
		t.Fatal("one success must not re-add it (Rise=2)")
	}
	h.CheckAll(context.Background())
	if !bs[0].Healthy() {
		t.Fatal("two successes must re-add it")
	}
}

func TestRoundRobinSpreadsTheShareOfAMissingBackendEvenly(t *testing.T) {
	p := pool(t, 3, balancer.RoundRobin)
	p.Backends()[1].SetHealthy(false)
	count := map[string]int{}
	for i := 0; i < 1000; i++ {
		count[p.Pick(nil).Name]++
	}
	if d := count["dw-1"] - count["dw-3"]; d > 10 || d < -10 {
		t.Fatalf("uneven split without dw-2: %v", count)
	}
}
