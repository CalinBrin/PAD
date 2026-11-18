package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pad-lab3/src/common/model"
	"pad-lab3/src/datawarehouse/dw"
	"pad-lab3/src/datawarehouse/store"
)

// The Cassandra store, against a real Cassandra (docker compose run --rm tests).
// Skipped when CASSANDRA_HOSTS is not set.
func cassandra(t *testing.T) *store.Cassandra {
	hosts := os.Getenv("CASSANDRA_HOSTS")
	if hosts == "" {
		t.Skip("CASSANDRA_HOSTS not set (runs in docker compose run --rm tests)")
	}
	cfg := store.CassandraConfig{Hosts: strings.Split(hosts, ","), Keyspace: "pad_lab3_test", Timeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := store.Migrate(ctx, cfg, 1, func(string, ...any) {}); err != nil {
		t.Fatal(err)
	}
	c, err := store.OpenCassandra(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestCassandraStore(t *testing.T) {
	c := cassandra(t)
	ctx := context.Background()
	id := fmt.Sprintf("t-%d", time.Now().UnixNano())
	now := time.Now().UTC().Truncate(time.Millisecond)
	e := model.Employee{ID: id, Name: "Ana", Salary: 1, Version: 1, UpdatedAt: now, UpdatedBy: "test"}
	if err := c.CompareAndSet(ctx, e, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.CompareAndSet(ctx, e, 0); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("IF NOT EXISTS must fail the second time: %v", err)
	}
	got, err := c.Get(ctx, id)
	if err != nil || got.Name != "Ana" || got.Version != 1 || !got.UpdatedAt.Equal(now) {
		t.Fatalf("get: %v %+v", err, got)
	}
	e.Version, e.Salary = 2, 2
	if err := c.CompareAndSet(ctx, e, 5); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("IF version = 5 must fail: %v", err)
	}
	if err := c.CompareAndSet(ctx, e, 1); err != nil {
		t.Fatal(err)
	}
	changed, err := c.ChangedSince(ctx, now.Add(-time.Second), 1000)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range changed {
		found = found || (x.ID == id && x.Version == 2)
	}
	if !found {
		t.Fatalf("ChangedSince (SAI index) must return %s", id)
	}
	if _, ok, _ := c.GetIdempotent(ctx, "k-"+id); ok {
		t.Fatal("unknown key")
	}
	if err := c.SaveIdempotent(ctx, "k-"+id, store.IdemRecord{RequestHash: "h", Status: 200, Employee: e}); err != nil {
		t.Fatal(err)
	}
	if r, ok, err := c.GetIdempotent(ctx, "k-"+id); err != nil || !ok || r.Employee.Salary != 2 {
		t.Fatalf("idempotency record: %v %v %+v", err, ok, r)
	}
	if _, total, err := c.List(ctx, 0, 10); err != nil || total < 1 {
		t.Fatalf("list: %v %d", err, total)
	}
}

// Two services (two warehouse replicas) write the same employee at the same
// time through Cassandra lightweight transactions: no change is lost.
func TestCassandraConcurrentReplicasLoseNoUpdate(t *testing.T) {
	c := cassandra(t)
	ctx := context.Background()
	replicas := []*dw.Service{dw.NewService(c, dw.Config{CASRetries: 100}), dw.NewService(c, dw.Config{CASRetries: 100})}
	id := fmt.Sprintf("cc-%d", time.Now().UnixNano())
	if _, err := replicas[0].Put(ctx, model.Employee{ID: id, Name: "X"}, "t", -1); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var changed atomic.Int64
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := int64(i)
			res, err := replicas[i%2].Patch(ctx, id, model.EmployeePatch{Salary: &s}, "t", "")
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
	e, err := c.Get(ctx, id)
	if err != nil || e.Version != 1+changed.Load() {
		t.Fatalf("version %d after %d changes (%v)", e.Version, changed.Load(), err)
	}
}
