// Package store is the persistence of the data warehouse. Every warehouse
// replica uses the same Store (one Cassandra cluster), so the replicas never
// synchronise data between them (ADR 0001).
package store

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"pad-lab3/src/common/model"
)

var (
	// ErrNotFound: no employee with this id.
	ErrNotFound = errors.New("not found")
	// ErrConflict: the compare-and-set lost against a concurrent writer.
	ErrConflict = errors.New("version conflict")
	// ErrUnavailable: the database cannot be reached or did not answer in
	// time (the node is unhealthy: HTTP 503, the proxy may retry elsewhere).
	ErrUnavailable = errors.New("storage unavailable")
	// ErrInternal: the database rejected this request (HTTP 500: a problem of
	// the request, not of the node; the proxy does not count it against the node).
	ErrInternal = errors.New("storage error")
)

// IdemRecord is the stored answer of a POST sent with an Idempotency-Key.
type IdemRecord struct {
	RequestHash string
	Status      int
	Employee    model.Employee
}

// Store is implemented by Cassandra (production) and Memory (tests).
type Store interface {
	Get(ctx context.Context, id string) (model.Employee, error)
	// List returns one page in a stable, store-defined order and the total count.
	List(ctx context.Context, offset, limit int) ([]model.Employee, int, error)
	// CompareAndSet writes e only if the stored version equals expected
	// (expected 0: the employee must not exist). ErrConflict otherwise.
	CompareAndSet(ctx context.Context, e model.Employee, expected int64) error
	// ChangedSince returns the employees with UpdatedAt > since (at most max).
	ChangedSince(ctx context.Context, since time.Time, max int) ([]model.Employee, error)
	GetIdempotent(ctx context.Context, key string) (IdemRecord, bool, error)
	SaveIdempotent(ctx context.Context, key string, rec IdemRecord) error
	Ping(ctx context.Context) error
	Close()
}

// Memory is a thread-safe in-memory Store, used by the tests and by
// `DW_STORE=memory` (one replica only: it is not shared).
type Memory struct {
	mu   sync.RWMutex
	emps map[string]model.Employee
	idem map[string]IdemRecord
	// Fail makes every call return ErrUnavailable (fault injection in tests).
	fail bool
}

func NewMemory() *Memory {
	return &Memory{emps: map[string]model.Employee{}, idem: map[string]IdemRecord{}}
}

// SetFail switches the simulated database outage on or off.
func (m *Memory) SetFail(v bool) { m.mu.Lock(); m.fail = v; m.mu.Unlock() }

func (m *Memory) Get(_ context.Context, id string) (model.Employee, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fail {
		return model.Employee{}, ErrUnavailable
	}
	e, ok := m.emps[id]
	if !ok {
		return model.Employee{}, ErrNotFound
	}
	return e, nil
}

func (m *Memory) List(_ context.Context, offset, limit int) ([]model.Employee, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fail {
		return nil, 0, ErrUnavailable
	}
	ids := make([]string, 0, len(m.emps))
	for id := range m.emps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := []model.Employee{}
	for i := offset; i < len(ids) && len(out) < limit; i++ {
		out = append(out, m.emps[ids[i]])
	}
	return out, len(ids), nil
}

func (m *Memory) CompareAndSet(_ context.Context, e model.Employee, expected int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return ErrUnavailable
	}
	cur, ok := m.emps[e.ID]
	if (expected == 0 && ok) || (expected != 0 && (!ok || cur.Version != expected)) {
		return ErrConflict
	}
	m.emps[e.ID] = e
	return nil
}

func (m *Memory) ChangedSince(_ context.Context, since time.Time, max int) ([]model.Employee, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fail {
		return nil, ErrUnavailable
	}
	out := []model.Employee{}
	for _, e := range m.emps {
		if e.UpdatedAt.After(since) {
			out = append(out, e)
		}
	}
	SortByUpdate(out)
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (m *Memory) GetIdempotent(_ context.Context, key string) (IdemRecord, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fail {
		return IdemRecord{}, false, ErrUnavailable
	}
	r, ok := m.idem[key]
	return r, ok, nil
}

func (m *Memory) SaveIdempotent(_ context.Context, key string, rec IdemRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return ErrUnavailable
	}
	m.idem[key] = rec
	return nil
}

func (m *Memory) Ping(context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fail {
		return ErrUnavailable
	}
	return nil
}

func (m *Memory) Close() {}

// SortByUpdate orders employees by UpdatedAt, then id.
func SortByUpdate(es []model.Employee) {
	sort.Slice(es, func(i, j int) bool {
		if !es[i].UpdatedAt.Equal(es[j].UpdatedAt) {
			return es[i].UpdatedAt.Before(es[j].UpdatedAt)
		}
		return es[i].ID < es[j].ID
	})
}
