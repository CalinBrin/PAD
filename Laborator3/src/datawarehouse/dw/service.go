// Package dw is the data warehouse node: the service (business rules,
// versions, idempotency) and the controller that maps HTTP onto it.
package dw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"pad-lab3/src/common/model"
	"pad-lab3/src/datawarehouse/store"
)

var (
	// ErrPrecondition: If-Match does not match the stored version (HTTP 412).
	ErrPrecondition = errors.New("precondition failed")
	// ErrIdempotencyMismatch: the Idempotency-Key was used for another request (HTTP 422).
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

// Config of the service.
type Config struct {
	Node        string
	FeedOverlap time.Duration // the change feed re-sends this window (late commits, clock skew)
	FeedMax     int
	CASRetries  int
}

// Service implements the operations of the warehouse on top of a Store.
// It is safe for concurrent use: the only shared state is the Store, and every
// write is a compare-and-set on the employee version.
type Service struct {
	store store.Store
	cfg   Config
	Now   func() time.Time
}

func NewService(s store.Store, cfg Config) *Service {
	if cfg.FeedOverlap == 0 {
		cfg.FeedOverlap = 5 * time.Second
	}
	if cfg.FeedMax == 0 {
		cfg.FeedMax = 1000
	}
	if cfg.CASRetries == 0 {
		cfg.CASRetries = 8
	}
	return &Service{store: s, cfg: cfg, Now: time.Now}
}

// WriteResult describes the outcome of a PUT or POST.
type WriteResult struct {
	Employee model.Employee
	Created  bool
	Changed  bool
	Replayed bool
	Status   int // HTTP status used for an idempotent replay
}

func (s *Service) now() time.Time { return s.Now().UTC().Truncate(time.Millisecond) }

func (s *Service) Get(ctx context.Context, id string) (model.Employee, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, offset, limit int) (model.EmployeeList, error) {
	items, total, err := s.store.List(ctx, offset, limit)
	if err != nil {
		return model.EmployeeList{}, err
	}
	return model.EmployeeList{Offset: offset, Limit: limit, Total: total, Items: items}, nil
}

// Put creates or replaces an employee (sent by a source node).
// PUT is idempotent: the same body sent again changes nothing (same version).
// ifMatch < 0 means no If-Match header.
func (s *Service) Put(ctx context.Context, in model.Employee, by string, ifMatch int64) (WriteResult, error) {
	if err := in.Validate(); err != nil {
		return WriteResult{}, err
	}
	return s.write(ctx, in.ID, by, ifMatch, func(cur model.Employee, exists bool) (model.Employee, error) {
		return in, nil
	})
}

// Patch changes some fields of an existing employee (sent by a client).
func (s *Service) Patch(ctx context.Context, id string, p model.EmployeePatch, by, idemKey string) (WriteResult, error) {
	if p.Empty() {
		return WriteResult{}, fmt.Errorf("%w: the patch changes no field", model.ErrInvalid)
	}
	var hash string
	if idemKey != "" {
		b, _ := json.Marshal(struct {
			ID    string
			Patch model.EmployeePatch
		}{id, p})
		sum := sha256.Sum256(b)
		hash = hex.EncodeToString(sum[:])
		rec, ok, err := s.store.GetIdempotent(ctx, idemKey)
		if err != nil {
			return WriteResult{}, err
		}
		if ok {
			if rec.RequestHash != hash {
				return WriteResult{}, ErrIdempotencyMismatch
			}
			return WriteResult{Employee: rec.Employee, Replayed: true, Status: rec.Status}, nil
		}
	}
	res, err := s.write(ctx, id, by, -1, func(cur model.Employee, exists bool) (model.Employee, error) {
		if !exists {
			return model.Employee{}, store.ErrNotFound
		}
		next := p.Apply(cur)
		return next, next.Validate()
	})
	if err != nil {
		return res, err
	}
	if idemKey != "" {
		// Best effort: if this fails, a retry applies the same absolute values
		// again, which is a no-op (same content, same version).
		_ = s.store.SaveIdempotent(context.WithoutCancel(ctx), idemKey,
			store.IdemRecord{RequestHash: hash, Status: 200, Employee: res.Employee})
	}
	return res, nil
}

// write is the read / compare-and-set loop shared by Put and Patch.
func (s *Service) write(ctx context.Context, id, by string, ifMatch int64,
	build func(cur model.Employee, exists bool) (model.Employee, error)) (WriteResult, error) {
	if by == "" {
		by = "anonymous"
	}
	for attempt := 0; attempt < s.cfg.CASRetries; attempt++ {
		cur, err := s.store.Get(ctx, id)
		exists := err == nil
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return WriteResult{}, err
		}
		if ifMatch >= 0 && ((exists && cur.Version != ifMatch) || (!exists && ifMatch != 0)) {
			return WriteResult{}, ErrPrecondition
		}
		next, err := build(cur, exists)
		if err != nil {
			return WriteResult{}, err
		}
		next.ID = id
		if exists && model.SameContent(cur, next) {
			return WriteResult{Employee: cur}, nil
		}
		next.Version = cur.Version + 1
		next.UpdatedAt = s.now()
		next.UpdatedBy = by
		// Cancellation: a caller that gave up before the commit gets no effect.
		if err := ctx.Err(); err != nil {
			return WriteResult{}, err
		}
		err = s.store.CompareAndSet(ctx, next, cur.Version)
		if errors.Is(err, store.ErrConflict) {
			continue // a concurrent writer won: read again and rebuild
		}
		if err != nil {
			return WriteResult{}, err
		}
		return WriteResult{Employee: next, Created: !exists, Changed: true}, nil
	}
	return WriteResult{}, store.ErrConflict
}

// Changes returns the employees changed after the cursor, re-sending the last
// FeedOverlap window so that a write committed late (or stamped by a replica
// whose clock is slightly behind) is not skipped. The reader drops what it
// already has by comparing versions. The answer is a pure read: repeating it
// has no effect, so it is safe to retry.
func (s *Service) Changes(ctx context.Context, cursor time.Time) ([]model.Employee, time.Time, error) {
	since := cursor
	if !cursor.IsZero() {
		since = cursor.Add(-s.cfg.FeedOverlap)
	}
	items, err := s.store.ChangedSince(ctx, since, s.cfg.FeedMax)
	if err != nil {
		return nil, cursor, err
	}
	next := cursor
	for _, e := range items {
		if e.UpdatedAt.After(next) {
			next = e.UpdatedAt
		}
	}
	return items, next, nil
}

func (s *Service) Ready(ctx context.Context) error { return s.store.Ping(ctx) }
