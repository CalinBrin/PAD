// Package service is the order service: it records orders and publishes the
// OrderCreated event. Every order is identified by the client's idempotency
// key; the order ID and the message ID are derived from it, so a repeated
// request (or a repeated publish) can never create a second order or a
// second event.
//
// Publishing has an explicit answer for every outcome:
//
//	published  the broker stored the event (or already had it: duplicate)
//	failed     the broker refused it for good (InvalidArgument, FailedPrecondition, ...)
//	pending    outcome unknown or broker unavailable: the order is kept and the
//	           outbox republishes it later with the same message ID (fallback)
package service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	brokerv1 "pad-lab2/gen/broker/v1"
	ordersv1 "pad-lab2/gen/orders/v1"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
)

const (
	StatusPending   = "pending"
	StatusPublished = "published"
	StatusFailed    = "failed"
)

type Order struct {
	OrderID         string `json:"order_id"`
	RequestID       string `json:"request_id"`
	CustomerID      string `json:"customer_id"`
	AmountCents     int64  `json:"amount_cents"`
	Currency        string `json:"currency"`
	MessageID       string `json:"message_id"`
	CorrelationID   string `json:"correlation_id"`
	Status          string `json:"status"`
	FailureReason   string `json:"failure_reason,omitempty"`
	RoutedQueues    int    `json:"routed_queues,omitempty"`
	PublishAttempts int    `json:"publish_attempts"`
	CreatedAt       string `json:"created_at"`
	PublishedAt     string `json:"published_at,omitempty"`
}

type Input struct {
	RequestID     string // Idempotency-Key of the client request
	CustomerID    string
	AmountCents   int64
	Currency      string
	CorrelationID string
}

var ErrKeyConflict = errors.New("idempotency key already used with a different request")

type Service struct {
	pub   *client.Publisher
	topic string
	log   *slog.Logger

	mu         sync.Mutex
	f          *os.File
	byKey      map[string]*Order
	byID       map[string]*Order
	publishing map[string]bool
	storeErr   error
}

// New opens (or creates) the order store in dataDir/orders.jsonl.
func New(dataDir string, pub *client.Publisher, log *slog.Logger) (*Service, error) {
	if log == nil {
		log = rpcx.Discard()
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Service{pub: pub, topic: envelope.TopicOrders, log: log, byKey: map[string]*Order{}, byID: map[string]*Order{}, publishing: map[string]bool{}}
	path := filepath.Join(dataDir, "orders.jsonl")
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var o Order
			if json.Unmarshal(sc.Bytes(), &o) == nil && o.OrderID != "" {
				cp := o
				s.byKey[o.RequestID], s.byID[o.OrderID] = &cp, &cp // last snapshot wins
			}
		}
		f.Close()
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	return s, nil
}

// saveLocked appends a snapshot of the order and fsyncs it (mu held).
func (s *Service) saveLocked(o *Order) error {
	b, _ := json.Marshal(o)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		s.storeErr = err
		return err
	}
	if err := s.f.Sync(); err != nil {
		s.storeErr = err
		return err
	}
	s.storeErr = nil
	return nil
}

// Create records the order (durably) and publishes its event. A repeated
// request with the same key returns the existing order (created = false).
func (s *Service) Create(ctx context.Context, in Input) (Order, bool, error) {
	if in.CorrelationID == "" {
		in.CorrelationID = rpcx.NewID()
	}
	if in.Currency == "" {
		in.Currency = envelope.DefaultCurrency
	}
	s.mu.Lock()
	if o, ok := s.byKey[in.RequestID]; ok {
		cp := *o
		s.mu.Unlock()
		if o.CustomerID != in.CustomerID || o.AmountCents != in.AmountCents || o.Currency != in.Currency {
			return Order{}, false, ErrKeyConflict
		}
		s.log.Info("repeated request, existing order returned", "event", "order_replayed", "order_id", cp.OrderID, "status", cp.Status, "correlation_id", in.CorrelationID)
		return cp, false, nil
	}
	o := &Order{
		OrderID:     "ord-" + envelope.DeriveID("order", in.RequestID)[:12],
		RequestID:   in.RequestID,
		CustomerID:  in.CustomerID,
		AmountCents: in.AmountCents,
		Currency:    in.Currency,
		// Same request => same message ID => the broker deduplicates retries.
		MessageID:     envelope.DeriveID("orders.v1.OrderCreated", in.RequestID),
		CorrelationID: in.CorrelationID,
		Status:        StatusPending,
		CreatedAt:     envelope.FormatTime(time.Now()),
	}
	if err := s.saveLocked(o); err != nil {
		s.mu.Unlock()
		s.log.Error("order not recorded, storage error", "event", "order_store_failed", "correlation_id", in.CorrelationID, "error", err)
		return Order{}, false, err
	}
	s.byKey[o.RequestID], s.byID[o.OrderID] = o, o
	s.mu.Unlock()
	s.log.Info("order recorded", "event", "order_recorded", "order_id", o.OrderID, "message_id", o.MessageID, "correlation_id", o.CorrelationID)

	s.publish(ctx, o.OrderID)
	cp, _ := s.Get(o.OrderID)
	return cp, true, nil
}

// publish tries to publish a pending order and records the outcome.
func (s *Service) publish(ctx context.Context, orderID string) {
	s.mu.Lock()
	o := s.byID[orderID]
	if o == nil || o.Status != StatusPending || s.publishing[orderID] {
		s.mu.Unlock()
		return
	}
	s.publishing[orderID] = true
	snap := *o
	s.mu.Unlock()

	created, _ := time.Parse(time.RFC3339Nano, snap.CreatedAt)
	env, err := envelope.NewOrderCreated(snap.MessageID, snap.CorrelationID, created, &ordersv1.OrderCreated{
		OrderId: snap.OrderID, CustomerId: snap.CustomerID, AmountCents: snap.AmountCents, Currency: snap.Currency})
	var res *brokerv1.PublishResponse
	if err == nil {
		res, err = s.pub.Publish(ctx, s.topic, env)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.publishing, orderID)
	o.PublishAttempts++
	log := s.log.With("order_id", o.OrderID, "message_id", o.MessageID, "correlation_id", o.CorrelationID)
	switch code := status.Code(err); {
	case err == nil:
		o.Status, o.RoutedQueues, o.PublishedAt = StatusPublished, int(res.RoutedQueues), envelope.FormatTime(time.Now())
		log.Info("order published", "event", "order_published", "routed_queues", o.RoutedQueues)
	case code == codes.InvalidArgument || code == codes.FailedPrecondition || code == codes.AlreadyExists:
		o.Status, o.FailureReason = StatusFailed, status.Convert(err).Message()
		log.Error("order event refused by the broker, not retried", "event", "order_publish_refused", "result", code.String(), "error", o.FailureReason)
	default:
		log.Warn("publish outcome unknown or broker unavailable, order kept in the outbox", "event", "order_publish_deferred", "result", code.String(), "error", err.Error())
	}
	if serr := s.saveLocked(o); serr != nil {
		log.Error("could not record publish outcome", "event", "order_store_failed", "error", serr)
	}
}

// RunOutbox republishes pending orders every interval until ctx is done.
func (s *Service) RunOutbox(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, id := range s.pendingIDs() {
			s.log.Info("outbox: republishing pending order", "event", "outbox_retry", "order_id", id)
			s.publish(ctx, id)
		}
	}
}

func (s *Service) pendingIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, o := range s.byID {
		if o.Status == StatusPending && !s.publishing[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (s *Service) Get(orderID string) (Order, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byID[orderID]
	if !ok {
		return Order{}, false
	}
	return *o, true
}

// Ready reports whether the service can record orders.
func (s *Service) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storeErr
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
