// Package service contains the producer logic of the order service.
package service

import (
	"io"
	"log/slog"
	"sync"

	"pad-lab1/src/api"
	"pad-lab1/src/integration-service/client"
	"pad-lab1/src/integration-service/protocol"
)

// Service creates orders and publishes OrderCreated events. It keeps one
// broker connection and reconnects once if a publish fails (the retry reuses
// the same message ID, so a duplicate can only be a duplicate of the same ID).
type Service struct {
	addr string
	log  *slog.Logger

	mu  sync.Mutex
	pub *client.Publisher
}

func New(brokerAddr string, log *slog.Logger) *Service {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{addr: brokerAddr, log: log}
}

func (s *Service) CreateOrder(customerID string, amount float64, correlationID string) (api.OrderResult, error) {
	orderID := "ord-" + protocol.NewID()[:8]
	msg, err := protocol.NewMessage(protocol.TypeOrderCreated, correlationID, protocol.OrderCreated{OrderID: orderID, CustomerID: customerID, Amount: amount})
	if err != nil {
		return api.OrderResult{}, err
	}
	routed, err := s.publish(msg)
	if err != nil {
		s.log.Error("publish failed", "correlation_id", msg.CorrelationID, "order_id", orderID, "error", err)
		return api.OrderResult{}, err
	}
	s.log.Info("order published", "correlation_id", msg.CorrelationID, "order_id", orderID, "message_id", msg.ID, "routed_queues", routed)
	return api.OrderResult{OrderID: orderID, MessageID: msg.ID, CorrelationID: msg.CorrelationID, Routed: routed}, nil
}

func (s *Service) publish(m protocol.Message) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if s.pub == nil {
			p, err := client.DialPublisher(s.addr)
			if err != nil {
				lastErr = err
				continue
			}
			s.pub = p
		}
		routed, err := s.pub.Publish(protocol.TopicOrders, m)
		if err == nil {
			return routed, nil
		}
		lastErr = err
		s.pub.Close()
		s.pub = nil
	}
	return 0, lastErr
}

func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pub != nil {
		s.pub.Close()
		s.pub = nil
	}
}
