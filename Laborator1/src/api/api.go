// Package api is the HTTP entry point of the producer side.
//
//	POST /orders   {"customer_id": "...", "amount": 12.5}   -> 202 Accepted
//	GET  /healthz
//
// 202 means "the broker persisted the OrderCreated event", not "the order was
// processed by the consumers".
package api

import (
	"encoding/json"
	"net/http"
)

const CorrelationHeader = "X-Correlation-ID"

type CreateOrderRequest struct {
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

type OrderResult struct {
	OrderID       string `json:"order_id"`
	MessageID     string `json:"message_id"`
	CorrelationID string `json:"correlation_id"`
	Routed        int    `json:"routed_queues"`
}

// OrderCreator is implemented by the order service.
type OrderCreator interface {
	CreateOrder(customerID string, amount float64, correlationID string) (OrderResult, error)
}

func NewHandler(svc OrderCreator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var req CreateOrderRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if req.CustomerID == "" {
			http.Error(w, "customer_id is required", http.StatusBadRequest)
			return
		}
		res, err := svc.CreateOrder(req.CustomerID, req.Amount, r.Header.Get(CorrelationHeader))
		if err != nil {
			http.Error(w, "broker unavailable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(CorrelationHeader, res.CorrelationID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(res)
	})
	return mux
}
