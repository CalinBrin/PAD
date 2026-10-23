// Package api is the HTTP entry point (contract: contracts/http-api.md).
//
//	POST /orders        create an order (Idempotency-Key header required)
//	GET  /orders/{id}   state of an order
//	GET  /healthz       liveness
//	GET  /readyz        readiness + state of the broker dependency
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/order-service/service"
)

const (
	CorrelationHeader = "X-Correlation-ID"
	IdempotencyHeader = "Idempotency-Key"
	ReplayedHeader    = "Idempotent-Replayed"
)

type CreateOrderRequest struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency,omitempty"`
}

type Orders interface {
	Create(ctx context.Context, in service.Input) (service.Order, bool, error)
	Get(orderID string) (service.Order, bool)
	Ready() error
}

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// NewHandler returns the HTTP API. brokerState reports the state of the gRPC
// connection to the broker (for /readyz, no network call is made).
func NewHandler(svc Orders, brokerState func() string, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Ready(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "reason": "order store unavailable"})
			return
		}
		state := brokerState()
		st := "ready"
		if state != "ready" && state != "idle" {
			// Orders are still accepted (outbox), the broker is temporarily unavailable.
			st = "degraded"
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": st, "dependencies": map[string]string{"broker": state}})
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		cid := r.Header.Get(CorrelationHeader)
		key := r.Header.Get(IdempotencyHeader)
		if key == "" || len(key) > 128 {
			writeError(w, http.StatusBadRequest, cid, "Idempotency-Key header is required (1..128 characters)")
			return
		}
		var req CreateOrderRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, cid, "invalid JSON body")
			return
		}
		switch {
		case req.CustomerID == "":
			writeError(w, http.StatusBadRequest, cid, "customer_id is required")
			return
		case req.AmountCents <= 0:
			writeError(w, http.StatusBadRequest, cid, "amount_cents must be an integer > 0")
			return
		case req.Currency != "" && !currencyRe.MatchString(req.Currency):
			writeError(w, http.StatusBadRequest, cid, "currency must be a 3-letter ISO 4217 code")
			return
		}
		o, created, err := svc.Create(r.Context(), service.Input{RequestID: key, CustomerID: req.CustomerID, AmountCents: req.AmountCents,
			Currency: req.Currency, CorrelationID: cid})
		switch {
		case errors.Is(err, service.ErrKeyConflict):
			writeError(w, http.StatusConflict, cid, err.Error())
			return
		case err != nil:
			writeError(w, http.StatusServiceUnavailable, cid, "order could not be recorded")
			return
		}
		w.Header().Set(CorrelationHeader, o.CorrelationID)
		code := http.StatusAccepted
		switch {
		case !created:
			w.Header().Set(ReplayedHeader, "true")
			code = http.StatusOK
		case o.Status == service.StatusFailed:
			code = http.StatusBadGateway
		}
		writeJSON(w, code, o)
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		o, ok := svc.Get(r.PathValue("id"))
		if !ok {
			writeError(w, http.StatusNotFound, r.Header.Get(CorrelationHeader), "order not found")
			return
		}
		w.Header().Set(CorrelationHeader, o.CorrelationID)
		writeJSON(w, http.StatusOK, o)
	})
	return logRequests(mux, log)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, cid, msg string) {
	if cid != "" {
		w.Header().Set(CorrelationHeader, cid)
	}
	writeJSON(w, code, map[string]string{"error": msg})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }

func logRequests(h http.Handler, log *slog.Logger) http.Handler {
	if log == nil {
		log = rpcx.Discard()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		h.ServeHTTP(sw, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		log.Info("http request", "event", "http_request", "method", r.Method, "path", r.URL.Path, "result", sw.code,
			"duration_ms", time.Since(start).Milliseconds(), "correlation_id", w.Header().Get(CorrelationHeader))
	})
}
