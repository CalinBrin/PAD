// Package payment is the external service called by the billing consumer. It
// is deliberately simple but has the properties a real payment provider has:
// an idempotency key, a status lookup, authentication, latency and failures.
package payment

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	paymentv1 "pad-lab2/gen/payment/v1"
	"pad-lab2/src/integration-service/rpcx"
)

type Config struct {
	DataDir    string
	Token      string        // required bearer token (secret, from the environment)
	LimitCents int64         // charges above the limit are declined (business outcome)
	Latency    time.Duration // simulated processing time before the commit
	Logger     *slog.Logger
}

// Charge is one executed charge (the remote effect).
type Charge struct {
	Key           string    `json:"idempotency_key"`
	ChargeID      string    `json:"charge_id"`
	OrderID       string    `json:"order_id"`
	AmountCents   int64     `json:"amount_cents"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	DeclineReason string    `json:"decline_reason,omitempty"`
	CorrelationID string    `json:"correlation_id"`
	At            time.Time `json:"at"`
}

type Service struct {
	paymentv1.UnimplementedPaymentServer
	cfg Config
	log *slog.Logger

	// Fault injector: the next N Charge answers are lost after the commit.
	Faults *rpcx.LoseResponses

	mu      sync.Mutex
	f       *os.File
	charges map[string]Charge
	order   []string
}

func New(cfg Config) (*Service, error) {
	if cfg.Logger == nil {
		cfg.Logger = rpcx.Discard()
	}
	if cfg.Token == "" {
		return nil, errors.New("payment: token is required")
	}
	if cfg.LimitCents <= 0 {
		cfg.LimitCents = 1_000_000
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Service{cfg: cfg, log: cfg.Logger, charges: map[string]Charge{},
		Faults: &rpcx.LoseResponses{Methods: map[string]bool{paymentv1.Payment_Charge_FullMethodName: true}, Log: cfg.Logger}}
	path := filepath.Join(cfg.DataDir, "charges.jsonl")
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var c Charge
			if json.Unmarshal(sc.Bytes(), &c) == nil && c.Key != "" {
				if _, ok := s.charges[c.Key]; !ok {
					s.order = append(s.order, c.Key)
				}
				s.charges[c.Key] = c
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

// GRPCServer returns the gRPC server: authentication, call logs, the fault
// injector and the gRPC health service.
func (s *Service) GRPCServer() *grpc.Server {
	gs := grpc.NewServer(rpcx.ServerOptions(s.log, s.authInterceptor, s.Faults.Interceptor())...)
	paymentv1.RegisterPaymentServer(gs, s)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	return gs
}

func (s *Service) authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == healthpb.Health_Check_FullMethodName {
		return handler(ctx, req)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	got := ""
	if v := md.Get("authorization"); len(v) > 0 {
		got = v[0]
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+s.cfg.Token)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "missing or invalid token")
	}
	return handler(ctx, req)
}

func (s *Service) Charge(ctx context.Context, req *paymentv1.ChargeRequest) (*paymentv1.ChargeResponse, error) {
	switch {
	case req.IdempotencyKey == "":
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	case req.OrderId == "":
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	case req.AmountCents <= 0:
		return nil, status.Error(codes.InvalidArgument, "amount_cents must be > 0")
	case len(req.Currency) != 3:
		return nil, status.Error(codes.InvalidArgument, "currency must be an ISO 4217 code")
	}
	log := s.log.With("correlation_id", rpcx.CorrelationID(ctx), "idempotency_key", req.IdempotencyKey, "order_id", req.OrderId)

	s.mu.Lock()
	if c, ok := s.charges[req.IdempotencyKey]; ok {
		s.mu.Unlock()
		if c.OrderID != req.OrderId || c.AmountCents != req.AmountCents || c.Currency != req.Currency {
			return nil, status.Error(codes.FailedPrecondition, "idempotency_key already used with different parameters")
		}
		log.Info("charge replayed, not executed again", "event", "charge_replayed", "charge_id", c.ChargeID, "status", c.Status)
		return toProto(c, true), nil
	}
	s.mu.Unlock()

	// Simulated processing time. The caller's deadline is propagated by gRPC:
	// if it expires (or the caller cancels) now, nothing is executed.
	if s.cfg.Latency > 0 {
		select {
		case <-time.After(s.cfg.Latency):
		case <-ctx.Done():
			log.Warn("charge abandoned before execution, caller deadline or cancel", "event", "charge_cancelled", "result", status.Code(ctx.Err()).String())
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.charges[req.IdempotencyKey]; ok { // a concurrent call with the same key won
		return toProto(c, true), nil
	}
	c := Charge{Key: req.IdempotencyKey, ChargeID: "ch_" + rpcx.NewID()[:12], OrderID: req.OrderId, AmountCents: req.AmountCents,
		Currency: req.Currency, Status: "approved", CorrelationID: rpcx.CorrelationID(ctx), At: time.Now().UTC()}
	if req.AmountCents > s.cfg.LimitCents {
		c.Status, c.DeclineReason = "declined", fmt.Sprintf("amount over the limit of %d cents", s.cfg.LimitCents)
	}
	b, _ := json.Marshal(c)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return nil, status.Error(codes.Unavailable, "charge not executed: storage error")
	}
	if err := s.f.Sync(); err != nil {
		return nil, status.Error(codes.Unavailable, "charge not executed: storage error")
	}
	s.charges[c.Key] = c
	s.order = append(s.order, c.Key)
	log.Info("charge executed", "event", "charge_executed", "charge_id", c.ChargeID, "status", c.Status, "amount_cents", c.AmountCents)
	return toProto(c, false), nil
}

func (s *Service) GetCharge(ctx context.Context, req *paymentv1.GetChargeRequest) (*paymentv1.ChargeResponse, error) {
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	s.mu.Lock()
	c, ok := s.charges[req.IdempotencyKey]
	s.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "no charge executed for this idempotency_key")
	}
	return toProto(c, true), nil
}

func toProto(c Charge, replayed bool) *paymentv1.ChargeResponse {
	st := paymentv1.ChargeStatus_CHARGE_STATUS_APPROVED
	if c.Status == "declined" {
		st = paymentv1.ChargeStatus_CHARGE_STATUS_DECLINED
	}
	return &paymentv1.ChargeResponse{ChargeId: c.ChargeID, Status: st, Replayed: replayed, DeclineReason: c.DeclineReason}
}

// Charges returns every executed charge (evidence for tests and the demo).
func (s *Service) Charges() []Charge {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Charge, 0, len(s.order))
	for _, k := range s.order {
		out = append(out, s.charges[k])
	}
	return out
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

// AdminHandler exposes:
//
//	GET  /healthz                    liveness
//	GET  /readyz                     readiness
//	GET  /charges                    executed charges
//	POST /faults?lose_responses=N    lose the answer of the next N charges
func (s *Service) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("GET /charges", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Charges()) })
	mux.HandleFunc("POST /faults", func(w http.ResponseWriter, r *http.Request) {
		n, err := strconv.Atoi(r.URL.Query().Get("lose_responses"))
		if err != nil || n < 0 {
			writeJSON(w, 400, map[string]string{"error": "lose_responses must be an integer >= 0"})
			return
		}
		s.Faults.Remaining.Store(int64(n))
		s.log.Warn("fault injection configured", "event", "fault_configured", "lose_responses", n)
		writeJSON(w, 200, map[string]int{"lose_responses": n})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// TokenCredentials adds the bearer token to every call (client side).
type TokenCredentials string

func (t TokenCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}
func (t TokenCredentials) RequireTransportSecurity() bool { return false }
