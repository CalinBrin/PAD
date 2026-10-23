package rpcx

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoseResponses is a fault injector for the critical scenario of the lab:
// the remote operation is executed, but its answer never reaches the caller.
// While Remaining > 0, each call to one of the methods runs the handler
// normally (the effect is committed), then the answer is dropped: the server
// holds the call until the client gives up (deadline or cancel) and returns
// UNAVAILABLE. The client cannot tell whether the operation happened.
type LoseResponses struct {
	Remaining atomic.Int64
	Methods   map[string]bool // full method names, e.g. "/payment.v1.Payment/Charge"
	Log       *slog.Logger
}

func (f *LoseResponses) Interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !f.Methods[info.FullMethod] {
			return handler(ctx, req)
		}
		resp, err := handler(ctx, req)
		if err != nil || f.Remaining.Load() <= 0 || f.Remaining.Add(-1) < 0 {
			return resp, err
		}
		if f.Log != nil {
			f.Log.Warn("FAULT INJECTED: operation committed, response dropped", "event", "fault_response_lost",
				"method", info.FullMethod, "correlation_id", CorrelationID(ctx))
		}
		select {
		case <-ctx.Done():
		case <-time.After(30 * time.Second):
		}
		return nil, status.Error(codes.Unavailable, "response lost (injected fault)")
	}
}
