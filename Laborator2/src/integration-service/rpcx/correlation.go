// Package rpcx holds what every service shares on top of gRPC: correlation ID
// propagation, structured call logs, the retry policy, configuration from the
// environment and fault injection.
package rpcx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// CorrelationKey is the gRPC metadata key (and, as X-Correlation-ID, the HTTP
// header) that carries the correlation ID.
const CorrelationKey = "x-correlation-id"

type ctxKey struct{}

// WithCorrelationID stores the correlation ID in the context. Outgoing gRPC
// calls made with this context carry it in their metadata.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the correlation ID of the context ("" if none).
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// NewID returns a random 128-bit hex identifier.
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func outgoing(ctx context.Context) context.Context {
	if id := CorrelationID(ctx); id != "" {
		return metadata.AppendToOutgoingContext(ctx, CorrelationKey, id)
	}
	return ctx
}

func incoming(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(CorrelationKey); len(v) > 0 && v[0] != "" {
			return WithCorrelationID(ctx, v[0])
		}
	}
	return ctx
}

// ClientOptions returns the dial options that propagate the correlation ID.
func ClientOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(outgoing(ctx), method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(outgoing(ctx), desc, cc, method, opts...)
		}),
	}
}

// ServerOptions returns the server interceptors that read the correlation ID
// and write one structured log line per call (method, result code, duration).
// extra unary interceptors run after them, closest to the handler.
func ServerOptions(log *slog.Logger, extra ...grpc.UnaryServerInterceptor) []grpc.ServerOption {
	if log == nil {
		log = Discard()
	}
	unary := append([]grpc.UnaryServerInterceptor{func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = incoming(ctx)
		start := time.Now()
		resp, err := handler(ctx, req)
		logCall(log, ctx, info.FullMethod, start, err)
		return resp, err
	}}, extra...)
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := incoming(ss.Context())
			start := time.Now()
			err := handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
			logCall(log, ctx, info.FullMethod, start, err)
			return err
		}),
	}
}

func logCall(log *slog.Logger, ctx context.Context, method string, start time.Time, err error) {
	code := status.Code(err)
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelWarn
	}
	attrs := []any{"event", "rpc_served", "method", method, "result", code.String(), "duration_ms", time.Since(start).Milliseconds()}
	if id := CorrelationID(ctx); id != "" {
		attrs = append(attrs, "correlation_id", id)
	}
	if err != nil {
		attrs = append(attrs, "error", status.Convert(err).Message())
	}
	log.Log(ctx, level, "rpc served", attrs...)
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
