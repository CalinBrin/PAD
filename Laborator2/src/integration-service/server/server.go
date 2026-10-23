// Package server is the gRPC adapter of the broker: it maps the RPCs of
// contracts/proto/broker/v1 to the broker core and the core errors to the
// status codes listed in contracts/errors.md.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	brokerv1 "pad-lab2/gen/broker/v1"
	"pad-lab2/src/integration-service/broker"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
)

type Server struct {
	brokerv1.UnimplementedBrokerServer
	b   *broker.Broker
	log *slog.Logger
}

// New returns a gRPC server with the Broker service and the standard gRPC
// health service. extra interceptors (fault injection in tests and demos) run
// closest to the handler.
func New(b *broker.Broker, log *slog.Logger, extra ...grpc.UnaryServerInterceptor) (*grpc.Server, *health.Server) {
	if log == nil {
		log = rpcx.Discard()
	}
	gs := grpc.NewServer(rpcx.ServerOptions(log, extra...)...)
	brokerv1.RegisterBrokerServer(gs, &Server{b: b, log: log})
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus(brokerv1.Broker_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	return gs, hs
}

func (s *Server) Publish(ctx context.Context, req *brokerv1.PublishRequest) (*brokerv1.PublishResponse, error) {
	env := req.GetMessage()
	if err := envelope.Validate(env); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	m := broker.Message{ID: env.MessageId, CorrelationID: env.CorrelationId, Type: env.MessageType,
		SchemaVersion: env.SchemaVersion, OccurredAt: env.OccurredAt, Payload: env.Payload}
	res, err := s.b.Publish(ctx, req.Topic, m)
	if err != nil {
		return nil, toStatus(err)
	}
	return &brokerv1.PublishResponse{MessageId: m.ID, RoutedQueues: int32(res.Routed), Duplicate: res.Duplicate}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, broker.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, broker.ErrNoQueue):
		return status.Error(codes.FailedPrecondition, err.Error()+"; message not stored")
	case errors.Is(err, broker.ErrIDConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, broker.ErrStore):
		return status.Error(codes.Unavailable, err.Error()+"; message not stored, retry is safe")
	case errors.Is(err, broker.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded before commit; message not stored")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "cancelled before commit; message not stored")
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Server) Consume(stream brokerv1.Broker_ConsumeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	sub := first.GetSubscribe()
	if sub == nil {
		return status.Error(codes.InvalidArgument, "the first message of Consume must be subscribe")
	}
	cons, err := s.b.Subscribe(sub.Topic, sub.Group, sub.ConsumerName, int(sub.Prefetch))
	if err != nil {
		return toStatus(err)
	}
	reason := "consumer disconnected"
	defer func() { s.b.Unsubscribe(cons, reason) }()
	if err := stream.Send(&brokerv1.ConsumeResponse{Response: &brokerv1.ConsumeResponse_Subscribed{Subscribed: &brokerv1.Subscribed{Queue: cons.Queue}}}); err != nil {
		return err
	}

	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			switch r := req.Request.(type) {
			case *brokerv1.ConsumeRequest_Ack:
				s.b.Ack(cons, r.Ack.DeliveryTag)
			case *brokerv1.ConsumeRequest_Nack:
				s.b.Nack(cons, r.Nack.DeliveryTag, r.Nack.Reason, r.Nack.Permanent)
			default:
				s.log.Warn("ignoring unexpected consume request", "event", "consume_request_ignored", "consumer", cons.Name)
			}
		}
	}()

	for {
		select {
		case d := <-cons.Deliveries():
			err := stream.Send(&brokerv1.ConsumeResponse{Response: &brokerv1.ConsumeResponse_Delivery{Delivery: &brokerv1.Delivery{
				DeliveryTag: d.Tag, Attempt: int32(d.Attempt), Redelivered: d.Redelivered,
				Message: &brokerv1.Envelope{MessageId: d.Msg.ID, CorrelationId: d.Msg.CorrelationID, MessageType: d.Msg.Type,
					SchemaVersion: d.Msg.SchemaVersion, OccurredAt: d.Msg.OccurredAt, Payload: d.Msg.Payload},
			}}})
			if err != nil {
				reason = "send to consumer failed"
				return err
			}
		case err := <-recvErr:
			if err == io.EOF {
				reason = "consumer closed the stream"
				return nil
			}
			reason = "consumer connection lost (" + status.Code(err).String() + ")"
			return err
		case <-cons.Done():
			reason = "session ended by broker"
			return status.Error(codes.Unavailable, "broker is shutting down")
		case <-stream.Context().Done():
			reason = "consumer connection lost (" + status.Code(stream.Context().Err()).String() + ")"
			return stream.Context().Err()
		}
	}
}
