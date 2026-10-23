// Package client is the producer/consumer SDK of the broker (gRPC).
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	brokerv1 "pad-lab2/gen/broker/v1"
	"pad-lab2/src/integration-service/rpcx"
)

// Dial opens a client connection with correlation ID propagation. The
// connection is lazy: it does not fail when the server is down, calls do.
func Dial(addr string, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, rpcx.ClientOptions()...)
	return grpc.NewClient(addr, append(opts, extra...)...)
}

// DefaultPublishPolicy: 4 attempts, 1s deadline each, jittered backoff.
var DefaultPublishPolicy = rpcx.Policy{MaxAttempts: 4, BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second, PerAttemptTimeout: time.Second}

// ---------------------------------------------------------------- Publisher

// Publisher publishes envelopes. Publish is idempotent at the broker (key:
// message_id), so a retry after an unknown outcome (deadline exceeded, lost
// connection) is safe: if the first attempt was stored, the broker answers
// duplicate=true and stores nothing new.
type Publisher struct {
	c      brokerv1.BrokerClient
	Policy rpcx.Policy
	Log    *slog.Logger
}

func NewPublisher(conn grpc.ClientConnInterface, policy rpcx.Policy, log *slog.Logger) *Publisher {
	if log == nil {
		log = rpcx.Discard()
	}
	return &Publisher{c: brokerv1.NewBrokerClient(conn), Policy: policy, Log: log}
}

func (p *Publisher) Publish(ctx context.Context, topic string, env *brokerv1.Envelope) (*brokerv1.PublishResponse, error) {
	ctx = rpcx.WithCorrelationID(ctx, env.CorrelationId)
	var res *brokerv1.PublishResponse
	err := rpcx.Do(ctx, p.Policy, p.Log, "broker.Publish", rpcx.Retryable, func(actx context.Context, attempt int) error {
		r, err := p.c.Publish(actx, &brokerv1.PublishRequest{Topic: topic, Message: env})
		if err != nil {
			if rpcx.Ambiguous(err) {
				p.Log.Warn("publish outcome unknown, the message may be stored; retrying with the same message_id",
					"event", "publish_outcome_unknown", "message_id", env.MessageId, "correlation_id", env.CorrelationId,
					"attempt", attempt, "result", status.Code(err).String())
			}
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	p.Log.Info("message published", "event", "message_published", "message_id", env.MessageId, "correlation_id", env.CorrelationId,
		"routed_queues", res.RoutedQueues, "duplicate", res.Duplicate)
	return res, nil
}

// --------------------------------------------------------------- Subscriber

// ErrCrash can be returned by a Handler to simulate a process crash: the
// session is aborted without ACK or NACK.
var ErrCrash = errors.New("simulated crash before ack")

// PermanentError marks a failure that retrying cannot fix. The subscriber
// sends a NACK with permanent=true and the broker dead-letters the message.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a non-retryable failure (nil stays nil).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Delivery is one message handed to a Handler.
type Delivery struct {
	Tag         uint64
	Message     *brokerv1.Envelope
	Attempt     int
	Redelivered bool
}

// Handler returns nil to ACK, an error to NACK (retried by the broker),
// Permanent(err) to dead-letter, or ErrCrash to simulate a crash. ctx is
// cancelled when the subscriber stops: a handler that returns because of it
// is neither acked nor nacked, and the broker redelivers the message.
type Handler func(ctx context.Context, d Delivery) error

type Options struct {
	Topic    string
	Group    string // durable identity: consumers of the same group share the queue
	Name     string // instance name, used in logs
	Prefetch int    // max unacknowledged deliveries, default 1
	// AckBeforeProcess switches to at-most-once: ACK before the handler runs.
	AckBeforeProcess bool
	// Reconnect keeps the subscriber running across broker restarts.
	Reconnect bool
	Log       *slog.Logger
	// OnSubscribed is called each time a session is established (tests, health).
	OnSubscribed func(connected bool)
}

// Subscription is a running subscriber.
type Subscription struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// Start runs a subscriber in the background until Close, a crash, or (without
// Reconnect) the end of the session.
func Start(ctx context.Context, conn grpc.ClientConnInterface, opt Options, h Handler) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	s := &Subscription{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		s.err = Run(ctx, conn, opt, h)
	}()
	return s
}

// Close stops the subscriber (dynamic removal). Unacked deliveries are requeued by the broker.
func (s *Subscription) Close()                { s.cancel(); <-s.done }
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Err is valid after Done: ErrCrash, a session error, or nil after Close.
func (s *Subscription) Err() error { <-s.done; return s.err }

// Run subscribes and processes deliveries sequentially until ctx is done. With
// Reconnect, a lost session is reopened with jittered backoff; the group name,
// not the process, owns the deliveries, so nothing is lost meanwhile.
func Run(ctx context.Context, conn grpc.ClientConnInterface, opt Options, h Handler) error {
	if opt.Log == nil {
		opt.Log = rpcx.Discard()
	}
	c := brokerv1.NewBrokerClient(conn)
	backoff := rpcx.Policy{BaseDelay: 200 * time.Millisecond, MaxDelay: 5 * time.Second}
	for failures := 1; ; failures++ {
		established, err := session(ctx, c, opt, h)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrCrash) || !opt.Reconnect {
			return err
		}
		if established {
			failures = 1
		}
		delay := backoff.Backoff(failures)
		opt.Log.Warn("connection to broker lost, reconnecting", "event", "broker_reconnect", "consumer", opt.Name,
			"result", status.Code(err).String(), "error", status.Convert(err).Message(), "retry_in_ms", delay.Milliseconds())
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil
		}
	}
}

func session(ctx context.Context, c brokerv1.BrokerClient, opt Options, h Handler) (established bool, err error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel() // ends the stream: the broker requeues what was not acked
	stream, err := c.Consume(sctx)
	if err != nil {
		return false, err
	}
	if err := stream.Send(&brokerv1.ConsumeRequest{Request: &brokerv1.ConsumeRequest_Subscribe{Subscribe: &brokerv1.Subscribe{
		Topic: opt.Topic, Group: opt.Group, ConsumerName: opt.Name, Prefetch: int32(opt.Prefetch)}}}); err != nil {
		return false, err
	}
	first, err := stream.Recv()
	if err != nil {
		return false, err
	}
	if first.GetSubscribed() == nil {
		return false, fmt.Errorf("unexpected first response %T", first.Response)
	}
	opt.Log.Info("subscribed", "event", "consumer_subscribed", "queue", first.GetSubscribed().Queue, "consumer", opt.Name)
	if opt.OnSubscribed != nil {
		opt.OnSubscribed(true)
		defer opt.OnSubscribed(false)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return true, err
		}
		d := resp.GetDelivery()
		if d == nil || d.Message == nil {
			continue
		}
		if err := handle(sctx, stream, opt, h, d); err != nil {
			if errors.Is(err, ErrCrash) {
				flushAndAbandon(stream)
			}
			return true, err
		}
	}
}

// flushAndAbandon models a crash after the bytes left the process: frames
// already sent (an at-most-once ack) still reach the broker, nothing more is
// sent. Cancelling the stream at once could drop them from the client buffer.
func flushAndAbandon(stream brokerv1.Broker_ConsumeClient) {
	_ = stream.CloseSend()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

func handle(ctx context.Context, stream brokerv1.Broker_ConsumeClient, opt Options, h Handler, d *brokerv1.Delivery) error {
	ack := &brokerv1.ConsumeRequest{Request: &brokerv1.ConsumeRequest_Ack{Ack: &brokerv1.Ack{DeliveryTag: d.DeliveryTag}}}
	if opt.AckBeforeProcess {
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
	hctx := rpcx.WithCorrelationID(ctx, d.Message.CorrelationId)
	err := h(hctx, Delivery{Tag: d.DeliveryTag, Message: d.Message, Attempt: int(d.Attempt), Redelivered: d.Redelivered})
	switch {
	case errors.Is(err, ErrCrash):
		return ErrCrash // no ACK, no NACK: the broker only sees the stream end
	case ctx.Err() != nil:
		return ctx.Err() // stopping: leave the message to the broker
	case opt.AckBeforeProcess:
		return nil
	case err == nil:
		return stream.Send(ack)
	default:
		return stream.Send(&brokerv1.ConsumeRequest{Request: &brokerv1.ConsumeRequest_Nack{Nack: &brokerv1.Nack{
			DeliveryTag: d.DeliveryTag, Reason: err.Error(), Permanent: IsPermanent(err)}}})
	}
}
