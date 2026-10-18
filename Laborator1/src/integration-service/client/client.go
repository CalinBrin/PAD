// Package client is the producer/consumer SDK for the broker.
package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"pad-lab1/src/integration-service/protocol"
)

// ErrCrash can be returned by a Handler to simulate a process crash: the
// subscriber drops the connection without sending ACK or NACK.
var ErrCrash = errors.New("simulated crash before ack")

// PermanentError marks a failure that retrying cannot fix (invalid payload,
// unsupported schema, business rule violation). The subscriber sends a NACK
// with permanent=true and the broker moves the message to the DLQ at once.
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

// IsPermanent reports whether err (or an error it wraps) is permanent.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// ---------------------------------------------------------------- Publisher

// Publisher publishes messages over one connection and waits for the broker
// confirmation (puback), which means "persisted by the broker", not "processed".
type Publisher struct {
	mu      sync.Mutex
	conn    net.Conn
	enc     *json.Encoder
	dec     *json.Decoder
	Timeout time.Duration
}

func DialPublisher(addr string) (*Publisher, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &Publisher{conn: conn, enc: json.NewEncoder(conn), dec: json.NewDecoder(bufio.NewReader(conn)), Timeout: 5 * time.Second}, nil
}

// Publish sends the message and blocks until the broker confirms it. It
// returns the number of queues the message was routed to.
func (p *Publisher) Publish(topic string, m protocol.Message) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.conn.SetDeadline(time.Now().Add(p.Timeout))
	if err := p.enc.Encode(protocol.Frame{Type: protocol.FramePublish, Topic: topic, Message: &m}); err != nil {
		return 0, err
	}
	for {
		var f protocol.Frame
		if err := p.dec.Decode(&f); err != nil {
			return 0, err
		}
		switch {
		case f.Type == protocol.FramePubAck && f.ID == m.ID:
			return f.Routed, nil
		case f.Type == protocol.FrameError:
			return 0, fmt.Errorf("broker error: %s", f.Reason)
		}
	}
}

func (p *Publisher) Close() error { return p.conn.Close() }

// --------------------------------------------------------------- Subscriber

// Delivery is one message handed to a Handler.
type Delivery struct {
	Message     protocol.Message
	Attempt     int
	Redelivered bool
}

// Handler returns nil to ACK, an error to NACK (retried by the broker), an
// error wrapped with Permanent to NACK without retry (straight to the DLQ),
// or ErrCrash to simulate a crash.
type Handler func(Delivery) error

type SubscribeOptions struct {
	Topic    string
	Group    string // consumers with the same group compete for messages
	Name     string // instance name, used in logs
	Prefetch int    // max unacknowledged messages, default 1
	// AckBeforeProcess switches to at-most-once: ACK is sent before the handler runs.
	AckBeforeProcess bool
}

// Subscriber is one consumer connection. Messages are handled sequentially;
// run several Subscribers (same group) for concurrent processing.
type Subscriber struct {
	opt     SubscribeOptions
	conn    net.Conn
	enc     *json.Encoder
	dec     *json.Decoder
	h       Handler
	closing atomic.Bool
	done    chan struct{}
	err     error
}

func Subscribe(addr string, opt SubscribeOptions, h Handler) (*Subscriber, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	s := &Subscriber{opt: opt, conn: conn, enc: json.NewEncoder(conn), dec: json.NewDecoder(bufio.NewReader(conn)), h: h, done: make(chan struct{})}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	err = s.enc.Encode(protocol.Frame{Type: protocol.FrameSubscribe, Topic: opt.Topic, Group: opt.Group, Name: opt.Name, Prefetch: opt.Prefetch})
	var f protocol.Frame
	if err == nil {
		err = s.dec.Decode(&f)
	}
	if err == nil && f.Type == protocol.FrameError {
		err = fmt.Errorf("broker error: %s", f.Reason)
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	go s.run()
	return s, nil
}

func (s *Subscriber) run() {
	defer close(s.done)
	for {
		var f protocol.Frame
		if err := s.dec.Decode(&f); err != nil {
			if !s.closing.Load() {
				s.err = err
			}
			return
		}
		if f.Type != protocol.FrameDeliver || f.Message == nil {
			continue
		}
		if !s.handle(f) {
			return
		}
	}
}

// handle returns false when the subscriber must stop.
func (s *Subscriber) handle(f protocol.Frame) bool {
	id, tag := f.Message.ID, f.Tag
	if s.opt.AckBeforeProcess {
		if s.enc.Encode(protocol.Frame{Type: protocol.FrameAck, ID: id, Tag: tag}) != nil {
			return false
		}
	}
	err := s.h(Delivery{Message: *f.Message, Attempt: f.Attempt, Redelivered: f.Redelivered})
	switch {
	case errors.Is(err, ErrCrash):
		s.err = ErrCrash
		s.closing.Store(true)
		s.conn.Close() // no ACK, no NACK: the broker only sees a dropped connection
		return false
	case s.opt.AckBeforeProcess:
		return true
	case err == nil:
		return s.enc.Encode(protocol.Frame{Type: protocol.FrameAck, ID: id, Tag: tag}) == nil
	default:
		return s.enc.Encode(protocol.Frame{Type: protocol.FrameNack, ID: id, Tag: tag, Reason: err.Error(), Permanent: IsPermanent(err)}) == nil
	}
}

// Close removes the subscriber (dynamic removal). In-flight messages are
// requeued by the broker.
func (s *Subscriber) Close() {
	s.closing.Store(true)
	s.conn.Close()
}

// Done is closed when the subscriber stopped.
func (s *Subscriber) Done() <-chan struct{} { return s.done }

// Err is valid after Done is closed: ErrCrash, a connection error, or nil after Close.
func (s *Subscriber) Err() error { <-s.done; return s.err }
