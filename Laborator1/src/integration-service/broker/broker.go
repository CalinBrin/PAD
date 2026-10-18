// Package broker is a small durable message broker.
//
// Model: a producer publishes to a topic. For every consumer group (queue)
// bound to the topic the broker keeps one copy of the message. Consumers of
// the same group compete for messages; different groups each get all messages.
//
// Guarantees (see docs/delivery-guarantees.md):
//   - a publish is confirmed (puback) only after it is fsynced to the log
//   - delivery is at-least-once: a message stays in-flight until ACK
//   - NACK, ack timeout and consumer disconnect cause redelivery
//   - after MaxAttempts deliveries the message goes to the dead-letter queue
//   - a NACK marked permanent goes to the DLQ at once, without retries
//   - a retried message goes to a consumer that has not failed it yet, when
//     the group has one
//   - ordering: FIFO per queue for first deliveries; redelivered messages are
//     put back at their original position, but there is no ordering between
//     competing consumers of the same group
//
// In-flight deliveries are keyed by a delivery tag (unique per delivery), not
// by message ID, so two messages published with the same ID are tracked and
// acknowledged independently.
package broker

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"pad-lab1/src/integration-service/protocol"
)

type Config struct {
	DataDir     string
	MaxAttempts int           // deliveries before DLQ (default 3)
	AckTimeout  time.Duration // in-flight message without ACK is redelivered (default 10s)
	RetryBase   time.Duration // base of the exponential retry backoff (default 500ms)
	Logger      *slog.Logger
}

// QueueName is the name of the queue of a (topic, group) pair.
func QueueName(topic, group string) string { return topic + "." + group }

// Backoff returns base * 2^(attempt-1), capped at 30s.
func Backoff(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 10 {
		attempt = 10
	}
	d := base << (attempt - 1)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

type entry struct {
	msg      protocol.Message
	attempts int
	seq      uint64
	failedBy map[*consumer]bool // consumers that NACKed or timed out on this entry
}

type consumer struct {
	name     string
	c        *conn
	q        *queue
	prefetch int
	inflight int
}

type inflightMsg struct {
	tag      uint64
	e        *entry
	cons     *consumer
	deadline time.Time
}

type queue struct {
	name, topic, group string
	pending            []*entry
	inflight           map[uint64]*inflightMsg // key: delivery tag
	consumers          []*consumer
	rr                 int
	retrying           int
	dlq                []DLQEntry
}

type QueueStats struct {
	Pending   int `json:"pending"`
	Inflight  int `json:"inflight"`
	Retrying  int `json:"retrying"`
	Consumers int `json:"consumers"`
	DLQ       int `json:"dlq"`
}

type Broker struct {
	cfg Config
	log *slog.Logger
	st  *store

	mu     sync.Mutex // guards everything below
	queues map[string]*queue
	topics map[string][]*queue
	conns  map[*conn]struct{}
	seq    uint64
	tag    uint64 // last delivery tag
	closed bool

	ln   net.Listener
	done chan struct{}
	wg   sync.WaitGroup
}

func New(cfg Config) (*Broker, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 10 * time.Second
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 500 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.DataDir == "" {
		return nil, errors.New("broker: DataDir is required")
	}
	st, replayed, err := openStore(cfg.DataDir, cfg.Logger)
	if err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, log: cfg.Logger, st: st, queues: map[string]*queue{}, topics: map[string][]*queue{}, conns: map[*conn]struct{}{}, done: make(chan struct{})}
	for _, d := range replayed.decls {
		q := b.newQueue(d.Name, d.Topic, d.Group)
		for _, m := range replayed.pending[d.Name] {
			b.seq++
			q.pending = append(q.pending, &entry{msg: m, seq: b.seq})
		}
		q.dlq = replayed.dlq[d.Name]
		if len(q.pending) > 0 || len(q.dlq) > 0 {
			b.log.Info("recovered queue from log", "queue", q.name, "pending", len(q.pending), "dlq", len(q.dlq))
		}
	}
	b.wg.Add(1)
	go b.sweeper()
	return b, nil
}

func (b *Broker) newQueue(name, topic, group string) *queue {
	q := &queue{name: name, topic: topic, group: group, inflight: map[uint64]*inflightMsg{}}
	b.queues[name] = q
	b.topics[topic] = append(b.topics[topic], q)
	return q
}

// Declare creates the queue of (topic, group) if it does not exist yet.
func (b *Broker) Declare(topic, group string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.declareLocked(topic, group)
	return err
}

func (b *Broker) declareLocked(topic, group string) (*queue, error) {
	name := QueueName(topic, group)
	if q, ok := b.queues[name]; ok {
		return q, nil
	}
	if err := b.st.append(record{Op: opDeclare, Queue: name, Topic: topic, Group: group}); err != nil {
		return nil, err
	}
	b.log.Info("queue declared", "queue", name)
	return b.newQueue(name, topic, group), nil
}

// Listen starts accepting connections in the background.
func (b *Broker) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.ln = ln
	b.wg.Add(1)
	go b.acceptLoop()
	return nil
}

func (b *Broker) Addr() string { return b.ln.Addr().String() }

func (b *Broker) acceptLoop() {
	defer b.wg.Done()
	for {
		nc, err := b.ln.Accept()
		if err != nil {
			return // listener closed
		}
		c := newConn(nc)
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			c.close()
			return
		}
		b.conns[c] = struct{}{}
		b.wg.Add(1)
		b.mu.Unlock()
		go b.serveConn(c)
	}
}

func (b *Broker) serveConn(c *conn) {
	defer b.wg.Done()
	go c.writeLoop()
	dec := json.NewDecoder(bufio.NewReader(c.nc))
	for {
		var f protocol.Frame
		err := dec.Decode(&f)
		if err == nil {
			b.handle(c, f)
			continue
		}
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			// Valid JSON with a wrong field type: the stream is still in sync,
			// so report it and keep the session.
			c.send(protocol.Frame{Type: protocol.FrameError, Reason: "invalid frame: " + err.Error()})
			continue
		}
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			// Invalid JSON: the stream cannot be resynchronised. Tell the
			// client why, then close after the error frame is written.
			b.log.Warn("invalid JSON frame, closing connection", "remote", c.nc.RemoteAddr().String(), "error", err)
			c.sendFinal(protocol.Frame{Type: protocol.FrameError, Reason: "invalid JSON: " + err.Error()})
			select {
			case <-c.done:
			case <-time.After(time.Second):
			}
		}
		break // EOF, reset, torn frame or invalid JSON: the session ends
	}
	c.close()
	b.dropConn(c)
}

func (b *Broker) handle(c *conn, f protocol.Frame) {
	switch f.Type {
	case protocol.FramePublish:
		b.onPublish(c, f)
	case protocol.FrameSubscribe:
		b.onSubscribe(c, f)
	case protocol.FrameAck:
		b.onAck(c, f)
	case protocol.FrameNack:
		b.onNack(c, f)
	default:
		c.send(protocol.Frame{Type: protocol.FrameError, Reason: "unknown frame type " + f.Type})
	}
}

func (b *Broker) onPublish(c *conn, f protocol.Frame) {
	if f.Topic == "" || f.Message == nil {
		c.send(protocol.Frame{Type: protocol.FrameError, Reason: "publish requires topic and message"})
		return
	}
	m := *f.Message
	if err := m.Validate(); err != nil {
		c.send(protocol.Frame{Type: protocol.FrameError, ID: m.ID, Reason: err.Error()})
		return
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	qs := b.topics[f.Topic]
	if len(qs) > 0 {
		names := make([]string, len(qs))
		for i, q := range qs {
			names[i] = q.name
		}
		// Persist first, confirm afterwards.
		if err := b.st.append(record{Op: opEnqueue, Queues: names, Msg: &m}); err != nil {
			b.mu.Unlock()
			b.log.Error("persist failed", "message_id", m.ID, "correlation_id", m.CorrelationID, "error", err)
			c.send(protocol.Frame{Type: protocol.FrameError, ID: m.ID, Reason: "broker could not persist message"})
			return
		}
		b.log.Info("message accepted", "topic", f.Topic, "message_id", m.ID, "correlation_id", m.CorrelationID, "queues", len(qs))
		for _, q := range qs {
			b.seq++
			q.pending = append(q.pending, &entry{msg: m, seq: b.seq})
		}
		for _, q := range qs {
			b.dispatch(q)
		}
	}
	routed := len(qs)
	b.mu.Unlock()
	if routed == 0 {
		b.log.Warn("no queue bound to topic, message dropped", "topic", f.Topic, "message_id", m.ID, "correlation_id", m.CorrelationID)
	}
	c.send(protocol.Frame{Type: protocol.FramePubAck, ID: m.ID, Routed: routed})
}

func (b *Broker) onSubscribe(c *conn, f protocol.Frame) {
	if f.Topic == "" || f.Group == "" {
		c.send(protocol.Frame{Type: protocol.FrameError, Reason: "subscribe requires topic and group"})
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c.sub != nil || b.closed {
		c.send(protocol.Frame{Type: protocol.FrameError, Reason: "already subscribed"})
		return
	}
	q, err := b.declareLocked(f.Topic, f.Group)
	if err != nil {
		c.send(protocol.Frame{Type: protocol.FrameError, Reason: "broker could not declare queue"})
		return
	}
	prefetch := f.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	cons := &consumer{name: f.Name, c: c, q: q, prefetch: prefetch}
	c.sub = cons
	q.consumers = append(q.consumers, cons)
	b.log.Info("consumer connected", "queue", q.name, "consumer", f.Name, "consumers", len(q.consumers))
	c.send(protocol.Frame{Type: protocol.FrameSubAck})
	b.dispatch(q)
}

// inflightFor returns the delivery acknowledged by f, if it belongs to this
// connection (mu held).
func (b *Broker) inflightFor(c *conn, f protocol.Frame) (*consumer, *inflightMsg) {
	cons := c.sub
	if cons == nil {
		c.send(protocol.Frame{Type: protocol.FrameError, ID: f.ID, Reason: f.Type + " before subscribe"})
		return nil, nil
	}
	if f.Tag == 0 {
		c.send(protocol.Frame{Type: protocol.FrameError, ID: f.ID, Reason: f.Type + " requires the delivery tag"})
		return nil, nil
	}
	in := cons.q.inflight[f.Tag]
	if in == nil || in.cons != cons {
		b.log.Warn("ignoring "+f.Type+" for unknown or expired delivery", "queue", cons.q.name, "message_id", f.ID, "tag", f.Tag, "consumer", cons.name)
		return nil, nil
	}
	return cons, in
}

func (b *Broker) onAck(c *conn, f protocol.Frame) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cons, in := b.inflightFor(c, f)
	if in == nil {
		return
	}
	q := cons.q
	delete(q.inflight, in.tag)
	cons.inflight--
	id := in.e.msg.ID
	if err := b.st.append(record{Op: opAck, Queue: q.name, ID: id}); err != nil {
		b.log.Error("could not persist ack, message may be redelivered after restart", "message_id", id, "error", err)
	}
	b.log.Info("message acked", "queue", q.name, "message_id", id, "correlation_id", in.e.msg.CorrelationID, "consumer", cons.name, "attempt", in.e.attempts)
	b.dispatch(q)
}

func (b *Broker) onNack(c *conn, f protocol.Frame) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cons, in := b.inflightFor(c, f)
	if in == nil {
		return
	}
	reason := f.Reason
	if reason == "" {
		reason = "nack"
	}
	if f.Permanent {
		b.removeInflight(cons.q, in)
		b.deadLetterLocked(cons.q, in.e, reason, true)
		b.dispatch(cons.q)
		return
	}
	b.failLocked(cons.q, in, reason, false)
}

// dispatch hands pending messages to consumers with free capacity (mu held).
// A message is not given back to a consumer that already failed it while
// another consumer of the group could take it; it waits for that consumer
// instead, and later messages may go first.
func (b *Broker) dispatch(q *queue) {
	for i := 0; i < len(q.pending); {
		e := q.pending[i]
		cons := q.pickConsumer(e)
		if cons == nil {
			if q.pickConsumer(nil) == nil {
				return // nobody has free capacity
			}
			i++ // only consumers that failed this message are free: try the next one
			continue
		}
		q.pending = append(q.pending[:i:i], q.pending[i+1:]...)
		e.attempts++
		b.tag++
		q.inflight[b.tag] = &inflightMsg{tag: b.tag, e: e, cons: cons, deadline: time.Now().Add(b.cfg.AckTimeout)}
		cons.inflight++
		b.log.Info("message delivered", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "consumer", cons.name, "attempt", e.attempts, "tag", b.tag)
		msg := e.msg
		cons.c.send(protocol.Frame{Type: protocol.FrameDeliver, Topic: q.topic, Tag: b.tag, Message: &msg, Attempt: e.attempts, Redelivered: e.attempts > 1})
	}
}

// pickConsumer returns the next consumer (round-robin) with free prefetch
// capacity. For an entry that already failed, consumers that failed it are
// skipped, unless every consumer of the group has failed it (then retrying
// on one of them is the only option).
func (q *queue) pickConsumer(e *entry) *consumer {
	avoid := e != nil && len(e.failedBy) > 0 && !q.allFailed(e)
	n := len(q.consumers)
	for i := 0; i < n; i++ {
		cons := q.consumers[(q.rr+i)%n]
		if cons.inflight >= cons.prefetch || (avoid && e.failedBy[cons]) {
			continue
		}
		q.rr = (q.rr + i + 1) % n
		return cons
	}
	return nil
}

func (q *queue) allFailed(e *entry) bool {
	for _, cons := range q.consumers {
		if !e.failedBy[cons] {
			return false
		}
	}
	return true
}

func (b *Broker) removeInflight(q *queue, in *inflightMsg) {
	delete(q.inflight, in.tag)
	if in.cons.inflight > 0 {
		in.cons.inflight--
	}
}

// deadLetterLocked moves an entry to the DLQ (mu held).
func (b *Broker) deadLetterLocked(q *queue, e *entry, reason string, permanent bool) {
	if err := b.st.append(record{Op: opDLQ, Queue: q.name, ID: e.msg.ID, Msg: &e.msg, Reason: reason, Attempts: e.attempts, Permanent: permanent}); err != nil {
		b.log.Error("could not persist dlq record", "message_id", e.msg.ID, "error", err)
	}
	q.dlq = append(q.dlq, DLQEntry{Message: e.msg, Reason: reason, Attempts: e.attempts, Permanent: permanent, At: time.Now().UTC()})
	b.log.Warn("message moved to DLQ", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "attempts", e.attempts, "permanent", permanent, "reason", reason)
}

// failLocked handles a failed delivery: retry (with backoff, or immediately
// when immediate is true) or dead-letter after MaxAttempts (mu held).
func (b *Broker) failLocked(q *queue, in *inflightMsg, reason string, immediate bool) {
	b.removeInflight(q, in)
	e := in.e
	if e.failedBy == nil {
		e.failedBy = map[*consumer]bool{}
	}
	e.failedBy[in.cons] = true
	if e.attempts >= b.cfg.MaxAttempts {
		b.deadLetterLocked(q, e, reason, false)
		b.dispatch(q)
		return
	}
	if immediate {
		q.requeue(e)
		b.log.Warn("message requeued", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "attempt", e.attempts, "reason", reason)
		b.dispatch(q)
		return
	}
	delay := Backoff(b.cfg.RetryBase, e.attempts)
	q.retrying++
	b.log.Warn("message will be retried", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "attempt", e.attempts, "reason", reason, "retry_in", delay.String())
	time.AfterFunc(delay, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		q.retrying--
		if b.closed {
			return
		}
		q.requeue(e)
		b.dispatch(q)
	})
	b.dispatch(q)
}

// requeue puts the entry back at its original position (ordered by seq).
func (q *queue) requeue(e *entry) {
	i := sort.Search(len(q.pending), func(i int) bool { return q.pending[i].seq > e.seq })
	q.pending = append(q.pending, nil)
	copy(q.pending[i+1:], q.pending[i:])
	q.pending[i] = e
}

// dropConn is called when a connection ends for any reason (clean close,
// crash, network failure). Its in-flight messages are requeued.
func (b *Broker) dropConn(c *conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.conns, c)
	cons := c.sub
	if cons == nil {
		return
	}
	q := cons.q
	for i, x := range q.consumers {
		if x == cons {
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}
	if q.rr >= len(q.consumers) {
		q.rr = 0
	}
	var lost []*inflightMsg
	for _, in := range q.inflight {
		if in.cons == cons {
			lost = append(lost, in)
		}
	}
	b.log.Warn("consumer disconnected", "queue", q.name, "consumer", cons.name, "inflight_requeued", len(lost), "consumers", len(q.consumers))
	for _, in := range lost {
		b.failLocked(q, in, "consumer disconnected before ack", true)
	}
	b.dispatch(q)
}

// sweeper turns expired in-flight messages (no ACK within AckTimeout) into failures.
func (b *Broker) sweeper() {
	defer b.wg.Done()
	tick := b.cfg.AckTimeout / 4
	if tick < 5*time.Millisecond {
		tick = 5 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-b.done:
			return
		case now := <-t.C:
			b.mu.Lock()
			for _, q := range b.queues {
				for _, in := range q.inflight {
					if now.After(in.deadline) {
						b.failLocked(q, in, "ack timeout", false)
					}
				}
			}
			b.mu.Unlock()
		}
	}
}

// Stats returns a snapshot of all queues.
func (b *Broker) Stats() map[string]QueueStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]QueueStats, len(b.queues))
	for name, q := range b.queues {
		out[name] = QueueStats{Pending: len(q.pending), Inflight: len(q.inflight), Retrying: q.retrying, Consumers: len(q.consumers), DLQ: len(q.dlq)}
	}
	return out
}

// DLQ returns the dead-lettered messages of a queue.
func (b *Broker) DLQ(queueName string) []DLQEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[queueName]
	if q == nil {
		return nil
	}
	return append([]DLQEntry(nil), q.dlq...)
}

// Close stops the broker. Unacknowledged messages stay in the log.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.done)
	if b.ln != nil {
		b.ln.Close()
	}
	conns := make([]*conn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	for _, c := range conns {
		c.close()
	}
	b.wg.Wait()
	return b.st.close()
}

// ---------------------------------------------------------------- connection

type conn struct {
	nc        net.Conn
	out       chan protocol.Frame
	done      chan struct{}
	final     chan struct{} // closed by sendFinal: flush out, then close
	once      sync.Once
	finalOnce sync.Once
	sub       *consumer // guarded by Broker.mu
}

func newConn(nc net.Conn) *conn {
	return &conn{nc: nc, out: make(chan protocol.Frame, 1024), done: make(chan struct{}), final: make(chan struct{})}
}

// send never blocks: a connection that cannot keep up is closed.
func (c *conn) send(f protocol.Frame) {
	select {
	case <-c.done:
		return
	default:
	}
	select {
	case c.out <- f:
	default:
		c.close()
	}
}

// sendFinal queues a last frame; the connection is closed once it is written.
func (c *conn) sendFinal(f protocol.Frame) {
	c.send(f)
	c.finalOnce.Do(func() { close(c.final) })
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.done)
		c.nc.Close()
	})
}

func (c *conn) writeLoop() {
	enc := json.NewEncoder(c.nc)
	for {
		select {
		case f := <-c.out:
			if err := enc.Encode(f); err != nil {
				c.close()
				return
			}
		case <-c.final:
			for {
				select {
				case f := <-c.out:
					if enc.Encode(f) != nil {
						c.close()
						return
					}
				default:
					c.close()
					return
				}
			}
		case <-c.done:
			return
		}
	}
}
