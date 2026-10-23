// Package broker is the transport-independent core of the message broker. The
// gRPC adapter (package server) turns RPCs into calls of this package.
//
// Model: a producer publishes to a topic. For every consumer group (queue)
// bound to the topic the broker keeps one copy. Consumers of the same group
// compete for messages (round-robin load balancing, bounded by prefetch);
// different groups each get every message.
//
// Guarantees (see docs/delivery-guarantees.md):
//   - Publish answers only after the message is fsynced in every queue
//   - Publish is idempotent by message ID (deduplication window, persisted)
//   - Publish to a topic without a queue is refused, nothing is silently lost
//   - delivery is at-least-once: a message stays in-flight until ACK
//   - NACK, ack timeout and a closed session cause redelivery; the queue (not
//     the consumer process) owns the message, so a restarted consumer of the
//     same group receives it
//   - a retried message goes to a consumer that has not failed it yet
//   - permanent NACK or MaxAttempts failures move the message to the DLQ
//   - ordering: FIFO per queue for first deliveries, none between competing
//     consumers of a group
package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalid    = errors.New("invalid message")
	ErrNoQueue    = errors.New("no queue is bound to the topic")
	ErrIDConflict = errors.New("message_id already used for a different message")
	ErrStore      = errors.New("message could not be stored")
	ErrClosed     = errors.New("broker is shut down")
)

type Config struct {
	DataDir        string
	MaxAttempts    int           // deliveries before DLQ for transient failures (default 3)
	AckTimeout     time.Duration // in-flight without ACK is redelivered (default 30s)
	RetryBase      time.Duration // base of the exponential retry backoff (default 500ms)
	DedupRetention time.Duration // how long a published ID is remembered (default 24h)
	Logger         *slog.Logger
	// WriteFault, if set, is called before every log write; a non-nil error
	// simulates a failing disk (fault injection for tests).
	WriteFault func(op string) error
}

// Message is the broker's view of an envelope.
type Message struct {
	ID            string `json:"id"`
	CorrelationID string `json:"correlation_id"`
	Type          string `json:"type"`
	SchemaVersion int32  `json:"schema_version"`
	OccurredAt    string `json:"occurred_at"`
	Payload       []byte `json:"payload"`
}

// Delivery is one delivery of a message to a consumer.
type Delivery struct {
	Tag         uint64
	Msg         Message
	Attempt     int
	Redelivered bool
}

type PublishResult struct {
	Routed    int
	Duplicate bool
}

// MaxPrefetch bounds the unacknowledged deliveries of one session (and the
// memory the broker reserves for it).
const MaxPrefetch = 1000

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
	msg      Message
	attempts int
	seq      uint64
	failedBy map[*Consumer]bool // consumers that NACKed or timed out on this entry
}

// Consumer is one subscription session of a consumer group.
type Consumer struct {
	Name     string
	Queue    string
	q        *queue
	prefetch int
	inflight int
	out      chan Delivery
	done     chan struct{}
	gone     bool
}

// Deliveries returns the channel the session reads deliveries from.
func (c *Consumer) Deliveries() <-chan Delivery { return c.out }

// Done is closed when the broker ends the session (shutdown, overload).
func (c *Consumer) Done() <-chan struct{} { return c.done }

type inflightMsg struct {
	tag      uint64
	e        *entry
	cons     *Consumer
	deadline time.Time
}

type queue struct {
	name, topic, group string
	pending            []*entry
	inflight           map[uint64]*inflightMsg // key: delivery tag
	consumers          []*Consumer
	rr                 int
	retrying           int
	dlq                []DLQEntry
}

type pubInfo struct {
	hash   string
	routed int
	at     time.Time
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

	mu        sync.Mutex // guards everything below
	queues    map[string]*queue
	topics    map[string][]*queue
	published map[string]pubInfo // message ID -> first publish (dedup window)
	seq       uint64
	tag       uint64
	closed    bool
	storeErr  error // last failed write, cleared by the next successful one

	done chan struct{}
	wg   sync.WaitGroup
}

func New(cfg Config) (*Broker, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 500 * time.Millisecond
	}
	if cfg.DedupRetention <= 0 {
		cfg.DedupRetention = 24 * time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.DataDir == "" {
		return nil, errors.New("broker: DataDir is required")
	}
	st, replayed, err := openStore(cfg.DataDir, cfg.DedupRetention, cfg.WriteFault, cfg.Logger)
	if err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, log: cfg.Logger, st: st, queues: map[string]*queue{}, topics: map[string][]*queue{},
		published: replayed.published, done: make(chan struct{})}
	for _, d := range replayed.decls {
		q := b.newQueue(d.Name, d.Topic, d.Group)
		for _, m := range replayed.pending[d.Name] {
			b.seq++
			q.pending = append(q.pending, &entry{msg: m, seq: b.seq})
		}
		q.dlq = replayed.dlq[d.Name]
		if len(q.pending) > 0 || len(q.dlq) > 0 {
			b.log.Info("recovered queue from log", "event", "queue_recovered", "queue", q.name, "pending", len(q.pending), "dlq", len(q.dlq))
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

// Declare creates the durable queue of (topic, group) if it does not exist.
// Messages published afterwards are kept for the group even while none of
// its consumers is connected.
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
	if err := b.writeLocked(record{Op: opDeclare, Queue: name, Topic: topic, Group: group}); err != nil {
		return nil, err
	}
	b.log.Info("queue declared", "event", "queue_declared", "queue", name)
	return b.newQueue(name, topic, group), nil
}

func (b *Broker) writeLocked(r record) error {
	if err := b.st.append(r); err != nil {
		b.storeErr = err
		return fmt.Errorf("%w: %v", ErrStore, err)
	}
	b.storeErr = nil
	return nil
}

// Publish stores the message in every queue bound to the topic.
//
//   - same ID, same content as an earlier publish: Duplicate=true, nothing new
//     stored (the earlier publish was durable, its answer was probably lost)
//   - same ID, different content: ErrIDConflict
//   - no queue bound: ErrNoQueue, nothing stored
//   - ctx already done (the caller gave up): its error, nothing stored
//   - write failed: ErrStore, nothing stored, the caller may retry
func (b *Broker) Publish(ctx context.Context, topic string, m Message) (PublishResult, error) {
	if topic == "" {
		return PublishResult{}, fmt.Errorf("%w: topic is required", ErrInvalid)
	}
	hash := m.hash()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return PublishResult{}, ErrClosed
	}
	if p, ok := b.published[m.ID]; ok {
		if p.hash != hash {
			return PublishResult{}, ErrIDConflict
		}
		b.log.Info("duplicate publish, already stored", "event", "publish_duplicate", "topic", topic, "message_id", m.ID, "correlation_id", m.CorrelationID)
		return PublishResult{Routed: p.routed, Duplicate: true}, nil
	}
	qs := b.topics[topic]
	if len(qs) == 0 {
		b.log.Warn("publish refused, no queue bound to topic", "event", "publish_refused", "topic", topic, "message_id", m.ID, "correlation_id", m.CorrelationID)
		return PublishResult{}, ErrNoQueue
	}
	if err := ctx.Err(); err != nil {
		// The caller already gave up: do not commit an operation nobody waits for.
		b.log.Warn("publish cancelled before commit, nothing stored", "event", "publish_cancelled", "message_id", m.ID, "correlation_id", m.CorrelationID, "error", err)
		return PublishResult{}, err
	}
	names := make([]string, len(qs))
	for i, q := range qs {
		names[i] = q.name
	}
	now := time.Now().UTC()
	if err := b.writeLocked(record{Op: opEnqueue, Queues: names, Msg: &m, Hash: hash, Routed: len(qs)}); err != nil {
		b.log.Error("publish failed, message not stored", "event", "publish_failed", "message_id", m.ID, "correlation_id", m.CorrelationID, "error", err)
		return PublishResult{}, err
	}
	b.published[m.ID] = pubInfo{hash: hash, routed: len(qs), at: now}
	b.log.Info("message accepted", "event", "message_accepted", "topic", topic, "message_id", m.ID, "correlation_id", m.CorrelationID, "queues", len(qs))
	for _, q := range qs {
		b.seq++
		q.pending = append(q.pending, &entry{msg: m, seq: b.seq})
	}
	for _, q := range qs {
		b.dispatch(q)
	}
	return PublishResult{Routed: len(qs)}, nil
}

// Subscribe opens a session for the group's queue (declared if needed).
func (b *Broker) Subscribe(topic, group, name string, prefetch int) (*Consumer, error) {
	if topic == "" || group == "" {
		return nil, fmt.Errorf("%w: subscribe requires topic and group", ErrInvalid)
	}
	if prefetch <= 0 {
		prefetch = 1
	}
	if prefetch > MaxPrefetch {
		return nil, fmt.Errorf("%w: prefetch must be <= %d", ErrInvalid, MaxPrefetch)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	q, err := b.declareLocked(topic, group)
	if err != nil {
		return nil, err
	}
	c := &Consumer{Name: name, Queue: q.name, q: q, prefetch: prefetch, out: make(chan Delivery, prefetch), done: make(chan struct{})}
	q.consumers = append(q.consumers, c)
	b.log.Info("consumer connected", "event", "consumer_connected", "queue", q.name, "consumer", name, "consumers", len(q.consumers))
	b.dispatch(q)
	return c, nil
}

// Ack confirms a delivery. Unknown or expired tags are ignored (logged).
func (b *Broker) Ack(c *Consumer, tag uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	in := b.inflightFor(c, tag, "ack")
	if in == nil {
		return
	}
	q := c.q
	b.removeInflight(q, in)
	id := in.e.msg.ID
	if err := b.writeLocked(record{Op: opAck, Queue: q.name, ID: id}); err != nil {
		b.log.Error("could not persist ack, message may be redelivered after restart", "event", "ack_persist_failed", "message_id", id, "error", err)
	}
	b.log.Info("message acked", "event", "message_acked", "queue", q.name, "message_id", id, "correlation_id", in.e.msg.CorrelationID, "consumer", c.Name, "attempt", in.e.attempts)
	b.dispatch(q)
}

// Nack reports a failed delivery: permanent goes to the DLQ at once,
// otherwise the message is retried (with backoff) or dead-lettered.
func (b *Broker) Nack(c *Consumer, tag uint64, reason string, permanent bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	in := b.inflightFor(c, tag, "nack")
	if in == nil {
		return
	}
	if reason == "" {
		reason = "nack"
	}
	if permanent {
		b.removeInflight(c.q, in)
		b.deadLetterLocked(c.q, in.e, reason, true)
		b.dispatch(c.q)
		return
	}
	b.failLocked(c.q, in, reason, false)
}

func (b *Broker) inflightFor(c *Consumer, tag uint64, op string) *inflightMsg {
	in := c.q.inflight[tag]
	if in == nil || in.cons != c {
		b.log.Warn("ignoring "+op+" for unknown or expired delivery", "event", "stale_"+op, "queue", c.q.name, "tag", tag, "consumer", c.Name)
		return nil
	}
	return in
}

// Unsubscribe ends a session for any reason (clean close, crash, network
// failure). Its unacknowledged deliveries go back to the queue at once.
func (b *Broker) Unsubscribe(c *Consumer, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeConsumerLocked(c, reason)
}

func (b *Broker) removeConsumerLocked(c *Consumer, reason string) {
	if c.gone {
		return
	}
	c.gone = true
	close(c.done)
	q := c.q
	for i, x := range q.consumers {
		if x == c {
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}
	if q.rr >= len(q.consumers) {
		q.rr = 0
	}
	var lost []*inflightMsg
	for _, in := range q.inflight {
		if in.cons == c {
			lost = append(lost, in)
		}
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i].tag < lost[j].tag })
	b.log.Warn("consumer disconnected", "event", "consumer_disconnected", "queue", q.name, "consumer", c.Name, "reason", reason, "inflight_requeued", len(lost), "consumers", len(q.consumers))
	for _, in := range lost {
		b.failLocked(q, in, "consumer disconnected before ack", true)
	}
	b.dispatch(q)
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
			i++
			continue
		}
		q.pending = append(q.pending[:i:i], q.pending[i+1:]...)
		e.attempts++
		b.tag++
		in := &inflightMsg{tag: b.tag, e: e, cons: cons, deadline: time.Now().Add(b.cfg.AckTimeout)}
		q.inflight[b.tag] = in
		cons.inflight++
		b.log.Info("message delivered", "event", "message_delivered", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "consumer", cons.Name, "attempt", e.attempts, "tag", b.tag)
		select {
		case cons.out <- Delivery{Tag: b.tag, Msg: e.msg, Attempt: e.attempts, Redelivered: e.attempts > 1}:
		default:
			// Cannot happen while inflight <= prefetch = cap(out); kept as a guard.
			b.removeConsumerLocked(cons, "delivery buffer full")
		}
	}
}

// pickConsumer returns the next consumer (round-robin) with free prefetch
// capacity. For an entry that already failed, consumers that failed it are
// skipped unless every connected consumer has failed it.
func (q *queue) pickConsumer(e *entry) *Consumer {
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

func (b *Broker) deadLetterLocked(q *queue, e *entry, reason string, permanent bool) {
	if err := b.writeLocked(record{Op: opDLQ, Queue: q.name, ID: e.msg.ID, Msg: &e.msg, Reason: reason, Attempts: e.attempts, Permanent: permanent}); err != nil {
		b.log.Error("could not persist dlq record", "event", "dlq_persist_failed", "message_id", e.msg.ID, "error", err)
	}
	q.dlq = append(q.dlq, DLQEntry{Message: e.msg, Reason: reason, Attempts: e.attempts, Permanent: permanent, At: time.Now().UTC()})
	b.log.Warn("message moved to DLQ", "event", "message_dead_lettered", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "attempts", e.attempts, "permanent", permanent, "reason", reason)
}

// failLocked handles a failed delivery: retry (with backoff, or immediately
// when immediate is true) or dead-letter after MaxAttempts (mu held).
func (b *Broker) failLocked(q *queue, in *inflightMsg, reason string, immediate bool) {
	b.removeInflight(q, in)
	e := in.e
	if e.failedBy == nil {
		e.failedBy = map[*Consumer]bool{}
	}
	e.failedBy[in.cons] = true
	if e.attempts >= b.cfg.MaxAttempts {
		b.deadLetterLocked(q, e, reason, false)
		b.dispatch(q)
		return
	}
	if immediate {
		q.requeue(e)
		b.log.Warn("message requeued", "event", "message_requeued", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "attempt", e.attempts, "reason", reason)
		b.dispatch(q)
		return
	}
	delay := Backoff(b.cfg.RetryBase, e.attempts)
	q.retrying++
	b.log.Warn("message will be retried", "event", "message_retry_scheduled", "queue", q.name, "message_id", e.msg.ID, "correlation_id", e.msg.CorrelationID, "attempt", e.attempts, "reason", reason, "retry_in_ms", delay.Milliseconds())
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
				var expired []*inflightMsg
				for _, in := range q.inflight {
					if now.After(in.deadline) {
						expired = append(expired, in)
					}
				}
				for _, in := range expired {
					b.failLocked(q, in, "ack timeout", false)
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

// Ready returns nil when the broker can accept traffic: not shut down and the
// last write to its log succeeded.
func (b *Broker) Ready() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if b.storeErr != nil {
		return fmt.Errorf("storage unavailable: %v", b.storeErr)
	}
	return nil
}

// Close stops the broker. Unacknowledged messages stay in the log. Open
// sessions are ended (their Done channel is closed).
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.done)
	for _, q := range b.queues {
		for _, c := range q.consumers {
			if !c.gone {
				c.gone = true
				close(c.done)
			}
		}
	}
	b.mu.Unlock()
	b.wg.Wait()
	return b.st.close()
}
