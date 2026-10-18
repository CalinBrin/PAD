package broker

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"pad-lab1/src/integration-service/protocol"
)

// The broker persists every state change in an append-only log
// (one JSON record per line, fsync after each write):
//
//	declare  - a queue (topic + consumer group) exists
//	enqueue  - a message was accepted for one or more queues
//	ack      - a consumer confirmed processing
//	dlq      - the message was moved to the dead-letter queue
//
// On startup the log is replayed: enqueued and not acked/dead-lettered
// messages become pending again (retransmission of undelivered messages).
// The log is then compacted (rewritten with the live state only).

const (
	opDeclare = "declare"
	opEnqueue = "enqueue"
	opAck     = "ack"
	opDLQ     = "dlq"
)

type record struct {
	Op        string            `json:"op"`
	Topic     string            `json:"topic,omitempty"`
	Group     string            `json:"group,omitempty"`
	Queue     string            `json:"queue,omitempty"`
	Queues    []string          `json:"queues,omitempty"`
	ID        string            `json:"id,omitempty"`
	Msg       *protocol.Message `json:"msg,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Attempts  int               `json:"attempts,omitempty"`
	Permanent bool              `json:"permanent,omitempty"`
	At        time.Time         `json:"at"`
}

// DLQEntry is a dead-lettered message.
type DLQEntry struct {
	Message   protocol.Message `json:"message"`
	Reason    string           `json:"reason"`
	Attempts  int              `json:"attempts"`
	Permanent bool             `json:"permanent,omitempty"` // rejected by the consumer as not retryable
	At        time.Time        `json:"at"`
}

type declared struct{ Name, Topic, Group string }

// state is the in-memory result of replaying the log.
type state struct {
	decls   []declared
	known   map[string]bool
	pending map[string][]protocol.Message
	dlq     map[string][]DLQEntry
}

func newState() *state {
	return &state{known: map[string]bool{}, pending: map[string][]protocol.Message{}, dlq: map[string][]DLQEntry{}}
}

func (s *state) apply(r record) {
	switch r.Op {
	case opDeclare:
		if !s.known[r.Queue] {
			s.known[r.Queue] = true
			s.decls = append(s.decls, declared{r.Queue, r.Topic, r.Group})
		}
	case opEnqueue:
		if r.Msg == nil {
			return
		}
		for _, q := range r.Queues {
			if s.known[q] {
				s.pending[q] = append(s.pending[q], *r.Msg)
			}
		}
	case opAck:
		s.remove(r.Queue, r.ID)
	case opDLQ:
		s.remove(r.Queue, r.ID)
		if r.Msg != nil {
			s.dlq[r.Queue] = append(s.dlq[r.Queue], DLQEntry{Message: *r.Msg, Reason: r.Reason, Attempts: r.Attempts, Permanent: r.Permanent, At: r.At})
		}
	}
}

func (s *state) remove(queue, id string) {
	list := s.pending[queue]
	for i := range list {
		if list[i].ID == id {
			s.pending[queue] = append(list[:i:i], list[i+1:]...)
			return
		}
	}
}

type store struct {
	mu sync.Mutex
	f  *os.File
}

func openStore(dir string, log *slog.Logger) (*store, *state, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, "broker.log")
	st := newState()
	if err := replay(path, st, log); err != nil {
		return nil, nil, err
	}
	if err := compact(path, st); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return &store{f: f}, st, nil
}

func replay(path string, st *state, log *slog.Logger) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			var rec record
			if jerr := json.Unmarshal(line, &rec); jerr != nil {
				// A torn last line (crash during write) or a corrupt line is skipped.
				log.Warn("ignoring unreadable log record", "error", jerr)
			} else {
				st.apply(rec)
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func compact(path string, st *state) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	now := time.Now().UTC()
	for _, d := range st.decls {
		_ = enc.Encode(record{Op: opDeclare, Queue: d.Name, Topic: d.Topic, Group: d.Group, At: now})
	}
	for _, d := range st.decls {
		for i := range st.pending[d.Name] {
			m := st.pending[d.Name][i]
			_ = enc.Encode(record{Op: opEnqueue, Queues: []string{d.Name}, Msg: &m, At: now})
		}
		for _, e := range st.dlq[d.Name] {
			m := e.Message
			_ = enc.Encode(record{Op: opDLQ, Queue: d.Name, ID: m.ID, Msg: &m, Reason: e.Reason, Attempts: e.Attempts, Permanent: e.Permanent, At: e.At})
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// append writes one record and fsyncs it.
func (s *store) append(r record) error {
	r.At = time.Now().UTC()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(b); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
