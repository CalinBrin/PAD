// Package source is a storage node ("employee node" in the laboratory
// statement). It owns a set of employees, sends them to the warehouse with
// PUT (through the proxy) and pulls the modifications made by the clients
// with GET /updates/employees (pull, ADR 0005).
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
)

// Config of a source node.
type Config struct {
	Name     string // also sent as From: <name>@<host>
	IDPrefix string
	Seed     int
	DataDir  string // "" keeps the state in memory only
	Poll     time.Duration
	Mutate   time.Duration // 0: never changes its own data
}

// State is what the node persists: its employees and the feed cursor.
type State struct {
	Cursor       string                    `json:"cursor"`
	LastModified string                    `json:"last_modified"`
	Employees    map[string]model.Employee `json:"employees"`
}

// Node is one source node.
type Node struct {
	cfg  Config
	c    *httpx.Client
	log  *slog.Logger
	from string

	mu sync.Mutex
	st State
}

func New(cfg Config, c *httpx.Client, log *slog.Logger) *Node {
	host, _ := os.Hostname()
	if log == nil {
		log = logx.Discard()
	}
	n := &Node{cfg: cfg, c: c, log: log, from: cfg.Name + "@" + host, st: State{Employees: map[string]model.Employee{}}}
	c.From = n.from
	return n
}

// From is the value of the From header sent by the node.
func (n *Node) From() string { return n.from }

// Snapshot returns a copy of the local state.
func (n *Node) Snapshot() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	cp := n.st
	cp.Employees = map[string]model.Employee{}
	for k, v := range n.st.Employees {
		cp.Employees[k] = v
	}
	return cp
}

var (
	firstNames = []string{"Ana", "Ion", "Maria", "Vasile", "Elena", "Mihai", "Irina", "Andrei", "Olga", "Victor"}
	lastNames  = []string{"Popescu", "Rusu", "Ciobanu", "Lungu", "Munteanu", "Ceban", "Turcan", "Bivol", "Rotaru", "Cojocaru"}
	depts      = []string{"IT", "HR", "Finance", "Sales", "Logistics"}
	positions  = []string{"Engineer", "Analyst", "Manager", "Specialist", "Intern"}
)

// SeedEmployees builds the deterministic initial data of the node.
func SeedEmployees(prefix string, count int) []model.Employee {
	out := make([]model.Employee, 0, count)
	for i := 1; i <= count; i++ {
		fn, ln := firstNames[(i-1)%len(firstNames)], lastNames[(i*7)%len(lastNames)]
		out = append(out, model.Employee{
			ID:         fmt.Sprintf("%s-%03d", prefix, i),
			Name:       fn + " " + ln,
			Department: depts[i%len(depts)],
			Position:   positions[(i*3)%len(positions)],
			Email:      fmt.Sprintf("%s.%s%d@example.md", fn, ln, i),
			Salary:     int64(10000 + 750*i),
		})
	}
	return out
}

func (n *Node) statePath() string { return filepath.Join(n.cfg.DataDir, n.cfg.Name+"-state.json") }

// Load reads the persisted state (if any).
func (n *Node) Load() error {
	if n.cfg.DataDir == "" {
		return nil
	}
	b, err := os.ReadFile(n.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := json.Unmarshal(b, &n.st); err != nil {
		return err
	}
	if n.st.Employees == nil {
		n.st.Employees = map[string]model.Employee{}
	}
	return nil
}

func (n *Node) save() {
	if n.cfg.DataDir == "" {
		return
	}
	n.mu.Lock()
	b, _ := json.MarshalIndent(n.st, "", "  ")
	n.mu.Unlock()
	tmp := n.statePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		_ = os.Rename(tmp, n.statePath())
	}
}

// Put sends one employee with retry. PUT is idempotent, so a retry after a
// lost answer is safe: the warehouse answers 200 with changed=false.
func (n *Node) Put(ctx context.Context, e model.Employee) (model.Employee, error) {
	var last error
	for attempt := 1; attempt <= 6; attempt++ {
		cctx := logx.WithCorrelationID(ctx, fmt.Sprintf("%s-put-%s-%s", n.cfg.Name, e.ID, logx.NewID()[:6]))
		resp, err := n.c.Do(cctx, http.MethodPut, "/employees/"+e.ID, e, nil)
		if err == nil && (resp.Status == http.StatusOK || resp.Status == http.StatusCreated) {
			var got model.Employee
			if err := n.c.Decode(resp, &got); err != nil {
				return got, err
			}
			n.mu.Lock()
			n.st.Employees[got.ID] = got
			n.mu.Unlock()
			n.log.Info("employee sent", "event", "employee_put", "id", got.ID, "status", resp.Status,
				"version", got.Version, "changed", resp.Header.Get("X-Changed"), "upstream", resp.Header.Get("X-Upstream"),
				"correlation_id", logx.CorrelationID(cctx))
			return got, nil
		}
		if err == nil {
			last = fmt.Errorf("PUT %s: HTTP %d: %s", e.ID, resp.Status, resp.Body)
			if resp.Status < 500 {
				return model.Employee{}, last
			}
		} else {
			last = err
		}
		n.log.Warn("put failed, retrying", "event", "put_retry", "id", e.ID, "attempt", attempt, "error", last)
		select {
		case <-ctx.Done():
			return model.Employee{}, ctx.Err()
		case <-time.After(time.Duration(200*(1<<attempt)) * time.Millisecond):
		}
	}
	return model.Employee{}, last
}

// Publish sends the initial data: the persisted employees, or the seed.
func (n *Node) Publish(ctx context.Context) error {
	n.mu.Lock()
	emps := make([]model.Employee, 0, len(n.st.Employees))
	for _, e := range n.st.Employees {
		emps = append(emps, e)
	}
	n.mu.Unlock()
	if len(emps) == 0 {
		emps = SeedEmployees(n.cfg.IDPrefix, n.cfg.Seed)
	}
	sort.Slice(emps, func(i, j int) bool { return emps[i].ID < emps[j].ID })
	for _, e := range emps {
		if _, err := n.Put(ctx, e); err != nil {
			return err
		}
	}
	n.save()
	return nil
}

// PullOnce asks the warehouse for the changes after the cursor and applies
// the ones about the node's own employees that it does not have yet
// (higher version). The request is a pure read: repeating it is harmless.
func (n *Node) PullOnce(ctx context.Context) (applied int, err error) {
	n.mu.Lock()
	cursor, lm := n.st.Cursor, n.st.LastModified
	n.mu.Unlock()
	path := "/updates/employees"
	if cursor != "" {
		path += "?since=" + cursor
	}
	hdr := map[string]string{}
	if lm != "" {
		hdr["If-Modified-Since"] = lm
	}
	ctx = logx.WithCorrelationID(ctx, n.cfg.Name+"-pull-"+logx.NewID()[:6])
	resp, err := n.c.Do(ctx, http.MethodGet, path, nil, hdr)
	if err != nil {
		return 0, err
	}
	if resp.Status == http.StatusNotModified {
		return 0, nil
	}
	if resp.Status != http.StatusOK {
		return 0, fmt.Errorf("pull: HTTP %d: %s", resp.Status, resp.Body)
	}
	var feed model.ChangeFeed
	if err := n.c.Decode(resp, &feed); err != nil {
		return 0, err
	}
	n.mu.Lock()
	for _, e := range feed.Items {
		cur, mine := n.st.Employees[e.ID]
		if !mine || e.Version <= cur.Version {
			continue // not ours, or already known (the feed re-sends an overlap window)
		}
		n.st.Employees[e.ID] = e
		applied++
		n.log.Info("modification pulled from the warehouse", "event", "change_applied", "id", e.ID,
			"version", e.Version, "updated_by", e.UpdatedBy, "salary", e.Salary, "name", e.Name,
			"correlation_id", logx.CorrelationID(ctx))
	}
	if feed.Cursor != "" {
		n.st.Cursor = feed.Cursor
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		n.st.LastModified = v
	}
	n.mu.Unlock()
	n.save()
	return applied, nil
}

// MutateOnce changes the salary of one own employee (a change at the source).
func (n *Node) MutateOnce(ctx context.Context) error {
	n.mu.Lock()
	ids := make([]string, 0, len(n.st.Employees))
	for id := range n.st.Employees {
		ids = append(ids, id)
	}
	n.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	n.mu.Lock()
	e := n.st.Employees[ids[rand.IntN(len(ids))]]
	n.mu.Unlock()
	e.Salary += 100
	_, err := n.Put(ctx, e)
	if err == nil {
		n.save()
	}
	return err
}

// Run publishes the data, then pulls (and optionally mutates) until ctx ends.
func (n *Node) Run(ctx context.Context) error {
	if err := n.Load(); err != nil {
		return err
	}
	for {
		err := n.Publish(ctx)
		if err == nil {
			break
		}
		n.log.Warn("initial publish failed, retrying", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	n.log.Info("data published, pulling modifications", "event", "published", "employees", len(n.Snapshot().Employees))
	poll := time.NewTicker(n.cfg.Poll)
	defer poll.Stop()
	var mutate <-chan time.Time
	if n.cfg.Mutate > 0 {
		t := time.NewTicker(n.cfg.Mutate)
		defer t.Stop()
		mutate = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
			if _, err := n.PullOnce(ctx); err != nil {
				n.log.Warn("pull failed, will retry at the next poll", "event", "pull_failed", "error", err)
			}
		case <-mutate:
			if err := n.MutateOnce(ctx); err != nil {
				n.log.Warn("mutation failed", "error", err)
			}
		}
	}
}
