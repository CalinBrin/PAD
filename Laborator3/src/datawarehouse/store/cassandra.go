package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/gocql/gocql"

	"pad-lab3/src/common/model"
)

// Schema is applied once by `datawarehouse -migrate` (the `schema` service in
// docker compose), never concurrently by the replicas.
// The SAI index lets the change feed query `updated_at > ?` without a scan.
func Schema(keyspace string, rf int) []string {
	return []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}`, keyspace, rf),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.employees (
			id text PRIMARY KEY,
			name text, department text, position text, email text, salary bigint,
			version bigint, updated_at timestamp, updated_by text)`, keyspace),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS employees_updated_at ON %s.employees (updated_at) USING 'sai'`, keyspace),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.idempotency (
			key text PRIMARY KEY, request_hash text, status int, employee text)
			WITH default_time_to_live = 86400`, keyspace),
	}
}

// CassandraConfig configures the connection.
type CassandraConfig struct {
	Hosts       []string
	Keyspace    string
	Consistency string // QUORUM by default
	Timeout     time.Duration
}

// Cassandra stores the employees in one Cassandra keyspace shared by every
// warehouse replica. Writes are lightweight transactions (Paxos):
// `IF NOT EXISTS` / `IF version = ?`, so two replicas writing the same
// employee at the same time cannot both win.
type Cassandra struct {
	s *gocql.Session
}

func cluster(cfg CassandraConfig) *gocql.ClusterConfig {
	c := gocql.NewCluster(cfg.Hosts...)
	c.Timeout = cfg.Timeout
	if c.Timeout == 0 {
		c.Timeout = 2 * time.Second
	}
	c.ConnectTimeout = 5 * time.Second
	c.Consistency = gocql.Quorum
	if cfg.Consistency != "" {
		if cl, err := gocql.ParseConsistencyWrapper(cfg.Consistency); err == nil {
			c.Consistency = cl
		}
	}
	c.SerialConsistency = gocql.LocalSerial
	c.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 1}
	return c
}

// OpenCassandra connects to the keyspace (which must exist).
func OpenCassandra(cfg CassandraConfig) (*Cassandra, error) {
	c := cluster(cfg)
	c.Keyspace = cfg.Keyspace
	s, err := c.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return &Cassandra{s: s}, nil
}

// Migrate creates the keyspace and the tables, waiting for Cassandra to start.
func Migrate(ctx context.Context, cfg CassandraConfig, rf int, logf func(string, ...any)) error {
	var s *gocql.Session
	var err error
	for {
		s, err = cluster(cfg).CreateSession()
		if err == nil {
			break
		}
		logf("cassandra not ready, retrying", "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("cassandra not reachable: %w", err)
		case <-time.After(3 * time.Second):
		}
	}
	defer s.Close()
	for _, stmt := range Schema(cfg.Keyspace, rf) {
		if err := s.Query(stmt).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return s.AwaitSchemaAgreement(ctx)
}

const cols = `id, name, department, position, email, salary, version, updated_at, updated_by`

func scanEmployee(sc interface{ Scan(...any) bool }) (model.Employee, bool) {
	var e model.Employee
	ok := sc.Scan(&e.ID, &e.Name, &e.Department, &e.Position, &e.Email, &e.Salary, &e.Version, &e.UpdatedAt, &e.UpdatedBy)
	e.UpdatedAt = e.UpdatedAt.UTC()
	return e, ok
}

// wrap classifies a driver error: unavailable / timeout (the node cannot
// serve, 503) or an error of this request (500).
func wrap(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var unavailable *gocql.RequestErrUnavailable
	var readTimeout *gocql.RequestErrReadTimeout
	var writeTimeout *gocql.RequestErrWriteTimeout
	var netErr net.Error
	if errors.As(err, &unavailable) || errors.As(err, &readTimeout) || errors.As(err, &writeTimeout) ||
		errors.As(err, &netErr) || errors.Is(err, gocql.ErrNoConnections) || errors.Is(err, gocql.ErrTimeoutNoResponse) ||
		errors.Is(err, gocql.ErrConnectionClosed) || errors.Is(err, gocql.ErrSessionClosed) {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return fmt.Errorf("%w: %v", ErrInternal, err)
}

func (c *Cassandra) Get(ctx context.Context, id string) (model.Employee, error) {
	it := c.s.Query(`SELECT `+cols+` FROM employees WHERE id = ?`, id).WithContext(ctx).Iter()
	e, ok := scanEmployee(it)
	if err := it.Close(); err != nil {
		return model.Employee{}, wrap(err)
	}
	if !ok {
		return model.Employee{}, ErrNotFound
	}
	return e, nil
}

// List pages in Cassandra token order (stable while the data does not change).
// Cassandra has no OFFSET: the rows before offset are read and skipped, which
// is fine for the sizes of this laboratory (limit <= 100).
func (c *Cassandra) List(ctx context.Context, offset, limit int) ([]model.Employee, int, error) {
	var total int
	if err := c.s.Query(`SELECT COUNT(*) FROM employees`).WithContext(ctx).Scan(&total); err != nil {
		return nil, 0, wrap(err)
	}
	it := c.s.Query(`SELECT ` + cols + ` FROM employees`).WithContext(ctx).PageSize(500).Iter()
	out := []model.Employee{}
	for i := 0; len(out) < limit; i++ {
		e, ok := scanEmployee(it)
		if !ok {
			break
		}
		if i >= offset {
			out = append(out, e)
		}
	}
	if err := it.Close(); err != nil {
		return nil, 0, wrap(err)
	}
	return out, total, nil
}

func (c *Cassandra) CompareAndSet(ctx context.Context, e model.Employee, expected int64) error {
	var q *gocql.Query
	if expected == 0 {
		q = c.s.Query(`INSERT INTO employees (`+cols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
			e.ID, e.Name, e.Department, e.Position, e.Email, e.Salary, e.Version, e.UpdatedAt, e.UpdatedBy)
	} else {
		q = c.s.Query(`UPDATE employees SET name = ?, department = ?, position = ?, email = ?, salary = ?,
			version = ?, updated_at = ?, updated_by = ? WHERE id = ? IF version = ?`,
			e.Name, e.Department, e.Position, e.Email, e.Salary, e.Version, e.UpdatedAt, e.UpdatedBy, e.ID, expected)
	}
	applied, err := q.WithContext(ctx).MapScanCAS(map[string]any{})
	if err != nil {
		return wrap(err)
	}
	if !applied {
		return ErrConflict
	}
	return nil
}

func (c *Cassandra) ChangedSince(ctx context.Context, since time.Time, max int) ([]model.Employee, error) {
	q := c.s.Query(`SELECT `+cols+` FROM employees WHERE updated_at > ?`, since)
	if since.Before(time.Unix(0, 0)) {
		// first pull (no cursor): every row. Cassandra 5.0.2 SAI fails on a
		// range bound before the epoch (year 1, Go's zero time).
		q = c.s.Query(`SELECT ` + cols + ` FROM employees`)
	}
	it := q.WithContext(ctx).PageSize(500).Iter()
	out := []model.Employee{}
	for {
		e, ok := scanEmployee(it)
		if !ok {
			break
		}
		out = append(out, e)
	}
	if err := it.Close(); err != nil {
		return nil, wrap(err)
	}
	SortByUpdate(out)
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (c *Cassandra) GetIdempotent(ctx context.Context, key string) (IdemRecord, bool, error) {
	var r IdemRecord
	var body string
	err := c.s.Query(`SELECT request_hash, status, employee FROM idempotency WHERE key = ?`, key).
		WithContext(ctx).Scan(&r.RequestHash, &r.Status, &body)
	if errors.Is(err, gocql.ErrNotFound) {
		return r, false, nil
	}
	if err != nil {
		return r, false, wrap(err)
	}
	if err := decodeEmployee(body, &r.Employee); err != nil {
		return r, false, err
	}
	return r, true, nil
}

func (c *Cassandra) SaveIdempotent(ctx context.Context, key string, rec IdemRecord) error {
	body, err := encodeEmployee(rec.Employee)
	if err != nil {
		return err
	}
	return wrap(c.s.Query(`INSERT INTO idempotency (key, request_hash, status, employee) VALUES (?, ?, ?, ?)`,
		key, rec.RequestHash, rec.Status, body).WithContext(ctx).Exec())
}

func (c *Cassandra) Ping(ctx context.Context) error {
	var v string
	return wrap(c.s.Query(`SELECT release_version FROM system.local`).WithContext(ctx).Scan(&v))
}

func (c *Cassandra) Close() { c.s.Close() }

func encodeEmployee(e model.Employee) (string, error) {
	b, err := json.Marshal(e)
	return string(b), err
}

func decodeEmployee(s string, e *model.Employee) error { return json.Unmarshal([]byte(s), e) }
