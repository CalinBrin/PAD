package dw

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/datawarehouse/store"
)

// ControllerConfig configures the HTTP side of a warehouse replica.
type ControllerConfig struct {
	Node         string
	CacheMaxAge  int // seconds, sent as Cache-Control: max-age for GET answers
	MaxInFlight  int // concurrent requests served; others wait (then 503)
	StoreTimeout time.Duration
}

// Controller maps the HTTP requests on the service operations.
// net/http serves every connection in its own goroutine (the Go equivalent of
// thread-per-request); MaxInFlight bounds how many are processed at once.
type Controller struct {
	svc    *Service
	cfg    ControllerConfig
	faults *Faults
	log    *slog.Logger
	slots  chan struct{}
}

func NewController(svc *Service, cfg ControllerConfig, faults *Faults, log *slog.Logger) *Controller {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 256
	}
	if cfg.StoreTimeout <= 0 {
		cfg.StoreTimeout = 2 * time.Second
	}
	if faults == nil {
		faults = &Faults{}
	}
	if log == nil {
		log = logx.Discard()
	}
	return &Controller{svc: svc, cfg: cfg, faults: faults, log: log, slots: make(chan struct{}, cfg.MaxInFlight)}
}

// Handler returns the routes of contracts/http-api.md, including the aliases
// used in the laboratory statement (/employee/?id=1, /employees/?offset=&limit=,
// /update/employees/).
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /employees/{id}", c.data(c.getEmployee))
	mux.HandleFunc("GET /employee", c.data(c.getEmployee))
	mux.HandleFunc("GET /employee/{$}", c.data(c.getEmployee))
	mux.HandleFunc("GET /employees", c.data(c.listEmployees))
	mux.HandleFunc("GET /employees/{$}", c.data(c.listEmployees))
	mux.HandleFunc("PUT /employees/{id}", c.data(c.putEmployee))
	mux.HandleFunc("PUT /employee", c.data(c.putEmployee))
	mux.HandleFunc("PUT /employee/{$}", c.data(c.putEmployee))
	mux.HandleFunc("POST /employees/{id}", c.data(c.postEmployee))
	mux.HandleFunc("POST /employee", c.data(c.postEmployee))
	mux.HandleFunc("POST /employee/{$}", c.data(c.postEmployee))
	mux.HandleFunc("GET /updates/employees", c.data(c.updates))
	mux.HandleFunc("GET /update/employees/{$}", c.data(c.updates))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", c.readyz)
	return httpx.Middleware(c.log, c.served(mux))
}

func (c *Controller) served(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", c.cfg.Node)
		next.ServeHTTP(w, r)
	})
}

// data wraps a data endpoint: injected faults, concurrency bound, store timeout.
func (c *Controller) data(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.faults.served.Add(1)
		sleep(r.Context(), c.faults.Latency())
		if c.faults.fail.Load() {
			httpx.WriteError(w, r, http.StatusServiceUnavailable, "unavailable", "injected fault: node failing")
			return
		}
		select {
		case c.slots <- struct{}{}:
			defer func() { <-c.slots }()
		case <-r.Context().Done():
			httpx.WriteError(w, r, http.StatusServiceUnavailable, "overloaded", "too many concurrent requests")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), c.cfg.StoreTimeout)
		defer cancel()
		h(w, r.WithContext(ctx))
	}
}

func (c *Controller) fail(w http.ResponseWriter, r *http.Request, err error) {
	var status int
	var code string
	switch {
	case errors.Is(err, model.ErrInvalid):
		status, code = http.StatusBadRequest, "invalid"
	case errors.Is(err, httpx.ErrUnsupportedMedia):
		status, code = http.StatusUnsupportedMediaType, "unsupported_media_type"
	case errors.Is(err, store.ErrNotFound):
		status, code = http.StatusNotFound, "not_found"
	case errors.Is(err, ErrPrecondition):
		status, code = http.StatusPreconditionFailed, "version_mismatch"
	case errors.Is(err, ErrIdempotencyMismatch):
		status, code = http.StatusUnprocessableEntity, "idempotency_key_reused"
	case errors.Is(err, store.ErrConflict):
		status, code = http.StatusConflict, "conflict"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		status, code = http.StatusServiceUnavailable, "timeout"
	default:
		status, code = http.StatusServiceUnavailable, "storage_unavailable"
		c.log.Warn("storage error", "event", "storage_error", "error", err,
			"correlation_id", logx.CorrelationID(r.Context()))
	}
	httpx.WriteError(w, r, status, code, err.Error())
}

func etag(e model.Employee, f httpx.Format) string {
	return fmt.Sprintf(`"v%d-%s"`, e.Version, f)
}

// parseIfMatch reads `"v<N>-..."` or `"v<N>"` or `<N>`; -1 when absent.
func parseIfMatch(h string) (int64, error) {
	h = strings.TrimSpace(h)
	if h == "" {
		return -1, nil
	}
	h = strings.TrimPrefix(strings.Trim(strings.TrimPrefix(h, "W/"), `"`), "v")
	if i := strings.IndexByte(h, '-'); i >= 0 {
		h = h[:i]
	}
	v, err := strconv.ParseInt(h, 10, 64)
	if err != nil || v < 0 {
		return -1, fmt.Errorf("%w: If-Match must be a version etag", model.ErrInvalid)
	}
	return v, nil
}

func (c *Controller) format(w http.ResponseWriter, r *http.Request) (httpx.Format, bool) {
	f, ok := httpx.Negotiate(r)
	if !ok {
		httpx.WriteError(w, r, http.StatusNotAcceptable, "not_acceptable", "supported formats: application/json, application/xml")
	}
	return f, ok
}

func pathID(r *http.Request) string {
	if id := r.PathValue("id"); id != "" {
		return id
	}
	return r.URL.Query().Get("id")
}

func (c *Controller) writeEmployee(w http.ResponseWriter, r *http.Request, f httpx.Format, status int, e model.Employee, cacheable bool) {
	h := w.Header()
	h.Set("ETag", etag(e, f))
	h.Set("Last-Modified", e.UpdatedAt.UTC().Format(http.TimeFormat))
	h.Set("Vary", "Accept")
	if cacheable {
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", c.cfg.CacheMaxAge))
	} else {
		h.Set("Cache-Control", "no-store")
	}
	httpx.Write(w, f, status, e)
}

func (c *Controller) getEmployee(w http.ResponseWriter, r *http.Request) {
	f, ok := c.format(w, r)
	if !ok {
		return
	}
	id := pathID(r)
	if !model.ValidID(id) {
		c.fail(w, r, fmt.Errorf("%w: missing or invalid id", model.ErrInvalid))
		return
	}
	e, err := c.svc.Get(r.Context(), id)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	sleep(context.WithoutCancel(r.Context()), c.faults.ReadDelay())
	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag(e, f) {
		w.Header().Set("ETag", inm)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	c.writeEmployee(w, r, f, http.StatusOK, e, true)
}

func intParam(r *http.Request, name string, def, min, max int) (int, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%w: %s must be an integer in [%d, %d]", model.ErrInvalid, name, min, max)
	}
	return n, nil
}

func (c *Controller) listEmployees(w http.ResponseWriter, r *http.Request) {
	f, ok := c.format(w, r)
	if !ok {
		return
	}
	offset, err := intParam(r, "offset", 0, 0, 1_000_000)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	limit, err := intParam(r, "limit", 10, 1, 100)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	page, err := c.svc.List(r.Context(), offset, limit)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	sleep(context.WithoutCancel(r.Context()), c.faults.ReadDelay())
	w.Header().Set("Vary", "Accept")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", c.cfg.CacheMaxAge))
	httpx.Write(w, f, http.StatusOK, page)
}

func (c *Controller) putEmployee(w http.ResponseWriter, r *http.Request) {
	f, ok := c.format(w, r)
	if !ok {
		return
	}
	var in model.Employee
	if err := httpx.Decode(r, &in); err != nil {
		c.fail(w, r, err)
		return
	}
	if id := pathID(r); id != "" {
		if in.ID != "" && in.ID != id {
			c.fail(w, r, fmt.Errorf("%w: id in body (%s) differs from the URL (%s)", model.ErrInvalid, in.ID, id))
			return
		}
		in.ID = id
	}
	ifMatch, err := parseIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	res, err := c.svc.Put(r.Context(), in, r.Header.Get("From"), ifMatch)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.logWrite(r, "PUT", res)
	// Fault injection: the write is committed, the answer is late.
	sleep(context.WithoutCancel(r.Context()), c.faults.WriteDelay())
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
		w.Header().Set("Location", "/employees/"+res.Employee.ID)
	}
	w.Header().Set("X-Changed", strconv.FormatBool(res.Changed))
	c.writeEmployee(w, r, f, status, res.Employee, false)
}

func (c *Controller) postEmployee(w http.ResponseWriter, r *http.Request) {
	f, ok := c.format(w, r)
	if !ok {
		return
	}
	id := pathID(r)
	if !model.ValidID(id) {
		c.fail(w, r, fmt.Errorf("%w: missing or invalid id", model.ErrInvalid))
		return
	}
	var p model.EmployeePatch
	if err := httpx.Decode(r, &p); err != nil {
		c.fail(w, r, err)
		return
	}
	res, err := c.svc.Patch(r.Context(), id, p, r.Header.Get("From"), r.Header.Get("Idempotency-Key"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.logWrite(r, "POST", res)
	sleep(context.WithoutCancel(r.Context()), c.faults.WriteDelay())
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("X-Changed", strconv.FormatBool(res.Changed))
	c.writeEmployee(w, r, f, http.StatusOK, res.Employee, false)
}

func (c *Controller) logWrite(r *http.Request, method string, res WriteResult) {
	c.log.Info("employee written", "event", "employee_written", "method", method, "id", res.Employee.ID,
		"version", res.Employee.Version, "created", res.Created, "changed", res.Changed, "replayed", res.Replayed,
		"by", res.Employee.UpdatedBy, "correlation_id", logx.CorrelationID(r.Context()))
}

// updates: GET /updates/employees?since=<cursor> (or If-Modified-Since).
func (c *Controller) updates(w http.ResponseWriter, r *http.Request) {
	f, ok := c.format(w, r)
	if !ok {
		return
	}
	var cursor time.Time
	ims := r.Header.Get("If-Modified-Since")
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			c.fail(w, r, fmt.Errorf("%w: since must be an RFC 3339 timestamp (the cursor of the previous answer)", model.ErrInvalid))
			return
		}
		cursor = t.UTC()
	} else if ims != "" {
		t, err := http.ParseTime(ims)
		if err != nil {
			c.fail(w, r, fmt.Errorf("%w: invalid If-Modified-Since", model.ErrInvalid))
			return
		}
		cursor = t.UTC()
	}
	items, next, err := c.svc.Changes(r.Context(), cursor)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	if !next.IsZero() {
		h.Set("Last-Modified", next.Format(http.TimeFormat))
	}
	if len(items) == 0 && ims != "" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	cur := ""
	if !next.IsZero() {
		cur = next.Format(time.RFC3339Nano)
	}
	httpx.Write(w, f, http.StatusOK, model.ChangeFeed{Cursor: cur, Items: items})
}

func (c *Controller) readyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if c.faults.notReady.Load() {
		http.Error(w, "not ready (injected fault)", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := c.svc.Ready(ctx); err != nil {
		http.Error(w, "storage unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready " + c.cfg.Node + "\n"))
}
