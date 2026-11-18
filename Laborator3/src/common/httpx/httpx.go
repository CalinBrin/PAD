// Package httpx holds the HTTP helpers shared by the warehouse, the proxy and
// the clients: content negotiation (JSON / XML), encoding, decoding and the
// correlation ID middleware.
package httpx

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
)

// Format is a representation of the resources.
type Format string

const (
	JSON Format = "json"
	XML  Format = "xml"
)

// ContentType returns the media type of the format.
func (f Format) ContentType() string {
	if f == XML {
		return "application/xml; charset=utf-8"
	}
	return "application/json; charset=utf-8"
}

// MaxBody limits request bodies.
const MaxBody = 1 << 20

// Negotiate chooses the representation of the answer.
// ?format=json|xml wins over the Accept header. An empty Accept, */* or
// application/* gives JSON. ok=false means nothing acceptable (HTTP 406).
func Negotiate(r *http.Request) (Format, bool) {
	switch strings.ToLower(r.URL.Query().Get("format")) {
	case "json":
		return JSON, true
	case "xml":
		return XML, true
	case "":
	default:
		return JSON, false
	}
	accept := strings.TrimSpace(r.Header.Get("Accept"))
	if accept == "" {
		return JSON, true
	}
	best, bestQ := Format(""), -1.0
	for _, part := range strings.Split(accept, ",") {
		mt, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		q := 1.0
		if v, ok := params["q"]; ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				q = f
			}
		}
		if q <= 0 {
			continue
		}
		var f Format
		switch mt {
		case "application/json", "text/json", "*/*", "application/*":
			f = JSON
		case "application/xml", "text/xml":
			f = XML
		default:
			continue
		}
		// on equal q the first range listed wins
		if q > bestQ {
			best, bestQ = f, q
		}
	}
	if best == "" {
		return JSON, false
	}
	return best, true
}

// Write encodes v in format f with the given status.
func Write(w http.ResponseWriter, f Format, status int, v any) {
	body, err := Encode(f, v)
	if err != nil {
		http.Error(w, "encoding error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", f.ContentType())
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// Encode serialises v as JSON or XML.
func Encode(f Format, v any) ([]byte, error) {
	if f == XML {
		b, err := xml.MarshalIndent(v, "", "  ")
		if err != nil {
			return nil, err
		}
		return append([]byte(xml.Header), append(b, '\n')...), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// ErrUnsupportedMedia is returned for a Content-Type that is neither JSON nor XML.
var ErrUnsupportedMedia = errors.New("unsupported media type")

// FormatOfContentType maps a Content-Type to a format. An empty type is JSON.
func FormatOfContentType(ct string) (Format, error) {
	if strings.TrimSpace(ct) == "" {
		return JSON, nil
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return JSON, ErrUnsupportedMedia
	}
	switch mt {
	case "application/json", "text/json":
		return JSON, nil
	case "application/xml", "text/xml":
		return XML, nil
	}
	return JSON, ErrUnsupportedMedia
}

// Decode reads a JSON or XML body according to Content-Type.
func Decode(r *http.Request, v any) error {
	f, err := FormatOfContentType(r.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
	if err != nil {
		return fmt.Errorf("%w: %v", model.ErrInvalid, err)
	}
	if len(body) > MaxBody {
		return fmt.Errorf("%w: body larger than %d bytes", model.ErrInvalid, MaxBody)
	}
	return DecodeBytes(f, body, v)
}

// DecodeBytes parses a JSON or XML document.
func DecodeBytes(f Format, body []byte, v any) error {
	var err error
	if f == XML {
		err = xml.Unmarshal(body, v)
	} else {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		err = dec.Decode(v)
	}
	if err != nil {
		return fmt.Errorf("%w: malformed %s body: %v", model.ErrInvalid, f, err)
	}
	return nil
}

// WriteError answers with a model.Error in the negotiated format.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	f, _ := Negotiate(r)
	w.Header().Set("Cache-Control", "no-store")
	Write(w, f, status, model.Error{Code: code, Message: msg, CorrelationID: logx.CorrelationID(r.Context())})
}

// StatusRecorder remembers the status code written by a handler.
type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

func (s *StatusRecorder) WriteHeader(code int) {
	if s.Status == 0 {
		s.Status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *StatusRecorder) Write(b []byte) (int, error) {
	if s.Status == 0 {
		s.Status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Middleware reads (or creates) the correlation ID, echoes it in the answer
// and writes one log line per request.
func Middleware(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get(logx.CorrelationHeader)
		if id == "" {
			id = logx.NewID()
			r.Header.Set(logx.CorrelationHeader, id)
		}
		r = r.WithContext(logx.WithCorrelationID(r.Context(), id))
		w.Header().Set(logx.CorrelationHeader, id)
		rec := &StatusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		log.Info("http request", "event", "http_request", "method", r.Method, "path", r.URL.RequestURI(),
			"status", rec.Status, "duration_ms", time.Since(start).Milliseconds(), "correlation_id", id)
	})
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
