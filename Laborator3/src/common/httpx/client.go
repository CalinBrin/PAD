package httpx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"pad-lab3/src/common/logx"
)

// Response is a fully read HTTP answer.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Client sends JSON or XML requests (the source nodes, the CLI, the load generator).
// Its transport keeps connections alive towards the proxy.
type Client struct {
	Base   string
	Format Format
	From   string // the From header (name of the node)
	HTTP   *http.Client
}

func NewClient(base string, f Format, timeout time.Duration) *Client {
	return &Client{Base: base, Format: f, HTTP: &http.Client{Timeout: timeout,
		Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}}}
}

// Do sends method path with v encoded in the client format (v may be nil).
func (c *Client) Do(ctx context.Context, method, path string, v any, hdr map[string]string) (*Response, error) {
	var body io.Reader
	if v != nil {
		b, err := Encode(c.Format, v)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return nil, err
	}
	accept := "application/json"
	if c.Format == XML {
		accept = "application/xml"
	}
	req.Header.Set("Accept", accept)
	if v != nil {
		req.Header.Set("Content-Type", c.Format.ContentType())
	}
	if c.From != "" {
		req.Header.Set("From", c.From)
	}
	if id := logx.CorrelationID(ctx); id != "" {
		req.Header.Set(logx.CorrelationHeader, id)
	}
	for k, val := range hdr {
		req.Header.Set(k, val)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Response{Status: resp.StatusCode, Header: resp.Header, Body: rb}, nil
}

// Decode parses the body of r in the client format.
func (c *Client) Decode(r *Response, v any) error {
	f := c.Format
	if ct := r.Header.Get("Content-Type"); ct != "" {
		if got, err := FormatOfContentType(ct); err == nil {
			f = got
		}
	}
	return DecodeBytesLoose(f, r.Body, v)
}

// DecodeBytesLoose is DecodeBytes accepting unknown JSON fields (answers).
func DecodeBytesLoose(f Format, body []byte, v any) error {
	if f == XML {
		return DecodeBytes(XML, body, v)
	}
	if err := jsonUnmarshal(body, v); err != nil {
		return fmt.Errorf("malformed json answer: %w", err)
	}
	return nil
}
