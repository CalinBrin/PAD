// Package logx holds what every process shares: structured logs, the
// correlation ID (X-Correlation-ID) and configuration read from the
// environment.
package logx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// CorrelationHeader carries the correlation ID between every component.
const CorrelationHeader = "X-Correlation-ID"

// New returns a logger that tags every line with the component name.
// LOG_FORMAT=json switches from key=value text to JSON.
func New(component string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	var h slog.Handler
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "json") {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h).With("component", component)
}

// Discard returns a logger that writes nothing (tests).
func Discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type ctxKey struct{}

// WithCorrelationID stores the correlation ID in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the correlation ID stored in ctx ("" if none).
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// NewID returns a random 64-bit hex identifier.
func NewID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Env returns the variable or def when it is unset or empty.
func Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// EnvInt returns the integer variable or def.
func EnvInt(key string, def int) int {
	if v, err := strconv.Atoi(Env(key, "")); err == nil {
		return v
	}
	return def
}

// EnvDuration returns the duration variable (Go syntax: 500ms, 2s) or def.
func EnvDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(Env(key, "")); err == nil {
		return v
	}
	return def
}

// EnvBool returns the boolean variable (1/true/yes) or def.
func EnvBool(key string, def bool) bool {
	switch strings.ToLower(Env(key, "")) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

// EnvList splits a comma separated variable.
func EnvList(key, def string) []string {
	var out []string
	for _, p := range strings.Split(Env(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
