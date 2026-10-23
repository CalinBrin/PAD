package rpcx

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Configuration comes from environment variables (see .env.example), so the
// same image runs in every environment. Secrets are read the same way and are
// never logged.

func Env(name, def string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return def
}

func EnvInt(name string, def int) int {
	v := Env(name, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s=%q is not an integer", name, v))
	}
	return n
}

func EnvDuration(name string, def time.Duration) time.Duration {
	v := Env(name, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s=%q is not a duration (e.g. 500ms, 2s)", name, v))
	}
	return d
}

func EnvBool(name string, def bool) bool {
	v := Env(name, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s=%q is not a boolean", name, v))
	}
	return b
}

// NewLogger returns a structured logger with UTC timestamps and the component
// name on every line. LOG_FORMAT=json switches from key=value to JSON.
func NewLogger(component string) *slog.Logger {
	return NewLoggerTo(os.Stderr, component, Env("LOG_FORMAT", "text"))
}

func NewLoggerTo(w io.Writer, component, format string) *slog.Logger {
	opts := &slog.HandlerOptions{ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey && len(groups) == 0 {
			a.Value = slog.StringValue(a.Value.Time().UTC().Format("2006-01-02T15:04:05.000Z"))
		}
		return a
	}}
	var h slog.Handler = slog.NewTextHandler(w, opts)
	if format == "json" {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h).With("component", component)
}

// Discard is a logger that drops everything (tests).
func Discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
