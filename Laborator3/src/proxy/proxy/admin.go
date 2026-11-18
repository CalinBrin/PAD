package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"pad-lab3/src/proxy/balancer"
)

// StatsView is the JSON of GET /proxy/stats.
type StatsView struct {
	Requests int64 `json:"requests"`
	Cache    struct {
		Enabled              bool    `json:"enabled"`
		Available            bool    `json:"available"`
		Hits                 int64   `json:"hits"`
		Misses               int64   `json:"misses"`
		Bypass               int64   `json:"bypass"`
		HitRatio             float64 `json:"hit_ratio"`
		Invalidations        int64   `json:"invalidations"`
		DelayedInvalidations int64   `json:"delayed_invalidations"`
		InvalidationErrors   int64   `json:"invalidation_errors"`
		RedisErrors          int64   `json:"redis_errors"`
	} `json:"cache"`
	Upstream struct {
		Algorithm     string            `json:"algorithm"`
		Requests      int64             `json:"requests"`
		Retries       int64             `json:"retries"`
		Errors        int64             `json:"errors"`
		NoBackend     int64             `json:"no_backend"`
		BreakerOpened int64             `json:"breaker_opened"`
		ConnsNew      int64             `json:"conns_new"`
		ConnsReused   int64             `json:"conns_reused"`
		Backends      []balancer.Status `json:"backends"`
	} `json:"upstream"`
	Clients struct {
		OpenConns     int64 `json:"open_conns"`
		RegistryDrops int64 `json:"registry_dropped_events"`
	} `json:"clients"`
}

// View collects the counters.
func (p *Proxy) View(l *LimitListener) StatsView {
	var v StatsView
	s := &p.Stats
	v.Requests = s.Requests.Load()
	v.Cache.Enabled = p.CacheEnabled()
	v.Cache.Available = p.cache != nil && p.cache.Available()
	v.Cache.Hits, v.Cache.Misses, v.Cache.Bypass = s.Hits.Load(), s.Misses.Load(), s.Bypass.Load()
	if t := v.Cache.Hits + v.Cache.Misses; t > 0 {
		v.Cache.HitRatio = float64(v.Cache.Hits) / float64(t)
	}
	v.Cache.Invalidations = s.Invalidations.Load()
	v.Cache.DelayedInvalidations = s.DelayedInvalidations.Load()
	v.Cache.InvalidationErrors = s.InvalidationErr.Load()
	if p.cache != nil {
		v.Cache.RedisErrors = p.cache.Errors()
	}
	v.Upstream.Algorithm = p.pool.Algorithm()
	v.Upstream.Requests, v.Upstream.Retries, v.Upstream.Errors = s.Upstream.Load(), s.Retries.Load(), s.UpstreamErrors.Load()
	v.Upstream.NoBackend, v.Upstream.BreakerOpened = s.NoBackend.Load(), s.BreakerOpened.Load()
	v.Upstream.ConnsNew, v.Upstream.ConnsReused = s.ConnsNew.Load(), s.ConnsReused.Load()
	v.Upstream.Backends = p.pool.Status()
	if l != nil {
		v.Clients.OpenConns = l.OpenConns()
	}
	if p.reg != nil {
		v.Clients.RegistryDrops = p.reg.Dropped()
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// AdminHandler serves the admin port (never exposed to the data clients):
//
//	GET  /healthz, /readyz             the proxy itself (ready = at least one replica in rotation)
//	GET  /proxy/stats                  counters, cache, backends
//	GET  /proxy/clients                client registry (Redis, every proxy replica)
//	GET  /proxy/requests/{id}          one request, by correlation ID
//	POST /proxy/cache?enabled=false    cache on/off at run time
//	POST /proxy/balancer?algorithm=least_conn
func (p *Proxy) AdminHandler(l *LimitListener) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		for _, b := range p.pool.Backends() {
			if b.Healthy() {
				_, _ = w.Write([]byte("ready\n"))
				return
			}
		}
		http.Error(w, "no backend in rotation", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("GET /proxy/stats", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, p.View(l)) })
	mux.HandleFunc("GET /proxy/clients", func(w http.ResponseWriter, r *http.Request) {
		if p.reg == nil {
			writeJSON(w, 200, map[string]any{"source": "none", "clients": []ClientInfo{}})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		cs, err := p.reg.Shared(ctx)
		if err != nil {
			writeJSON(w, 200, map[string]any{"source": "local (redis unavailable)", "clients": p.reg.Local()})
			return
		}
		writeJSON(w, 200, map[string]any{"source": "redis", "clients": cs})
	})
	mux.HandleFunc("GET /proxy/requests/{id}", func(w http.ResponseWriter, r *http.Request) {
		if p.reg == nil {
			writeJSON(w, 404, map[string]string{"error": "no registry"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		m, err := p.reg.Request(ctx, r.PathValue("id"))
		if err != nil {
			writeJSON(w, 404, map[string]string{"error": "unknown request (records are kept 10 minutes)"})
			return
		}
		writeJSON(w, 200, m)
	})
	mux.HandleFunc("POST /proxy/cache", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("enabled") {
		case "true", "1":
			p.SetCacheEnabled(true)
		case "false", "0":
			p.SetCacheEnabled(false)
		}
		writeJSON(w, 200, map[string]bool{"enabled": p.CacheEnabled()})
	})
	mux.HandleFunc("POST /proxy/balancer", func(w http.ResponseWriter, r *http.Request) {
		if err := p.pool.SetAlgorithm(r.URL.Query().Get("algorithm")); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]string{"algorithm": p.pool.Algorithm()})
	})
	return mux
}
