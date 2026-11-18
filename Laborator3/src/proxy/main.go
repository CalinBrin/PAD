// Command proxy is the reverse proxy in front of the data warehouse replicas.
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/proxy/balancer"
	"pad-lab3/src/proxy/cache"
	"pad-lab3/src/proxy/proxy"
)

func main() {
	host, _ := os.Hostname()
	id := logx.Env("PROXY_ID", host)
	log := logx.New("proxy").With("proxy", id)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	threshold := logx.EnvInt("BREAKER_THRESHOLD", 3)
	cooldown := logx.EnvDuration("BREAKER_COOLDOWN", 5*time.Second)
	backends, err := balancer.ParseBackends(logx.EnvList("PROXY_BACKENDS", "dw-1=http://localhost:8000"),
		func() *balancer.Breaker { return balancer.NewBreaker(threshold, cooldown) })
	if err != nil {
		log.Error("configuration", "error", err)
		os.Exit(1)
	}
	pool := balancer.NewPool(backends, logx.Env("PROXY_ALGORITHM", balancer.RoundRobin))

	var c *cache.Redis
	var reg *proxy.Registry
	if addr := logx.Env("REDIS_ADDR", ""); addr != "" {
		c = cache.NewRedis(addr, logx.EnvDuration("REDIS_TIMEOUT", 200*time.Millisecond))
		defer c.Close()
		reg = proxy.NewRegistry(id, c.Client(), log)
		go reg.Run(ctx)
		// keeps the Redis fail-fast window short once Redis is back
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if !c.Available() {
						pctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
						if c.Ping(pctx) == nil {
							log.Info("redis reachable again, cache back in use", "event", "redis_up")
						}
						cancel()
					}
				}
			}
		}()
	} else {
		log.Warn("REDIS_ADDR not set: no cache, no shared client registry")
		reg = proxy.NewRegistry(id, nil, log)
	}

	p := proxy.New(proxy.Config{
		UpstreamTimeout: logx.EnvDuration("UPSTREAM_TIMEOUT", 2*time.Second),
		RequestTimeout:  logx.EnvDuration("REQUEST_TIMEOUT", 6*time.Second),
		MaxAttempts:     logx.EnvInt("MAX_ATTEMPTS", 3),
		RetryBackoff:    logx.EnvDuration("RETRY_BACKOFF", 50*time.Millisecond),
		CacheTTL:        logx.EnvDuration("CACHE_TTL", 30*time.Second),
		WriteGrace:      logx.EnvDuration("WRITE_GRACE", 3*time.Second),
		CacheEnabled:    logx.EnvBool("CACHE_ENABLED", true),
	}, pool, c, reg, log)

	hc := &balancer.HealthChecker{
		Pool:     pool,
		Interval: logx.EnvDuration("HEALTH_INTERVAL", 2*time.Second),
		Timeout:  logx.EnvDuration("HEALTH_TIMEOUT", time.Second),
		Fall:     logx.EnvInt("HEALTH_FALL", 2),
		Rise:     logx.EnvInt("HEALTH_RISE", 2),
		Client:   &http.Client{Transport: proxy.NewTransport()},
		Log:      log,
	}
	go hc.Run(ctx)

	ln, err := net.Listen("tcp", logx.Env("PROXY_LISTEN", ":8080"))
	if err != nil {
		log.Error("listen", "error", err)
		os.Exit(1)
	}
	lim := &proxy.LimitListener{Listener: ln, MaxPerClient: logx.EnvInt("MAX_CONNS_PER_CLIENT", 100), Reg: reg}
	srv := &http.Server{
		Handler:           httpx.Middleware(log, p),
		ReadHeaderTimeout: 5 * time.Second,
		// client connections are kept open (HTTP/1.1 keep-alive) between requests
		IdleTimeout:    logx.EnvDuration("CLIENT_IDLE_TIMEOUT", 120*time.Second),
		MaxHeaderBytes: 64 << 10,
	}
	admin := &http.Server{Addr: logx.Env("PROXY_ADMIN_LISTEN", ":8081"), Handler: p.AdminHandler(lim), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = admin.ListenAndServe() }()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		_ = admin.Shutdown(sctx)
	}()
	names := make([]string, 0, len(backends))
	for _, b := range backends {
		names = append(names, b.Name+"="+b.URL.String())
	}
	log.Info("proxy listening", "event", "started", "addr", ln.Addr().String(), "admin", admin.Addr,
		"backends", names, "algorithm", pool.Algorithm(), "cache", p.CacheEnabled())
	if err := srv.Serve(lim); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}
