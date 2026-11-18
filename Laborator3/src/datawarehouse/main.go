// Command datawarehouse is one replica of the data warehouse (DW).
// `datawarehouse -migrate` creates the Cassandra schema and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pad-lab3/src/common/logx"
	"pad-lab3/src/datawarehouse/dw"
	"pad-lab3/src/datawarehouse/store"
)

func main() {
	migrate := flag.Bool("migrate", false, "create the Cassandra schema and exit")
	flag.Parse()

	node := logx.Env("DW_NODE_NAME", "dw")
	log := logx.New("dw").With("node", node)
	ccfg := store.CassandraConfig{
		Hosts:       logx.EnvList("CASSANDRA_HOSTS", "localhost"),
		Keyspace:    logx.Env("CASSANDRA_KEYSPACE", "pad_lab3"),
		Consistency: logx.Env("CASSANDRA_CONSISTENCY", "QUORUM"),
		Timeout:     logx.EnvDuration("CASSANDRA_TIMEOUT", 2*time.Second),
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *migrate {
		mctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if err := store.Migrate(mctx, ccfg, logx.EnvInt("CASSANDRA_RF", 1), func(m string, a ...any) { log.Info(m, a...) }); err != nil {
			log.Error("migration failed", "error", err)
			os.Exit(1)
		}
		log.Info("schema ready", "event", "schema_ready", "keyspace", ccfg.Keyspace)
		return
	}

	var st store.Store
	if logx.Env("DW_STORE", "cassandra") == "memory" {
		st = store.NewMemory()
		log.Warn("in-memory store: data is not shared between replicas")
	} else {
		for {
			c, err := store.OpenCassandra(ccfg)
			if err == nil {
				st = c
				break
			}
			log.Warn("cassandra not reachable, retrying", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
	defer st.Close()

	faults := &dw.Faults{}
	faults.SetLatency(logx.EnvDuration("DW_LATENCY", 0))
	faults.SetWriteDelay(logx.EnvDuration("DW_WRITE_RESPONSE_DELAY", 0))
	svc := dw.NewService(st, dw.Config{
		Node:        node,
		FeedOverlap: logx.EnvDuration("DW_FEED_OVERLAP", 5*time.Second),
		FeedMax:     logx.EnvInt("DW_FEED_MAX", 1000),
	})
	ctrl := dw.NewController(svc, dw.ControllerConfig{
		Node:         node,
		CacheMaxAge:  logx.EnvInt("DW_CACHE_MAX_AGE", 30),
		MaxInFlight:  logx.EnvInt("DW_MAX_INFLIGHT", 256),
		StoreTimeout: logx.EnvDuration("DW_STORE_TIMEOUT", 2*time.Second),
	}, faults, log)

	srv := &http.Server{
		Addr:              logx.Env("DW_LISTEN", ":8000"),
		Handler:           ctrl.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second, // keep-alive connections from the proxy pool
	}
	admin := &http.Server{Addr: logx.Env("DW_ADMIN_LISTEN", ":9000"), Handler: faults.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = admin.ListenAndServe() }()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		_ = admin.Shutdown(sctx)
	}()
	log.Info("data warehouse listening", "event", "started", "addr", srv.Addr, "admin", admin.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}
