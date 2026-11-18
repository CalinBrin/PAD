// Command source-node is a storage node that sends its employees to the
// warehouse (PUT, through the proxy) and pulls the client modifications.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/source-node/source"
)

func main() {
	name := logx.Env("NODE_NAME", "source-json")
	log := logx.New("source").With("node", name)
	f := httpx.JSON
	if logx.Env("NODE_FORMAT", "json") == "xml" {
		f = httpx.XML
	}
	c := httpx.NewClient(logx.Env("PROXY_URL", "http://localhost:8080"), f, logx.EnvDuration("SOURCE_TIMEOUT", 8*time.Second))
	n := source.New(source.Config{
		Name:     name,
		IDPrefix: logx.Env("SOURCE_ID_PREFIX", "emp"),
		Seed:     logx.EnvInt("SOURCE_SEED_COUNT", 20),
		DataDir:  logx.Env("SOURCE_DATA_DIR", ""),
		Poll:     logx.EnvDuration("SOURCE_POLL_INTERVAL", 2*time.Second),
		Mutate:   logx.EnvDuration("SOURCE_MUTATE_INTERVAL", 0),
	}, c, log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("source node starting", "event", "started", "format", f, "from", n.From())
	if err := n.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("source node failed", "error", err)
		os.Exit(1)
	}
}
