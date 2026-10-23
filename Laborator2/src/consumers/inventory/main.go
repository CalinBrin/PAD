// Command inventory is the first consumer: it reserves stock for each order.
package main

import (
	"os"

	"pad-lab2/src/consumers/common"
	"pad-lab2/src/integration-service/rpcx"
)

func main() {
	log := rpcx.NewLogger("inventory")
	dataDir := rpcx.Env("CONSUMER_DATA_DIR", "./data/inventory")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Error("cannot create data dir", "error", err)
		os.Exit(1)
	}
	store, err := common.OpenEffectStore(dataDir + "/effects.jsonl")
	if err != nil {
		log.Error("cannot open effect store", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	inv := &common.Inventory{Store: store, Dedup: rpcx.EnvBool("CONSUMER_DEDUP", true),
		CrashAt: common.CrashOnceHook(rpcx.Env("CRASH_ONCE", ""), dataDir),
		Delay:   rpcx.EnvDuration("CONSUMER_WORK_DELAY", 0), Log: log}
	(&common.Process{
		Group: "inventory", Workers: rpcx.EnvInt("CONSUMER_WORKERS", 2), Prefetch: rpcx.EnvInt("CONSUMER_PREFETCH", 1),
		AckMode: rpcx.Env("CONSUMER_ACK_MODE", "after"), BrokerAddr: rpcx.Env("BROKER_ADDR", "localhost:7000"),
		HealthAddr: rpcx.Env("CONSUMER_HEALTH_ADDR", ":8090"), Handler: inv.Handle, Log: log,
	}).Run()
}
