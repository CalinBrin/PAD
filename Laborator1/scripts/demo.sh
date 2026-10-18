#!/usr/bin/env bash
# End-to-end demo with real processes:
#   1. normal order            -> both consumers process it
#   2. FAILURE SCENARIO        -> inventory crashes after the local effect, before ACK;
#                                 it is restarted, the message is redelivered, dedup skips the effect
#   3. poison order (amount<0) -> billing NACKs it as a permanent error, message goes to the DLQ without retry
# Output: logs/*.log and logs/correlated.log (all log lines of the three flows, sorted by time).
set -euo pipefail
cd "$(dirname "$0")/.."

ROOT=$(pwd)
BROKER=127.0.0.1:7000
ADMIN=127.0.0.1:8081
API=127.0.0.1:8080

rm -rf data logs bin
mkdir -p logs bin
go build -o bin/ ./src/integration-service ./src/order-service ./src/consumers/inventory ./src/consumers/billing
mv bin/integration-service bin/broker

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

start() { # name, logfile, command...
  local name=$1 log=$2; shift 2
  "$@" >>"logs/$log.log" 2>&1 &
  PIDS+=($!)
  echo "started $name (pid $!)"
}

wait_for() { # description, command...
  local what=$1; shift
  for _ in $(seq 1 100); do "$@" >/dev/null 2>&1 && return 0; sleep 0.1; done
  echo "timeout waiting for $what" >&2; exit 1
}

start broker broker bin/broker -listen $BROKER -admin $ADMIN -data data/broker -queues orders:inventory,orders:billing -ack-timeout 5s -retry-base 300ms -max-attempts 3
wait_for broker curl -sf http://$ADMIN/stats

start inventory inventory bin/inventory -broker $BROKER -data data/inventory -workers 1 -crash-once after_effect
INV_PID=$!
start billing billing bin/billing -broker $BROKER -data data/billing -workers 2
start order-service order-service bin/order-service -broker $BROKER -listen $API
wait_for api curl -sf http://$API/healthz
sleep 0.5

post() { # correlation id, amount
  curl -s -X POST http://$API/orders -H "Content-Type: application/json" -H "X-Correlation-ID: $1" -d "{\"customer_id\":\"cust-1\",\"amount\":$2}"; echo
}

echo; echo "== 1) FAILURE SCENARIO: inventory crashes after its effect, before ACK"
post demo-crash-001 120
sleep 1
if kill -0 "$INV_PID" 2>/dev/null; then echo "inventory is still running (unexpected)"; else echo "inventory process is dead (exit 137), as intended"; fi
echo "restarting inventory..."
start inventory-restarted inventory bin/inventory -broker $BROKER -data data/inventory -workers 1 -crash-once after_effect
sleep 1.5

echo; echo "== 2) normal order"
post demo-normal-001 50
sleep 1

echo; echo "== 3) poison order (negative amount): billing rejects it as permanent, straight to the DLQ"
post demo-poison-001 -1
sleep 3

echo; echo "== broker queues"; curl -s http://$ADMIN/stats
echo "== DLQ of orders.billing"; curl -s "http://$ADMIN/dlq?queue=orders.billing" | head -30

echo; echo "== effects applied (message_id appears exactly once per consumer)"
for f in data/inventory/effects.jsonl data/billing/effects.jsonl; do
  echo "$f: $(wc -l < "$f") effects, $(jq -r .message_id "$f" | sort -u | wc -l) distinct message ids"
done

# correlated log: every line that mentions one of the correlation ids, sorted by time
cat logs/broker.log logs/inventory.log logs/billing.log logs/order-service.log \
  | grep -E 'correlation_id=demo-(crash|normal|poison)-001' | sort -s -k1,1 > logs/correlated.log
echo; echo "correlated log written to logs/correlated.log ($(wc -l < logs/correlated.log) lines)"
