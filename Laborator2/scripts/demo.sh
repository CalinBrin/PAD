#!/usr/bin/env bash
# End-to-end demo with real processes (Linux, macOS, WSL). Needs: go, curl, jq,
# and the generated code (./scripts/gen.sh). On Windows use the Docker steps
# of README.md instead.
#
# Scenarios (correlation IDs in brackets):
#   1. publish answer lost after the commit, retry deduplicated   [demo-lost-publish-001]
#   2. repeated HTTP request with the same Idempotency-Key        [demo-idem-001]
#   3. permanent error (unsupported currency) straight to the DLQ [demo-dlq-001]
#   4. inventory crashes after its effect, before ACK             [demo-crash-001]
#   5. payment answer lost after the charge, billing reconciles   [demo-lost-charge-001]
#   6. broker down: order kept pending, published by the outbox   [demo-outbox-001]
#   7. load balancing between the two billing replicas            [demo-lb-*]
# Output: logs/*.log and docs-ready evidence in logs/evidence/.
set -euo pipefail
cd "$(dirname "$0")/.."

export PAYMENT_API_TOKEN=${PAYMENT_API_TOKEN:-demo-token}
BROKER=127.0.0.1:7000; ADMIN=127.0.0.1:8081; API=127.0.0.1:8080; PAYMENT=127.0.0.1:9000; PADMIN=127.0.0.1:8082

rm -rf data logs bin && mkdir -p logs/evidence bin
go build -o bin/ ./src/integration-service ./src/payment-service ./src/order-service ./src/consumers/inventory ./src/consumers/billing
mv bin/integration-service bin/broker

declare -A PID
cleanup() { for p in "${PID[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

start() { # key, logfile, env..., -- command
  local key=$1 log=$2; shift 2
  env "$@" >>"logs/$log.log" 2>&1 &
  PID[$key]=$!
}
wait_http() { for _ in $(seq 1 100); do curl -sf "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done; echo "timeout: $1" >&2; exit 1; }
post() { # correlation id, idempotency key, json body
  curl -s -X POST "http://$API/orders" -H "X-Correlation-ID: $1" -H "Idempotency-Key: $2" -H "Content-Type: application/json" -d "$3" -w '  [HTTP %{http_code}]\n'
}
start_broker() { # lose_publish_responses
  start broker broker BROKER_LISTEN=$BROKER BROKER_ADMIN=$ADMIN BROKER_DATA_DIR=data/broker BROKER_QUEUES=orders:inventory,orders:billing \
    BROKER_MAX_ATTEMPTS=5 BROKER_ACK_TIMEOUT=5s BROKER_RETRY_BASE=300ms BROKER_LOSE_PUBLISH_RESPONSES=$1 bin/broker
  wait_http "http://$ADMIN/readyz"
}
start_inventory() { # crash_once stage ("" for none)
  start inventory inventory BROKER_ADDR=$BROKER CONSUMER_DATA_DIR=data/inventory CONSUMER_WORKERS=1 CONSUMER_HEALTH_ADDR= CRASH_ONCE=$1 bin/inventory
}

start_broker 1   # the answer of the very first publish will be lost
start payment payment PAYMENT_LISTEN=$PAYMENT PAYMENT_ADMIN=$PADMIN PAYMENT_DATA_DIR=data/payment bin/payment-service
wait_http "http://$PADMIN/readyz"
start_inventory ""
for r in 1 2; do
  start billing$r billing-$r BROKER_ADDR=$BROKER PAYMENT_ADDR=$PAYMENT CONSUMER_WORKERS=1 CONSUMER_HEALTH_ADDR= HOSTNAME=replica$r bin/billing
done
start orders order-service ORDER_LISTEN=$API ORDER_DATA_DIR=data/orders BROKER_ADDR=$BROKER PUBLISH_TIMEOUT=500ms OUTBOX_INTERVAL=1s bin/order-service
wait_http "http://$API/healthz"
sleep 1

echo "== 1) publish answer lost after the commit"
post demo-lost-publish-001 req-lost-publish '{"customer_id":"c1","amount_cents":5000}'
sleep 2

echo "== 2) same request twice (same Idempotency-Key)"
post demo-idem-001 req-idem '{"customer_id":"c2","amount_cents":1500,"currency":"EUR"}'
post demo-idem-001 req-idem '{"customer_id":"c2","amount_cents":1500,"currency":"EUR"}'
sleep 1

echo "== 3) unsupported currency: permanent error, DLQ"
post demo-dlq-001 req-dlq '{"customer_id":"c3","amount_cents":700,"currency":"GBP"}'
sleep 1
curl -s "http://$ADMIN/dlq?queue=orders.billing" | jq -c '.[] | {id: .message.id, correlation_id: .message.correlation_id, reason, attempts, permanent}'

echo "== 4) inventory crashes after its effect, before ACK"
kill "${PID[inventory]}"; wait "${PID[inventory]}" 2>/dev/null || true
start_inventory after_effect     # crashes once, on the next message it handles
sleep 1
post demo-crash-001 req-crash '{"customer_id":"c4","amount_cents":120}'
sleep 1
if kill -0 "${PID[inventory]}" 2>/dev/null; then echo "inventory still running (unexpected)"; else echo "inventory crashed (exit 137) as injected; restarting it"; fi
start_inventory after_effect     # the marker file keeps it from crashing again
sleep 2

echo "== 5) payment answer lost after the charge"
curl -s -X POST "http://$PADMIN/faults?lose_responses=1"; echo
post demo-lost-charge-001 req-lost-charge '{"customer_id":"c5","amount_cents":4200,"currency":"MDL"}'
sleep 3

echo "== 6) broker down: outbox fallback"
kill "${PID[broker]}"; wait "${PID[broker]}" 2>/dev/null || true
post demo-outbox-001 req-outbox '{"customer_id":"c6","amount_cents":990}'
sleep 1
start_broker 0
sleep 4
curl -s "http://$API/orders/$(curl -s -X POST "http://$API/orders" -H 'Idempotency-Key: req-outbox' -d '{"customer_id":"c6","amount_cents":990}' | jq -r .order_id)" | jq -c '{order_id, status, publish_attempts}'

echo "== 7) load balancing: 10 orders, 2 billing replicas"
for i in $(seq -w 1 10); do post "demo-lb-$i" "req-lb-$i" '{"customer_id":"c7","amount_cents":100}' >/dev/null; done
sleep 3
for r in 1 2; do echo "billing-$r charged $(grep -c 'event=charge_approved.*demo-lb-\|demo-lb-.*event=charge_approved' logs/billing-$r.log || true) of the demo-lb orders"; done

echo; echo "== broker queues"; curl -s "http://$ADMIN/stats" | jq -c .
echo "== charges executed by the payment service"; curl -s "http://$PADMIN/charges" | jq -r '.[] | "\(.order_id) \(.status) \(.correlation_id)"' | sort | uniq -c | sort -rn | head -20
echo "== inventory effects: lines per message id"; jq -r .message_id data/inventory/effects.jsonl | sort | uniq -c | sort -rn | head -3

for id in demo-lost-publish-001 demo-idem-001 demo-dlq-001 demo-crash-001 demo-lost-charge-001 demo-outbox-001; do
  cat logs/*.log | grep -F "$id" | sort -s -k1,1 > "logs/evidence/$id.log" || true
done
echo; echo "evidence written to logs/evidence/"; wc -l logs/evidence/*
