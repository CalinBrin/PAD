#!/usr/bin/env sh
# Measurements for docs/evidence/results: with / without cache, healthy /
# slow replica / replica down / Redis down, round robin / least connections.
# The load generator runs inside the compose network (tools service); the
# faults are injected from the host (admin ports, docker compose stop).
#
# Usage: docker compose up -d --build && ./scripts/bench.sh [duration] [concurrency]
set -eu
cd "$(dirname "$0")/.."
DUR=${1:-20s}
C=${2:-16}
OUT=docs/evidence/results
ADMIN=http://127.0.0.1:8081
fault() { curl -fsS -X POST "http://127.0.0.1:910$1/admin/faults?$2" >/dev/null; }
cache() { curl -fsS -X POST "$ADMIN/proxy/cache?enabled=$1" >/dev/null; }
algo()  { curl -fsS -X POST "$ADMIN/proxy/balancer?algorithm=$1" >/dev/null; }
reset() { for i in 1 2 3; do fault "$i" reset=1; done; cache true; algo round_robin; }
wait_healthy() { # until every replica is back in rotation
  for _ in $(seq 1 60); do
    n=$(curl -fsS "$ADMIN/proxy/stats" | grep -c '"healthy": true' || true)
    [ "$n" -ge 3 ] && return 0; sleep 1
  done; echo "replicas not healthy" >&2; exit 1
}
run() { # label scenario [extra loadgen flags]
  label=$1; scenario=$2; shift 2
  docker compose run --rm -T tools go run ./src/loadgen -url http://proxy:8080 -admin http://proxy:8081 \
    -duration "$DUR" -c "$C" -ids 50 -write-ratio 0.02 -dist zipf -out "$OUT" -label "$label" -scenario "$scenario" "$@"
}

mkdir -p "$OUT"; rm -f "$OUT"/*.json "$OUT"/summary.csv "$OUT"/summary.md
reset; wait_healthy
docker compose run --rm -T tools go run ./src/loadgen -url http://proxy:8080 -admin "" -duration 1s -c 1 -seed -label warmup -out /tmp >/dev/null

run 01-healthy-cache-on  "3 replici sănătoase"
cache false
run 02-healthy-cache-off "3 replici sănătoase"

reset; fault 2 latency=200ms
run 03-slow-cache-on      "dw-2 lent (+200 ms)"
cache false
run 04-slow-cache-off     "dw-2 lent (+200 ms)"
algo least_conn
run 05-slow-cache-off-lc  "dw-2 lent (+200 ms), least_conn"

reset
docker compose stop dw-2 >/dev/null
run 06-down-cache-on      "dw-2 oprit (docker stop) la începutul rulării"
cache false
run 07-down-cache-off     "dw-2 oprit (docker stop)"
docker compose start dw-2 >/dev/null
reset; wait_healthy

docker compose stop redis >/dev/null
run 08-redis-down         "Redis oprit: proxy fără cache (degradat)"
docker compose start redis >/dev/null
sleep 3
reset
run 09-healthy-cache-on-uniform "3 replici sănătoase, 500 de angajați, acces uniform" -ids 500 -dist uniform -seed

docker compose run --rm -T tools go run ./src/loadgen -summarize "$OUT"
echo; cat "$OUT/summary.md"
