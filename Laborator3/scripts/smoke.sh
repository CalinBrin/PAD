#!/usr/bin/env bash
# Quick check that the running system works (about 20 seconds).
# Usage: docker compose up --build -d && ./scripts/smoke.sh
set -u
cd "$(dirname "$0")/.."
P=${PROXY:-http://localhost:8080}
ADMIN=${ADMIN:-http://127.0.0.1:8081}
ok=0; total=0
green=$'\e[32m'; red=$'\e[31m'; off=$'\e[0m'
check() { # name condition-exit-code detail
  total=$((total+1))
  if [ "$2" -eq 0 ]; then ok=$((ok+1)); echo "  ${green}[OK]${off}   $1  ${3:-}"; else echo "  ${red}[FAIL]${off} $1  ${3:-}"; fi
}
hdr() { printf '%s' "$1" | tr -d '\r' | grep -i "^$2:" | head -1 | cut -d' ' -f2-; }
get() { curl -s -i "$@"; }

echo "1. Proxy and warehouse replicas"
healthy=0
for _ in $(seq 1 30); do
  healthy=$(curl -s "$ADMIN/proxy/stats" | grep -c '"healthy": true'); [ "$healthy" -ge 3 ] && break; sleep 2
done
[ "$healthy" -ge 3 ]; check "proxy answers, 3 replicas in rotation" $? "healthy=$healthy"

echo "2. GET through the proxy: MISS, then HIT (cache in Redis)"
r1=$(get -H 'Cache-Control: no-cache' "$P/employees/emp-j-001"); r2=$(get "$P/employees/emp-j-001")
[ "$(hdr "$r1" X-Cache)" = MISS ]; check "first GET reaches a replica" $? "X-Cache=$(hdr "$r1" X-Cache) X-Upstream=$(hdr "$r1" X-Upstream)"
[ "$(hdr "$r2" X-Cache)" = HIT ]; check "second GET served from cache" $? "X-Cache=$(hdr "$r2" X-Cache) Age=$(hdr "$r2" Age)"

echo "3. JSON or XML, chosen by the client"
curl -s -H 'Accept: application/xml' "$P/employees/emp-x-002" | grep -q '^<?xml'; check "Accept: application/xml gives XML" $?
curl -s -H 'Accept: application/json' "$P/employees/emp-x-002" | grep -q '^{'; check "Accept: application/json gives JSON" $?

echo "4. Routes of the statement"
l=$(curl -s "$P/employees/?offset=1&limit=4")
printf '%s' "$l" | grep -q '"offset":1,"limit":4'; check "GET /employees/?offset=1&limit=4" $? "$(printf '%s' "$l" | grep -oE '"total":[0-9]+')"
curl -s "$P/employee/?id=emp-j-003" | grep -q '"id":"emp-j-003"'; check "GET /employee/?id=emp-j-003" $?

echo "5. Round robin over the 3 replicas"
seen=$(for _ in 1 2 3 4 5 6; do hdr "$(get -H 'Cache-Control: no-cache' "$P/employees/emp-j-002")" X-Upstream; echo; done | sort -u | tr '\n' ' ')
[ "$(echo $seen | wc -w | tr -d ' ')" -eq 3 ]; check "requests spread over dw-1, dw-2, dw-3" $? "$seen"

echo "6. Client modification (POST, Idempotency-Key) pulled by the XML source node"
salary=$((20000 + RANDOM % 70000)); key="smoke-$RANDOM$RANDOM"; since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
post() { get -X POST -H "Idempotency-Key: $key" -H 'From: smoke-test' -H 'Content-Type: application/json' --data "{\"salary\":$salary}" "$P/employees/emp-x-003"; }
p1=$(post); p2=$(post)
[ "$(hdr "$p1" X-Changed)" = true ]; check "POST applied" $? "salary=$salary"
[ "$(hdr "$p2" Idempotent-Replayed)" = true ] && [ "$(hdr "$p2" X-Changed)" = false ]; check "same POST again: replayed, no new effect" $?
g=$(get "$P/employees/emp-x-003")
printf '%s' "$g" | grep -q "\"salary\":$salary"; check "GET after POST sees the new value (cache invalidated)" $? "X-Cache=$(hdr "$g" X-Cache)"
pulled=1
for _ in 1 2 3 4 5 6 7 8; do
  sleep 1
  docker compose logs --no-color --since "$since" source-xml 2>/dev/null | grep -q "change_applied id=emp-x-003 .*salary=$salary" && { pulled=0; break; }
done
check "source-xml pulled the modification" $pulled

echo "7. Smart proxy"
s=$(curl -s "$ADMIN/proxy/stats")
new=$(printf '%s' "$s" | grep -oE '"conns_new": [0-9]+' | grep -oE '[0-9]+'); reused=$(printf '%s' "$s" | grep -oE '"conns_reused": [0-9]+' | grep -oE '[0-9]+')
[ "$reused" -gt "$new" ]; check "connections to the replicas are reused" $? "new=$new reused=$reused"
curl -s "$ADMIN/proxy/clients" | grep -q '"source": "redis"'; check "client registry kept in Redis" $?

echo
if [ "$ok" -eq "$total" ]; then echo "${green}All $total checks passed.${off}"; exit 0; fi
echo "${red}$ok of $total checks passed.${off}"; exit 1
