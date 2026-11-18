#!/usr/bin/env bash
# Reproduces every scenario on the running system and writes the evidence
# (client answers + the logs of every component, filtered by correlation ID)
# to docs/evidence/demo-*.log.
#
# Usage: docker compose up -d --build && ./scripts/demo.sh
set -u
cd "$(dirname "$0")/.."
P=http://localhost:8080
ADMIN=http://127.0.0.1:8081
EV=docs/evidence
ID=emp-j-005
mkdir -p "$EV"

fault() { curl -fsS -X POST "http://127.0.0.1:910$1/admin/faults?$2" >/dev/null; }
reset_faults() { for i in 1 2 3; do fault "$i" reset=1; done; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
ms() { perl -MTime::HiRes=time -MPOSIX=strftime -e '$t=time; printf "%s.%03d", strftime("%H:%M:%S", gmtime($t)), ($t-int($t))*1000'; }
# show: one line per answer, the headers that matter and the salary
show() { # cid method path [body]
  local cid=$1 m=$2 path=$3 body=${4:-}
  local args=(-s -i -X "$m" -H "X-Correlation-ID: $cid" -H "From: demo-client")
  [ -n "$body" ] && args+=(-H "Content-Type: application/json" --data "$body")
  local out; out=$(curl "${args[@]}" "$P$path" ${EXTRA_CURL:-})
  local status; status=$(printf '%s' "$out" | head -1 | tr -d '\r')
  local hdr; hdr=$(printf '%s' "$out" | tr -d '\r' | grep -iE '^(x-cache|x-upstream|x-attempts|x-outcome|x-changed|age|idempotent-replayed):' | tr '\n' ' ')
  local sal; sal=$(printf '%s' "$out" | grep -oE '"(salary|code)":[^,}]*' | head -1)
  printf '%s  %-6s %-28s -> %s | %s| %s\n' "$(ms)" "$m" "$path" "$status" "$hdr" "$sal"
}
logs() { # since regex
  docker compose logs --no-color --since "$1" proxy dw-1 dw-2 dw-3 source-json source-xml 2>/dev/null |
    grep -E "$2" | sed -E 's/^([a-z0-9-]+)-1 +\| /\1 | /' | sort -t'=' -k2,2 -s
}
emp() { printf '{"id":"%s","name":"Demo Employee","department":"IT","position":"Engineer","email":"demo@example.md","salary":%s}' "$ID" "$1"; }
header() { printf '# %s\n# %s\n# generat de scripts/demo.sh la %s\n\n' "$1" "$2" "$(now)"; }

reset_faults
curl -fsS -X POST "$ADMIN/proxy/cache?enabled=true" >/dev/null

# ---------------------------------------------------------------------------
echo "1. cache: MISS, HIT, invalidare la PUT"
f=$EV/demo-cache-001.log; S=$(now)
{
  header "Cache: MISS, HIT, JSON/XML separat, invalidare per angajat la PUT" "correlation_id=demo-cache-001-*"
  echo "## Răspunsuri (client)"
  show demo-cache-001-1 PUT  /employees/$ID "$(emp 30000)"
  show demo-cache-001-2 GET  /employees/$ID
  show demo-cache-001-3 GET  /employees/$ID
  show demo-cache-001-4 GET  "/employees/$ID?format=xml"
  show demo-cache-001-5 GET  /employees/emp-j-006
  show demo-cache-001-6 PUT  /employees/$ID "$(emp 31000)"
  show demo-cache-001-7 GET  /employees/$ID
  show demo-cache-001-8 GET  /employees/$ID
  show demo-cache-001-9 GET  /employees/emp-j-006
  sleep 1
  echo; echo "## Loguri (proxy, replici)"
  logs "$S" "demo-cache-001"
} > "$f"; cat "$f" | sed -n '/Răspunsuri/,/^$/p'

# ---------------------------------------------------------------------------
echo "2. cursa GET lent / PUT (remarca 1)"
f=$EV/demo-race-001.log; S=$(now)
show demo-race-001-0 GET /employees/$ID >/dev/null
for i in 1 2 3; do fault $i read_delay=1500ms; done
{
  header "Cursa: un GET început înainte de un PUT citește valoarea veche și răspunde după invalidare" \
         "read_delay=1500ms pe toate replicile (după citire, înainte de răspuns); correlation_id=demo-race-001-*"
  echo "## Răspunsuri (client)"
  # no-cache: the slow GET must go to a replica (and then fill the cache)
  EXTRA_CURL="-H Cache-Control:no-cache" show demo-race-001-1 GET /employees/$ID "" > /tmp/race-get.txt &
  sleep 0.4
  show demo-race-001-2 PUT /employees/$ID "$(emp 32000)"
  wait; cat /tmp/race-get.txt
  echo
  echo "## Intrarea scrisă în Redis de GET-ul lent (generațiile citite ÎNAINTE de PUT)"
  docker compose exec -T redis redis-cli GET "cache:employees:GET:/employees/$ID?:json" | grep -oE '"gens":\[[^]]*\]|"stored_at":"[^"]*"'
  ga=$(docker compose exec -T redis redis-cli GET gen:employees:all | tr -d '\r'); gi=$(docker compose exec -T redis redis-cli GET gen:employees:item:$ID | tr -d '\r')
  echo "contoarele curente: [gen:employees:all, gen:employees:item:$ID] = [${ga:-0},${gi:-0}]  => intrarea nu mai e validă, nu se servește"
  for i in 1 2 3; do fault $i reset=1; done
  echo
  show demo-race-001-3 GET /employees/$ID
  show demo-race-001-4 GET /employees/$ID
  sleep 1
  echo; echo "## Loguri"
  logs "$S" "demo-race-001"
} > "$f"; sed -n '/Răspunsuri/,/## Loguri/p' "$f"

# ---------------------------------------------------------------------------
echo "3. PUT expirat, dar aplicat (remarca 1)"
f=$EV/demo-put-timeout-001.log; S=$(now)
show demo-put-timeout-001-0 GET /employees/$ID >/dev/null
for i in 1 2 3; do fault $i write_delay=3s; done
{
  header "PUT aplicat de replică, dar răspunsul întârzie peste UPSTREAM_TIMEOUT (2s)" \
         "write_delay=3s pe toate replicile (după commit, înainte de răspuns); correlation_id=demo-put-timeout-001-*"
  echo "## Răspunsuri (client)"
  show demo-put-timeout-001-1 GET /employees/$ID
  show demo-put-timeout-001-2 PUT /employees/$ID "$(emp 33000)"
  for i in 1 2 3; do fault $i reset=1; done
  show demo-put-timeout-001-3 GET /employees/$ID
  show demo-put-timeout-001-4 GET /employees/$ID
  sleep 4   # WRITE_GRACE: the delayed invalidation
  echo; echo "## Loguri"
  logs "$S" "demo-put-timeout-001"
} > "$f"; sed -n '/Răspunsuri/,/## Loguri/p' "$f"

# ---------------------------------------------------------------------------
echo "4. modificare de la client, preluată de sursă prin pull"
f=$EV/demo-pull-001.log; S=$(now)
{
  header "POST de la client (cu Idempotency-Key), apoi pull de la nodul sursă XML" "correlation_id=demo-pull-001-*"
  echo "## Răspunsuri (client)"
  EXTRA_CURL="-H Idempotency-Key:demo-pull-key-$(date +%s)" show demo-pull-001-1 POST /employees/emp-x-003 '{"salary":45000,"position":"Manager"}'
  echo "(același POST, aceeași cheie: reluat, fără efect nou)"
  KEY=demo-pull-replay-$(date +%s)
  EXTRA_CURL="-H Idempotency-Key:$KEY" show demo-pull-001-2 POST /employees/emp-x-003 '{"salary":46000}'
  EXTRA_CURL="-H Idempotency-Key:$KEY" show demo-pull-001-3 POST /employees/emp-x-003 '{"salary":46000}'
  sleep 5
  echo; echo "## Loguri (proxy, replici, source-xml)"
  logs "$S" "demo-pull-001|change_applied.*emp-x-003"
} > "$f"; sed -n '/Răspunsuri/,/## Loguri/p' "$f"

# ---------------------------------------------------------------------------
echo "5. health check activ, fără trafic (remarca 3)"
f=$EV/demo-health-001.log; S=$(now)
{
  header "Health check activ: dw-3 oprit și repornit, fără nicio cerere de la clienți" \
         "HEALTH_INTERVAL=2s, HEALTH_FALL=2, HEALTH_RISE=2"
  echo "## Acțiuni"
  echo "$(now) docker compose stop dw-3"; docker compose stop dw-3 >/dev/null 2>&1
  sleep 6
  echo "$(now) stare backend-uri: $(curl -fsS $ADMIN/proxy/stats | tr -d ' \n' | grep -oE '"name":"dw-3","url":"[^"]*","healthy":(true|false)')"
  echo "$(now) docker compose start dw-3"; docker compose start dw-3 >/dev/null 2>&1
  sleep 8
  echo "$(now) stare backend-uri: $(curl -fsS $ADMIN/proxy/stats | tr -d ' \n' | grep -oE '"name":"dw-3","url":"[^"]*","healthy":(true|false)')"
  echo; echo "## Loguri (proxy)"
  logs "$S" "backend_down|backend_up"
} > "$f"; sed -n '/Acțiuni/,/## Loguri/p' "$f"

# ---------------------------------------------------------------------------
echo "6. circuit breaker între două health check-uri"
f=$EV/demo-breaker-001.log; S=$(now)
fault 1 fail=true
{
  header "dw-1 răspunde 503 la cereri, dar /readyz e OK: detecție pasivă (breaker)" \
         "BREAKER_THRESHOLD=3, BREAKER_COOLDOWN=5s; Cache-Control: no-cache ca fiecare GET să ajungă la o replică"
  echo "## Răspunsuri (client)"
  for n in 1 2 3 4 5 6 7 8 9; do EXTRA_CURL="-H Cache-Control:no-cache" show demo-breaker-001-$n GET /employees/$ID; done
  fault 1 fail=false
  echo "$(now) dw-1 reparat; aștept cooldown-ul"; sleep 6
  for n in 10 11 12 13; do EXTRA_CURL="-H Cache-Control:no-cache" show demo-breaker-001-$n GET /employees/$ID; done
  echo; echo "## Loguri"
  logs "$S" "demo-breaker-001|breaker_"
} > "$f"; sed -n '/Răspunsuri/,/## Loguri/p' "$f"

# ---------------------------------------------------------------------------
echo "7. Redis oprit"
f=$EV/demo-redis-down-001.log; S=$(now)
show demo-redis-001-0 GET /employees/$ID >/dev/null
{
  header "Redis oprit: proxy-ul servește fără cache; invalidarea pierdută e scrisă la revenire" "correlation_id=demo-redis-001-*"
  echo "## Răspunsuri (client)"
  show demo-redis-001-1 GET /employees/$ID
  echo "$(now) docker compose stop redis"; docker compose stop redis >/dev/null 2>&1
  show demo-redis-001-2 GET /employees/$ID
  show demo-redis-001-3 PUT /employees/$ID "$(emp 34000)"
  show demo-redis-001-4 GET /employees/$ID
  echo "$(now) docker compose start redis"; docker compose start redis >/dev/null 2>&1
  sleep 4
  show demo-redis-001-5 GET /employees/$ID
  show demo-redis-001-6 GET /employees/$ID
  echo; echo "## Loguri"
  logs "$S" "demo-redis-001|redis_up"
} > "$f"; sed -n '/Răspunsuri/,/## Loguri/p' "$f"

# ---------------------------------------------------------------------------
echo "8. smart proxy: conexiuni păstrate, registrul din Redis"
f=$EV/demo-smart-proxy-001.log
{
  header "Smart proxy: 5 cereri pe o singură conexiune keep-alive; registrul clienților și al cererilor în Redis" "X-Client-ID: demo-client"
  echo "## curl: 5 URL-uri, o singură conexiune TCP (num_connects=1 doar la prima)"
  curl -s -o /dev/null -o /dev/null -o /dev/null -o /dev/null -o /dev/null -H "X-Client-ID: demo-client" -H "X-Correlation-ID: demo-smart-001" \
    -w '%{url_effective} status=%{http_code} num_connects=%{num_connects}\n' \
    "$P/employees/emp-j-001" "$P/employees/emp-j-002" "$P/employees/emp-j-003" "$P/employees/emp-x-001" "$P/employees?limit=3"
  sleep 1
  echo; echo "## GET :8081/proxy/requests/demo-smart-001 (ultima cerere cu acest id)"
  curl -fsS "$ADMIN/proxy/requests/demo-smart-001"
  echo; echo "## GET :8081/proxy/clients"
  curl -fsS "$ADMIN/proxy/clients"
  echo; echo "## GET :8081/proxy/stats (conns_new / conns_reused: pool-ul spre replici)"
  curl -fsS "$ADMIN/proxy/stats"
} > "$f"; head -12 "$f"

reset_faults
echo; echo "Dovezile sunt în $EV/demo-*.log"
