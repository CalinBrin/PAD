# Runbook

## Pornire

```bash
docker compose up --build -d     # prima pornire: ~1-2 minute (Cassandra, apoi schema)
docker compose ps                # cassandra, redis, dw-1..3, proxy: healthy; schema: exited (0)
```

Ordinea: `cassandra` (healthy) → `schema` (creează keyspace-ul, se oprește cu 0) → `dw-1..3` → `redis` + `proxy` → `source-json`, `source-xml` (trimit câte 20 de angajați).

Cassandra are nevoie de ~1 GB de memorie. Dacă Docker are mai puțin, scade `CASSANDRA_HEAP` în `.env` (minim `512M`).

## Verificări

| Ce vrei să știi | Comandă |
|---|---|
| Replicile în rotație, breaker-e, cereri per replică | `curl -s localhost:8081/proxy/stats` |
| Rata de HIT, invalidări, erori Redis | idem, secțiunea `cache` |
| Conexiuni spre DW reutilizate | idem, `upstream.conns_new` / `conns_reused` |
| Clienții și conexiunile lor | `curl -s localhost:8081/proxy/clients` |
| O cerere anume | `curl -s localhost:8081/proxy/requests/<correlation-id>` |
| Fluxul unei cereri prin toate componentele | `docker compose logs \| grep <correlation-id>` |
| Un angajat (JSON / XML) | `curl -s localhost:8080/employees/emp-j-001`, `curl -s -H 'Accept: application/xml' localhost:8080/employees/emp-x-001` |
| Starea unei replici | `curl -s localhost:9101/admin/faults` (9102, 9103) |
| Datele din Cassandra | `docker compose exec cassandra cqlsh -e "SELECT id, version, updated_by FROM pad_lab3.employees"` |
| Cheile din Redis | `docker compose exec redis redis-cli --scan --pattern 'gen:*'` |

## Situații și acțiuni

**`/proxy/stats` arată o replică `healthy: false`.** Health check-ul activ o consideră indisponibilă (`/readyz` eșuează: proces oprit sau Cassandra inaccesibilă din acea replică). Traficul merge la celelalte. Verifică `docker compose logs dw-N`. Revine singură după 2 verificări reușite.

**O replică are `breaker: open`.** Replica a răspuns cu 502/503/504 sau n-a răspuns la 3 cereri la rând, deși `/readyz` poate fi OK. După 5 s trece o cerere de probă. Verifică logurile replicii (`storage_error`, `timeout`).

**Clienții primesc `503 no_backend`.** Nicio replică în rotație. Cel mai des, Cassandra e căzută (toate `/readyz` pică): `docker compose ps cassandra`, `docker compose logs cassandra`. Ce e în cache se servește în continuare.

**Clienții primesc `504` cu `X-Outcome: unknown` la o scriere.** Replicile nu au răspuns în 2 s. Scrierea **poate fi aplicată**. Repetă un PUT (e idempotent) sau un POST cu aceeași `Idempotency-Key`, ori citește cu `Cache-Control: no-cache`.

**`cache.available: false`, `X-Cache: BYPASS`.** Redis e inaccesibil. Serviciul merge fără cache. La revenire, logul arată `redis_up`, iar invalidările pierdute se scriu automat. Verifică `docker compose ps redis`.

**Hit ratio mic.** Scrierile frecvente pe aceiași angajați îi invalidează (corect). Fiecare scriere invalidează și listele. Verifică `cache.invalidations` față de `requests`.

**Un nod sursă logează `pull failed`.** Proxy-ul sau replicile nu răspund. Sursa reîncearcă la următorul poll, cu același cursor, și nu pierde nimic.

## Injectarea eșecurilor (demo)

```bash
curl -X POST 'localhost:9102/admin/faults?latency=200ms'     # dw-2 lent
curl -X POST 'localhost:9101/admin/faults?fail=true'         # dw-1 răspunde 503 (readyz OK)
curl -X POST 'localhost:9103/admin/faults?ready=false'       # dw-3 pică /readyz
curl -X POST 'localhost:9101/admin/faults?write_delay=3s'    # scrierea se aplică, răspunsul întârzie
curl -X POST 'localhost:9101/admin/faults?read_delay=1500ms' # citirea se face, răspunsul întârzie
curl -X POST 'localhost:9101/admin/faults?reset=1'
docker compose stop dw-2 ; docker compose start dw-2
docker compose stop redis ; docker compose start redis
curl -X POST 'localhost:8081/proxy/cache?enabled=false'
curl -X POST 'localhost:8081/proxy/balancer?algorithm=least_conn'
```

## Oprire

```bash
docker compose down        # păstrează datele (volume)
docker compose down -v     # șterge și datele Cassandra, Redis, ale surselor
```
