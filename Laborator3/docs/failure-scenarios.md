# Scenariile de eșec și dovezile lor

Fiecare scenariu are:
- un **test automat** (`go test -race ./...`, cu replici HTTP reale, Redis real prin protocol (miniredis) și stocarea comună în memorie);
- un **log real** de pe sistemul pornit cu Docker Compose (Cassandra, Redis, 3 replici, proxy), produs de `scripts/demo.sh` și filtrat după correlation ID: `docs/evidence/demo-*.log`. Fișierele conțin răspunsurile clientului (status, `X-Cache`, `X-Upstream`, `X-Attempts`, salariul) și logurile tuturor componentelor.

Comparația cantitativă (cu cache / fără cache × sănătos / replică lentă / replică oprită / Redis oprit) e în `docs/evidence/README.md`, cu fișierele de rezultat în `docs/evidence/results/`.

Extrasele de mai jos păstrează doar câmpurile relevante (ora UTC, componenta, evenimentul).

## 1. Un GET lent, început înainte de un PUT, nu pune valoarea veche în cache

| | |
|---|---|
| Stare inițială | `emp-j-005` cu salariul 31000, în cache |
| Injectarea eșecului | `read_delay=1500ms` pe toate replicile: replica citește valoarea, apoi așteaptă 1,5 s înainte să răspundă. GET-ul trimis cu `Cache-Control: no-cache` ajunge la replică și apoi scrie în cache |
| Desfășurare | GET pornește (citește 31000) → după 0,4 s un PUT cu 32000 se termină și invalidează → GET-ul răspunde cu 31000 și **scrie** în Redis |
| Comportament observat | intrarea din Redis are contoarele citite înainte de PUT, `[0,14]`; contoarele curente sunt `[0,16]` |
| Recuperare | nu e nevoie: intrarea nu se potrivește, următorul GET e `MISS` și aduce 32000 |
| Garanția | o valoare calculată înaintea unei scrieri nu e servită după acea scriere, indiferent când ajunge în cache |
| Nu acoperă | o scriere făcută ocolind proxy-ul |

Test: `TestSlowGetStartedBeforePut_DoesNotCacheStaleValue`. Log: `docs/evidence/demo-race-001.log`.

```
client 20:56:50.2  GET (no-cache) pornește                         -> ajunge la dw-3, citește 31000
proxy  20:56:50.601 cache invalidated  keys=[gen:employees:item:emp-j-005 gen:employees:lists] reason=before_write  demo-race-001-2
dw-1   20:56:50.606 employee written   method=PUT version=9 changed=true                                         demo-race-001-2
proxy  20:56:50.607 cache invalidated  reason=after_write                                                          demo-race-001-2
proxy  20:56:51.682 proxied GET status=200 cache=MISS backend=dw-3 duration_ms=1502  (salary 31000, scris în Redis cu gens [0,14])  demo-race-001-1
proxy  20:56:51.965 proxied GET status=200 cache=MISS backend=dw-1  (salary 32000)                                 demo-race-001-3
proxy  20:56:51.992 proxied GET status=200 cache=HIT                (salary 32000)                                 demo-race-001-4
```

## 2. Un PUT care expiră, dar a fost aplicat, invalidează

| | |
|---|---|
| Stare inițială | `emp-j-005` = 32000, în cache (`HIT`) |
| Injectarea eșecului | `write_delay=3s` pe toate replicile: scrierea e confirmată în Cassandra, răspunsul vine după 3 s; `UPSTREAM_TIMEOUT` = 2 s |
| Comportament observat | prima încercare (dw-2) **aplică** scrierea (`version=10 changed=true`) și expiră; retry pe dw-1 și dw-3: `changed=false` (PUT idempotent); clientul primește `504`, `X-Outcome: unknown` |
| Recuperare | invalidare înainte, **după** (oricare ar fi rezultatul) și încă o dată după `WRITE_GRACE` = 3 s (rezultat necunoscut) |
| Stare finală | GET imediat după: `MISS`, 33000; o singură versiune nouă |
| Garanția | o scriere cu rezultat necunoscut nu lasă în cache valoarea dinainte; retry-ul unui PUT nu creează versiuni în plus |

Teste: `TestPutTimesOutButApplied_CacheStillInvalidated`, `TestWriteCommittedAfterProxyGaveUp_DelayedInvalidationBoundsStaleness` (scrierea ajunge în bază **după** ce proxy-ul a renunțat și a invalidat; valoarea veche e servită cel mult `WRITE_GRACE`), `TestPutRetryAfterLostAnswerIsNoOp`. Log: `docs/evidence/demo-put-timeout-001.log`.

```
proxy 20:56:53.253 cache invalidated  reason=before_write
dw-2  20:56:53.258 employee written   method=PUT version=10 changed=true
proxy 20:56:55.258 upstream attempt failed backend=dw-2 attempt=1 error="context deadline exceeded" will_retry=true
dw-1  20:56:55.271 employee written   method=PUT version=10 changed=false
proxy 20:56:57.270 upstream attempt failed backend=dw-1 attempt=2 ... will_retry=true
dw-3  20:56:57.334 employee written   method=PUT version=10 changed=false
proxy 20:56:59.256 upstream attempt failed backend=dw-3 attempt=3 ... will_retry=false
proxy 20:56:59.257 cache invalidated  reason=after_write
proxy 20:56:59.257 proxied PUT status=504 attempts=3 duration_ms=6003
proxy 20:56:59.324 proxied GET status=200 cache=MISS backend=dw-2      (salary 33000)
proxy 20:57:02.261 cache invalidated  reason=delayed_after_ambiguous_write
```

## 3. Retry-ul unui GET nu are efecte (feed-ul de modificări)

| | |
|---|---|
| Injectarea eșecului | o replică răspunde la feed după 400 ms, peste `UPSTREAM_TIMEOUT` din test (150 ms) |
| Comportament observat | proxy-ul reîncearcă pe altă replică (`X-Attempts: 2`) și primește aceleași 3 modificări |
| Garanția | feed-ul e o citire pură cu cursor ținut de cititor: nicio închiriere, nicio încercare consumată, nicio DLQ. Un răspuns pierdut se recere la următorul poll |

Test: `TestChangeFeedRetryHasNoSideEffect`. Decizia: ADR 0005.

## 4. Modificarea unui client ajunge la nodul sursă (pull)

| | |
|---|---|
| Acțiune | `POST /employees/emp-x-003 {"salary":46000}` cu `Idempotency-Key`, apoi același POST cu aceeași cheie |
| Comportament observat | primul: `version=5 changed=true`; al doilea: `changed=false replayed=true`, `Idempotent-Replayed: true`; `source-xml` aplică versiunea 5 la următorul poll |
| Garanția | o modificare apare o singură dată, oricâte repetări; sursa aplică doar versiuni mai mari decât cea locală |

Test: `TestSourceNodesPublishAndPullClientModification`. Log: `docs/evidence/demo-pull-001.log`.

```
dw-1       20:57:03.596 employee written  method=POST id=emp-x-003 version=5 changed=true                  demo-pull-001-2
dw-2       20:57:03.627 employee written  method=POST id=emp-x-003 version=5 changed=false replayed=true   demo-pull-001-3
source-xml 20:57:05.514 modification pulled from the warehouse  id=emp-x-003 version=5 updated_by=demo-client salary=46000
```

## 5. O replică oprită, fără trafic: health check activ

| | |
|---|---|
| Injectarea eșecului | `docker compose stop dw-3`, **fără nicio cerere de la clienți** |
| Comportament observat | după 2 verificări eșuate (~4 s), `backend_down`; `/proxy/stats` arată `healthy: false` |
| Recuperare | `docker compose start dw-3` → după 2 verificări reușite, `backend_up` |
| Garanția | o replică indisponibilă iese din rotație chiar dacă nimeni nu-i trimite cereri |

Teste: `TestActiveHealthCheckWithoutTraffic` (verifică explicit că replica nu primește cereri de date), `TestActiveHealthCheckDetectsStoppedReplica`, `TestHealthCheckerFallAndRise`. Log: `docs/evidence/demo-health-001.log`.

```
proxy 20:55:55.321 backend removed from rotation (active health check) backend=dw-3 consecutive_failures=2 error="... no such host"
proxy 20:56:01.319 backend back in rotation (active health check)     backend=dw-3 consecutive_successes=2
```

## 6. O replică trece `/readyz`, dar eșuează la cereri: circuit breaker

| | |
|---|---|
| Injectarea eșecului | `fail=true` pe dw-1: cererile de date primesc `503`, `/readyz` rămâne OK |
| Comportament observat | cererile trimise la dw-1 sunt reîncercate pe altă replică (`X-Attempts: 2`, clientul primește `200`); după 3 eșecuri, `breaker_open`, iar cererile nu mai ajung la dw-1 (`X-Attempts: 1`) |
| Recuperare | după `BREAKER_COOLDOWN` (5 s), o cerere de probă reușește: `breaker_closed` |

Teste: `TestBreakerOpensBetweenHealthChecks`, `TestBreakerTransitions`, `TestRequestErrorDoesNotTakeReplicasOutOfRotation` (un `500` nu deschide breaker-ul). Log: `docs/evidence/demo-breaker-001.log`.

## 7. Redis oprit

| | |
|---|---|
| Injectarea eșecului | `docker compose stop redis`, apoi un PUT |
| Comportament observat | GET-urile trec direct la replici (`X-Cache: BYPASS`); PUT-ul reușește, invalidarea nu se poate scrie (`invalidation_failed`) și e reținută |
| Recuperare | `docker compose start redis` → `redis_up`; invalidările reținute se scriu **înainte** de prima folosire a cache-ului; GET: `MISS` cu valoarea nouă |
| Garanția | Redis căzut nu oprește serviciul și nu-l încetinește: în benchmark, p99 = 2,7 ms; nicio valoare veche după revenire |
| Nu acoperă | mai multe replici de proxy, dintre care doar una e izolată de Redis (ADR 0002) |

Test: `TestRedisDown_ProxyServesWithoutCache_AndLostInvalidationIsReplayed`. Log: `docs/evidence/demo-redis-down-001.log`.

## 8. Toate replicile indisponibile

| | |
|---|---|
| Injectarea eșecului | baza de date căzută (toate `/readyz` pică) sau toate replicile oprite |
| Comportament observat | `503 no_backend`; ce era în cache se servește în continuare (`HIT`) |

Teste: `TestAllReplicasDownGives503`, `TestCachedAnswersSurviveWhenEveryReplicaIsDown`.

## 9. Scrieri concurente pe replici diferite

| | |
|---|---|
| Acțiune | 30 de POST-uri concurente pe același angajat, prin proxy, împărțite round-robin pe 3 replici; 20 de patch-uri concurente prin 2 servicii pe Cassandra reală |
| Garanția | `version` = 1 + numărul de modificări reale: nicio modificare pierdută (compare-and-set: `IF version = ?`) |

Teste: `TestConcurrentWritesThroughDifferentReplicasLoseNothing`, `TestConcurrentWritesGetConsecutiveVersions`, `TestCassandraConcurrentReplicasLoseNoUpdate`.
