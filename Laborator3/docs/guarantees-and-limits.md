# Garanții și limite

## Ce garantează sistemul

| Proprietate | Garanție | Mecanism | Dovadă |
|---|---|---|---|
| Date comune replicilor | orice replică vede ultima scriere confirmată | o singură bază Cassandra, `QUORUM` | ADR 0001 |
| Scrieri concurente pe același angajat | nicio modificare pierdută | compare-and-set `IF version = ?` (Paxos), reîncercat la conflict | `TestCassandraConcurrentReplicasLoseNoUpdate`, `TestConcurrentWritesThroughDifferentReplicasLoseNothing` |
| PUT repetat | aceeași stare, aceeași versiune | conținut identic ⇒ fără scriere | `TestPutRetryAfterLostAnswerIsNoOp`, `demo-put-timeout-001.log` |
| POST repetat cu aceeași `Idempotency-Key` | un singur efect, același răspuns (24 h) | tabela `idempotency` + patch cu valori absolute | `TestContract`, `demo-pull-001.log` |
| Scriere abandonată înainte de commit | fără efect | DW verifică `ctx` înainte de compare-and-set | `TestCancelledWriteIsNotApplied` |
| Cache după o scriere prin proxy | valoarea veche nu se servește după încheierea scrierii | contoare de generație citite înainte de cererea la DW | `TestSlowGetStartedBeforePut_*`, `demo-race-001.log` |
| Cache după o scriere cu rezultat necunoscut | valoarea veche servită cel mult `WRITE_GRACE` (3 s) | invalidare după scriere (oricare ar fi rezultatul) + invalidare întârziată | `TestPutTimesOutButApplied_*`, `TestWriteCommittedAfterProxyGaveUp_*` |
| Redis indisponibil | serviciul merge, fără latență în plus; nicio valoare veche la revenire | fail fast + invalidări reținute | `TestRedisDown_*`, benchmark 08 |
| Replică oprită | iese din rotație în ~4 s, chiar fără trafic; cererile idempotente sunt reîncercate pe alta | health check activ + retry | `TestActiveHealthCheck*`, benchmark 06 (0 erori) |
| Replică care eșuează doar la cereri | scoasă după 3 eșecuri, reintrodusă după probă | circuit breaker | `TestBreakerOpensBetweenHealthChecks`, `demo-breaker-001.log` |
| O cerere stricată (500) | nu scoate replicile din rotație | 500 e neutru pentru breaker, nu e reîncercat | `TestRequestErrorDoesNotTakeReplicasOutOfRotation` |
| Feed de modificări | nicio modificare pierdută la retry sau la răspuns pierdut; aplicată o singură dată | cursor ținut de cititor, fereastră de suprapunere, versiuni | `TestChangeFeed*`, `TestSourceNodesPublishAndPullClientModification` |
| Concurență în DW | thread-safe | goroutine per cerere, semafor `DW_MAX_INFLIGHT`, stocare cu `RWMutex` (memorie) sau LWT (Cassandra); toate testele cu `-race` | `TestConcurrentWritesGetConsecutiveVersions` |

## Deadline-uri, retry, idempotență pe fiecare apel

| Apel | Deadline | Încercări | Pauză | Idempotent după | Retry pe |
|---|---|---|---|---|---|
| client → proxy | `REQUEST_TIMEOUT` 6 s (tot) | — | — | — | — |
| proxy → replică | `UPSTREAM_TIMEOUT` 2 s / încercare | `MAX_ATTEMPTS` 3, pe replici diferite | full jitter 50 ms × 2ⁿ | metodă (GET/PUT) sau `Idempotency-Key` | fără răspuns, 502, 503, 504 |
| proxy → Redis | `REDIS_TIMEOUT` 200 ms | 1; apoi fail fast, probă la 500 ms | — | INCR: invalidare în plus = inofensivă | — |
| proxy → `/readyz` | `HEALTH_TIMEOUT` 1 s | la `HEALTH_INTERVAL` 2 s | — | citire | — |
| replică → Cassandra | `DW_STORE_TIMEOUT` 2 s | 1 retry al driverului; ≤ 8 runde CAS la conflict | — | `IF version = ?` | conflict de versiune |
| sursă → proxy (PUT) | `SOURCE_TIMEOUT` 8 s | 6 | 400 ms × 2ⁿ | PUT | eroare, 5xx |
| sursă → proxy (feed) | idem | la fiecare poll (2 s) | — | citire | — |

`WRITE_GRACE` (3 s) > `DW_STORE_TIMEOUT` (2 s): după invalidarea întârziată, o scriere care era în drum spre Cassandra s-a terminat sau a eșuat.

## Concurența (thread-per-request)

`net/http` servește fiecare conexiune în propria goroutine. E echivalentul Go al modelului thread-per-request din enunț, cu fire ieftine (câțiva KB de stivă), deci zeci de mii de cereri concurente sunt posibile. `DW_MAX_INFLIGHT` (256) limitează câte cereri lucrează simultan cu Cassandra; celelalte așteaptă un loc până la deadline-ul lor. Starea comună e:

- în DW: doar stocarea (Cassandra cu LWT, sau `Memory` cu `sync.RWMutex` în teste);
- în proxy: contoare `atomic`, starea breaker-elor (`sync.Mutex`), registrul clienților (`sync.Mutex` + canal), indexul round-robin (`atomic`).

Toate testele rulează cu detectorul de data race (`go test -race`).

## Limite cunoscute

| Limită | Efect | Ce ar trebui pentru a o elimina |
|---|---|---|
| Un singur nod Cassandra (RF=1) în Docker | punct unic de cădere pentru date (cache-ul continuă să servească) | 3 noduri, `CASSANDRA_RF=3` |
| Un singur proxy și un singur Redis | punct unic de cădere pentru intrare | mai multe replici de proxy (cache-ul și registrul sunt deja comune, în Redis) în spatele unui DNS/L4 balancer; Redis Sentinel |
| Scrieri care ocolesc proxy-ul | valoare veche în cache până la TTL (30 s) | porturile DW nepublicate (făcut); sau invalidare publicată de DW |
| Proxy-ul cade între commit și invalidarea de după | valoare veche până la TTL | invalidare publicată de DW după commit |
| Feed: ceasul replicilor diferă cu mai mult de `DW_FEED_OVERLAP` | o modificare poate fi sărită | secvență globală atomică (ADR 0005) |
| Paginare cu `offset` și `COUNT(*)` în Cassandra | cost liniar în `offset` și în numărul de rânduri | paginare cu token opac |
| Fără TLS și fără autentificare | trafic în clar, oricine din rețea poate scrie | TLS la proxy, token pentru noduri sursă |
| `X-Client-ID` e declarat de client | registrul poate fi păcălit | autentificare |
