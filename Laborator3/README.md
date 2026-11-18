# PAD Lab 3: Web proxy (transparența în distribuire)

Un nod informativ **data warehouse (DW)** care servește angajați prin HTTP, în **JSON sau XML** la alegerea clientului, cu procesare concurentă a cererilor (etapa 1). În fața lui stă un **reverse proxy** prin care trece orice client (etapa 2):

- **smart proxy**: păstrează conexiunile clienților (keep-alive, limită per client) și ține în Redis registrul conexiunilor și al fiecărei cereri;
- **caching**: în Redis, cu invalidare corectă prin contoare de generație;
- **load balancing**: round-robin (sau least-connections) pe 3 replici DW, cu health check activ și circuit breaker.

Datele sunt într-o singură bază **Cassandra**, comună replicilor, iar cache-ul și registrul clienților în **Redis** (recomandările din enunț). Scris în Go, rulat cu Docker Compose.

Versiunea evaluată: tag-ul git **`lab3-v1.0`**.

## Arhitectura

```mermaid
flowchart LR
    SJ[source-json] & SX[source-xml] & C[clienți] -->|HTTP :8080| P[proxy]
    P <-->|cache, registru| R[(Redis)]
    P -->|round-robin, pool keep-alive| D1[dw-1] & D2[dw-2] & D3[dw-3]
    P -.->|GET /readyz la 2 s| D1 & D2 & D3
    D1 & D2 & D3 --> CS[(Cassandra)]
```

Detalii: `docs/diagrams/architecture.md`; diagrame de secvență (MISS/HIT, cursa GET/PUT, PUT expirat, replică oprită, pull): `docs/diagrams/sequence.md`.

## Structura

```
contracts/      http-api.md, errors.md, versioning.md, employee.schema.json, employee.xsd
src/
  common/       logx (loguri, correlation ID, config), model (resursele), httpx (JSON/XML, negociere, client)
  datawarehouse/  dw/ (controller.go = Controller, service.go = Service, faults.go), store/ (Cassandra, Memory)
  proxy/        proxy/ (proxy, registry = smart proxy, admin), balancer/ (pool, health, breaker), cache/ (Redis)
  source-node/  nodul sursă: PUT + pull cu cursor
  client/       CLI
  loadgen/      generator de încărcare, rezultate în fișiere
tests/          unit, contract, integration, failure (+ testutil: pornește replici, Redis și proxy reale)
deployment/     Dockerfile (toate serviciile, testele și uneltele)
docs/           adr/ (0001-0006), diagrams/, runbook/, failure-scenarios.md, guarantees-and-limits.md,
                evidence/ (demo-*.log, results/*.json, summary.csv, summary.md)
scripts/        demo.sh (scenariile de eșec), bench.sh (măsurătorile)
docker-compose.yml, .env.example
```

## Rulare

Necesită doar Docker (Docker Desktop pe Windows/macOS). Nu se instalează Cassandra sau Redis: rulează în containere.

```bash
docker compose up --build -d      # prima pornire ~1-2 minute (Cassandra)
docker compose ps                 # toate healthy; "schema" exited (0)
```

Câteva cereri:

```bash
curl -i localhost:8080/employees/emp-j-001                          # JSON; X-Cache: MISS
curl -i localhost:8080/employees/emp-j-001                          # X-Cache: HIT
curl -H 'Accept: application/xml' localhost:8080/employees/emp-x-002   # XML
curl 'localhost:8080/employees/?offset=1&limit=4'                   # pagina, ca în enunț
curl 'localhost:8080/employee/?id=emp-j-003'                        # alias din enunț
curl -X POST localhost:8080/employees/emp-x-003 -H 'Idempotency-Key: k1' \
     -H 'Content-Type: application/json' -d '{"salary":45000}'     # modificare de la client
docker compose logs source-xml | grep change_applied                # sursa a preluat-o (pull)
curl -s localhost:8081/proxy/stats                                  # cache, replici, conexiuni
curl -s localhost:8081/proxy/clients                                # registrul din Redis
```

Clientul CLI (în rețeaua Docker): `docker compose run --rm tools go run ./src/client -url http://proxy:8080 -format xml get emp-x-001`.

PowerShell: aceleași comenzi, cu `curl.exe` în loc de `curl`.

### Teste

```bash
docker compose run --rm tests     # toate testele, cu -race, plus testele pe Cassandra reală
go test -race ./...               # local (cu Go 1.24+); testele Cassandra se sar
```

### Dovezile

```bash
./scripts/demo.sh                 # scenariile de eșec → docs/evidence/demo-*.log   (~2 min)
./scripts/bench.sh 20s 16         # măsurătorile      → docs/evidence/results/       (~4 min)
```

Scripturile sunt bash (macOS, Linux, WSL, Git Bash). Oprire: `docker compose down -v`.

## Cerințele laboratorului

| Cerință | Realizare | Unde |
|---|---|---|
| DW comunică concurent prin HTTP | `net/http`: o goroutine per cerere (thread-per-request), `DW_MAX_INFLIGHT` | `src/datawarehouse`, `guarantees-and-limits.md` |
| GET date, PUT de la nodurile de stocare, POST modificări, pull `GET update/employees` | toate, inclusiv rutele exacte din enunț (`/employee/?id=`, `/employees/?offset=&limit=`, `/update/employees/`) | `contracts/http-api.md` |
| Clientul alege XML / JSON | `Accept` (cu `q`) sau `?format=`; și la cereri (`Content-Type`); `406`/`415` | `httpx.Negotiate`, `TestNegotiate`, `TestContract` |
| Colecții thread-safe / acces exclusiv | compare-and-set în Cassandra (LWT), `RWMutex` în stocarea de test, `atomic`/`Mutex` în proxy; testele cu `-race` | ADR 0001, `TestConcurrent*` |
| Controller | `dw/controller.go` (rute → Service → Data) | schema din enunț |
| Toate cererile prin proxy | porturile DW nu sunt publicate; sursele și clienții folosesc `:8080` | `docker-compose.yml` |
| Smart proxy (conexiunile clienților) | keep-alive gestionat, limită per client, registrul conexiunilor și cererilor în Redis, pool spre DW | ADR 0004, `demo-smart-proxy-001.log` |
| Caching | Redis, invalidare per angajat prin contoare de generație, `ETag`/`304`, `Cache-Control` | ADR 0002, `demo-race-001.log` |
| Load balancing | round-robin / least-conn peste replicile disponibile; health check activ + breaker | ADR 0003, `demo-health-001.log` |
| Recomandare: Cassandra | o bază comună, fără SyncService | ADR 0001 |
| Recomandare: Redis | cache + registrul conexiunilor | ADR 0002, 0004 |
| Opțional: PUSH | nu: pull cu cursor, argumentat | ADR 0005 |

## Cum sunt tratate remarcile primite pe o soluție anterioară

| Remarca | Ce s-a făcut | Dovada |
|---|---|---|
| Un GET început înainte de un PUT pune valoarea veche în cache; fără contor de generație; fără test | contoare de generație citite **înainte** de cererea la DW și salvate în intrare; intrarea veche nu se mai potrivește | `TestSlowGetStartedBeforePut_DoesNotCacheStaleValue`, `docs/evidence/demo-race-001.log` |
| Un PUT care expiră, dar a fost aplicat, nu invalidează | invalidare după scriere oricare ar fi rezultatul, plus una întârziată (`WRITE_GRACE`) la rezultat necunoscut | `TestPutTimesOutButApplied_*`, `TestWriteCommittedAfterProxyGaveUp_*`, `demo-put-timeout-001.log` |
| Un GET reîncercat care închiriază (are efecte) | niciun GET nu are efecte; pull-ul folosește cursor ținut de cititor, fără lease; retry doar pentru operații idempotente | ADR 0005, ADR 0006, `TestChangeFeedRetryHasNoSideEffect` |
| Fără health check activ; documentația spune altceva decât codul | `HealthChecker` interoghează `/readyz` la 2 s, plus breaker pasiv; funcționează fără trafic | `TestActiveHealthCheckWithoutTraffic`, `demo-health-001.log` |
| Fără ADR, fără tag, un singur commit | 6 ADR-uri cu alternative comparate; commit-uri pe etape; tag `lab3-v1.0` | `docs/adr/`, `git log` |
| Dovezi doar într-un tabel, fără loguri și fișiere, fără comparație cu / fără cache | loguri reale per scenariu, fișiere JSON/CSV per rulare, comparație cu / fără cache × sănătos / lent / oprit / Redis oprit | `docs/evidence/` |
| „Smart proxy” neargumentat; fără Redis; SQLite valabil doar pe același host | ADR 0004 compară interpretările; Redis pentru cache și registru; Cassandra prin rețea | ADR 0001, ADR 0004 |

## Rezultatul măsurătorilor (pe scurt)

| Scenariu | Fără cache | Cu cache |
|---|---|---|
| 3 replici sănătoase | 10 181 req/s, p50 1,48 ms | **13 232 req/s, p50 0,89 ms**, doar 14,7% din cereri ajung la DW |
| dw-2 lent (+200 ms), round-robin | 232 req/s | 2 899 req/s |
| dw-2 lent, least-connections | **7 930 req/s** | |
| dw-2 oprit în timpul rulării | 0 erori | 0 erori (9 retry) |
| Redis oprit | | 12 859 req/s, p99 2,7 ms, 0 erori |

Analiza completă: `docs/evidence/README.md`.

## Livrabile

| Livrabil | Unde |
|---|---|
| contracte | `contracts/` |
| teste automate | `tests/` (`docker compose run --rm tests`) |
| diagrame | `docs/diagrams/` |
| ADR-uri | `docs/adr/0001` – `0006` |
| dovezile scenariilor de eșec | `docs/failure-scenarios.md`, `docs/evidence/demo-*.log` |
| comparația cu / fără cache | `docs/evidence/README.md`, `docs/evidence/results/` |
| instrucțiuni de reproducere | acest README, `docs/runbook/runbook.md`, `scripts/` |
| garanții și limite | `docs/guarantees-and-limits.md` |

## Teste

```
tests/unit         negociere JSON/XML, encode/decode, validare, patch idempotent, versiuni concurente,
                   PUT no-op și If-Match, anulare înainte de commit, fereastra feed-ului, breaker,
                   round-robin (și fără o replică), least-conn, health check fall/rise
tests/contract     API-ul pe o replică și prin proxy: statusuri, câmpuri JSON (golden), XML, erori,
                   ETag/304, If-Match/412, Idempotency-Key, aliasurile din enunț, feed, antetele proxy-ului
tests/integration  distribuție round-robin, MISS/HIT, JSON/XML separat, invalidare per angajat și liste,
                   no-cache, feed necache-uit, surse JSON/XML + pull, pool spre DW, registrul clienților,
                   limita de conexiuni, least-conn cu replică lentă, scrieri concurente; Cassandra reală
tests/failure      cursa GET/PUT, PUT expirat dar aplicat, scriere confirmată după abandon, Redis oprit,
                   retry GET, POST cu/fără cheie, retry PUT no-op, retry feed, health check fără trafic,
                   replică oprită, breaker, 500 nu scoate replici, toate replicile căzute, cache cu replici căzute
```
