# ADR 0003: Round-robin, health check activ și circuit breaker

Status: acceptat (lab3-v1.0)

## Context

Enunțul cere ca proxy-ul să aleagă una din replicile DW „conform unui algoritm selectat” (exemplu: Round-Robin). Proxy-ul trebuie să nu trimită cereri la o replică căzută. Remarca primită pe o soluție anterioară: „Nu există health check activ. Comentariul spune că proxy-ul face polling pe /health, dar proxy-ul nu o face; doar docker o folosește. Detecția e pasivă, prin breaker.”

## Decizie

**Algoritm:** `round_robin` implicit, `least_conn` configurabil (`PROXY_ALGORITHM`, sau la rulare `POST :8081/proxy/balancer`). Round-robin-ul se face **peste replicile disponibile acum**: când una iese din rotație, cota ei se împarte egal între celelalte.

**Disponibilă** = în rotație după health check-ul activ **și** cu breaker-ul care admite cereri.

**Health check activ** (`balancer.HealthChecker`). Proxy-ul face `GET /readyz` la fiecare replică la `HEALTH_INTERVAL` (2 s), cu `HEALTH_TIMEOUT` (1 s), în paralel. `/readyz` al replicii verifică și Cassandra. O replică iese din rotație după `HEALTH_FALL` (2) eșecuri consecutive și revine după `HEALTH_RISE` (2) succese. **Funcționează fără niciun trafic de la clienți:** `TestActiveHealthCheckWithoutTraffic` verifică explicit că replica nu primește nicio cerere de date, iar `demo-health-001.log` arată `backend_down` / `backend_up` pe sistemul real, fără cereri.

**Circuit breaker pasiv** (`balancer.Breaker`), pe fiecare replică. Reacționează între două health check-uri la erorile traficului real: `BREAKER_THRESHOLD` (3) eșecuri consecutive îl deschid; după `BREAKER_COOLDOWN` (5 s) trece o singură cerere de probă (half-open); succesul îl închide, eșecul îl redeschide. Contează ca eșec doar un semnal că **nodul** e bolnav: fără răspuns, 502, 503, 504. Un 500 (eroarea unei cereri anume) e neutru (`errors.md`).

Cele două mecanisme se completează. Health check-ul prinde o replică oprită sau fără Cassandra, chiar fără trafic. Breaker-ul prinde o replică care trece `/readyz` dar eșuează la cereri reale (`demo-breaker-001.log`).

## Alternative comparate

| Algoritm | Ce face | Măsurat cu dw-2 lent (+200 ms), fără cache, 16 clienți | Când e potrivit |
|---|---|---|---|
| **Round-robin** (implicit) | pe rând | **232 req/s**, p95 203 ms: o treime din cereri așteaptă replica lentă | replici omogene; simplu și previzibil; distribuție verificabilă (exact 1/3 fiecare) |
| **Least connections** | replica cu cele mai puține cereri în curs | **7930 req/s**, p95 2 ms: dw-2 primește 588 din 160 000 | replici eterogene sau una degradată |
| Random | aleator | ~ca round-robin, dar distribuție mai puțin uniformă | foarte mulți clienți, fără stare |
| Weighted RR | proporțional cu o pondere fixă | ponderea nu se adaptează la o replică lentă | capacități cunoscute dinainte |
| Consistent hashing pe `id` | același angajat → aceeași replică | nu se aplică: replicile nu au cache local, datele sunt în Cassandra | replici cu cache local |

Round-robin rămâne implicit pentru că e cel cerut în enunț și dă o distribuție exact verificabilă (testele și benchmark-ul arată 1/3 fiecare). Least-connections e alternativa corectă când o replică e degradată. Benchmark-ul arată diferența de 34× (`docs/evidence/results`).

| Detecție | Avantaj | Dezavantaj |
|---|---|---|
| doar pasivă (breaker) | fără cereri suplimentare | o replică fără trafic nu e testată; prima cerere după cădere eșuează |
| doar activă (health check) | detectează fără trafic | între două verificări (până la ~4 s) cererile încă ajung la replica bolnavă |
| **ambele** (ales) | | o cerere `/readyz` la 2 s per replică |

## Consecințe

- O replică oprită iese din rotație în cel mult `HEALTH_FALL × HEALTH_INTERVAL` ≈ 4 s. Între timp, cererile idempotente trimise la ea sunt reîncercate pe altă replică: în benchmark-ul cu dw-2 oprit, 9 cereri au nimerit dw-2 și au fost reîncercate, cu 0 erori la clienți.
- `healthcheck` din Docker Compose (`wget /readyz`) există separat. Docker îl folosește doar pentru ordinea pornirii; rotația o decide proxy-ul.
