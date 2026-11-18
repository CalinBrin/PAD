# Dovezi

| Fișier | Ce conține | Produs de |
|---|---|---|
| `results/*.json` | o rulare de încărcare: latențe (p50, p90, p95, p99, max), throughput, statusuri, `X-Cache`, distribuția pe replici, contoarele proxy-ului (cereri la DW, retry, conexiuni noi/reutilizate) | `scripts/bench.sh` → `loadgen` |
| `results/summary.csv` | un rând per rulare | idem |
| `results/summary.md` | tabelul tuturor rulărilor | `loadgen -summarize` |
| `demo-*.log` | un scenariu de eșec: răspunsurile clientului și logurile tuturor componentelor, filtrate după correlation ID | `scripts/demo.sh` |

Explicația scenariilor: `docs/failure-scenarios.md`.

## Măsurătorile

Condiții: un MacBook (Apple Silicon, Docker Desktop, 8 GB pentru Docker); toate containerele pe aceeași mașină: Cassandra, Redis, 3 replici DW, proxy și generatorul de încărcare. `loadgen` rulează în rețeaua Docker (serviciul `tools`), cu 16 clienți concurenți, fiecare cu conexiunea lui keep-alive, timp de 20 s. Workload: 50 de angajați, acces Zipf (câțiva angajați „fierbinți”), 98% `GET /employees/{id}`, 2% `PUT` (care invalidează).

Eșecurile sunt injectate din afară, în timpul rulării: latență pe portul de administrare al lui dw-2, `docker compose stop dw-2`, `docker compose stop redis`.

| Rulare | Scenariu | Cache | req/s | p50 ms | p95 ms | p99 ms | Cereri la DW / cerere | Erori | Pe replici |
|---|---|---|---|---|---|---|---|---|---|
| 01 | sănătos | da | **13 232** | **0,89** | 3,36 | 4,83 | **0,147** | 0 | 12 986 / 12 986 / 12 986 |
| 02 | sănătos | nu | 10 181 | 1,48 | 2,61 | 3,53 | 1,000 | 0 | 67 881 / 67 882 / 67 882 |
| 03 | dw-2 lent (+200 ms) | da | **2 899** | 0,14 | 1,70 | 202,78 | 0,078 | 0 | 1 518 / 1 517 / 1 517 |
| 04 | dw-2 lent (+200 ms) | nu | 232 | 1,78 | 202,91 | 205,76 | 1,004 | 0 | 1 570 / 1 570 / 1 569 |
| 05 | dw-2 lent, `least_conn` | nu | **7 930** | 1,16 | 2,15 | 3,05 | 1,000 | 0 | 79 271 / **588** / 80 084 |
| 06 | dw-2 oprit în timpul rulării | da | 13 696 | 0,88 | 3,15 | 4,49 | 0,144 | **0** (9 retry) | 19 658 / 9 / 19 659 |
| 07 | dw-2 oprit | nu | 10 230 | 1,47 | 2,59 | 3,47 | 1,000 | 0 | 102 316 / 0 / 102 315 |
| 08 | Redis oprit (cache pornit, indisponibil) | — | 12 859 | 1,14 | 2,06 | 2,66 | 1,000 | 0 | 85 740 × 3 |
| 09 | sănătos, 500 de angajați, acces uniform | da | 16 801 | 0,77 | 2,13 | 4,94 | 0,045 | 0 | 5 094 × 3 |

## Ce arată

**Cache vs fără cache, replici sănătoase (01 vs 02).** Cu cache: +30% throughput, p50 de 1,7 ori mai mic. Replicile primesc doar 14,7% din cereri (87% HIT). Coada distribuției (p95/p99) e puțin mai mare cu cache: un MISS face un drum în plus la Redis, iar Redis concurează pe același CPU cu restul. Câștigul principal e **încărcarea DW și Cassandra, de 6,8 ori mai mică**.

**Replică lentă (03, 04, 05).** Cu round-robin și fără cache, o treime din cereri așteaptă 200 ms, iar cei 16 clienți ajung blocați aproape tot timpul în ele: 232 req/s. Cache-ul crește throughput-ul de 12,5 ori (2 899 req/s), fiindcă doar MISS-urile mai ajung la replica lentă. p99 rămâne însă 200 ms. **Least-connections** rezolvă problema la rădăcină chiar fără cache (7 930 req/s, p99 3 ms): replica lentă are mereu cereri în curs, așa că nu mai e aleasă (588 din 160 000). Concluzia e în ADR 0003: round-robin pentru replici omogene, `least_conn` când una e degradată.

**Replică oprită în timpul rulării (06, 07).** 0 erori la clienți. Cererile trimise la dw-2 înainte ca health check-ul să-l scoată din rotație (9) au fost reîncercate pe altă replică. Restul s-a împărțit egal pe dw-1 și dw-3.

**Redis oprit (08).** Proxy-ul servește fără cache, fără erori și fără latență în plus (p99 2,7 ms): după prima eroare, nicio cerere nu mai așteaptă Redis. Throughput-ul e mai mare decât la 02 (cache oprit, Redis pornit) din cauza registrului de clienți. Cu Redis pornit, fiecare cerere produce 2 scrieri asincrone în Redis (ADR 0004), iar pe o singură mașină asta consumă CPU comun.

**Acces uniform pe 500 de angajați (09).** Scrierile de 2% se împart pe mai multe chei, iar o scriere invalidează doar angajatul ei și listele. Ca urmare: 97% HIT, 4,5% din cereri ajung la DW.

## Ce a schimbat prima rulare

Prima rulare a benchmark-ului (aceleași scenarii, codul de dinainte de commit-ul `per-employee cache invalidation`) a găsit trei probleme. Toate au fost reparate, iar istoricul git le păstrează:

| Problemă | Măsurat atunci | După reparație |
|---|---|---|
| O scriere invalida toată colecția (o singură generație); un HIT făcea 2 apeluri Redis secvențiale | cu cache **7 306** req/s (mai lent decât fără cache: 10 132), 41% HIT | 13 232 req/s, 87% HIT |
| Round-robin dădea cota replicii oprite vecinei ei | 29 189 / 15 / 58 363 | 19 658 / 9 / 19 659 |
| Cu Redis oprit, la fiecare secundă câteva cereri așteptau timeout-ul Redis | p99 163,8 ms | p99 2,66 ms |

## Reproducere

```bash
docker compose up -d --build
./scripts/bench.sh 20s 16      # ~4 minute; rescrie results/
./scripts/demo.sh              # ~2 minute; rescrie demo-*.log
```

Cifrele absolute depind de mașină. Raporturile (cache / fără cache, round-robin / least-conn) și absența erorilor sunt cele care contează.
