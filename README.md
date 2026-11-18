# PAD
Programarea Aplicațiilor Distribuite

Laboratoarele la PAD. Toate sunt scrise în Go, se pornesc cu Docker Compose și au aceeași structură: `src/`, `contracts/`, `tests/` (unit, contract, integration, failure), `docs/` (ADR-uri, diagrame, runbook, dovezi), `scripts/`. Fiecare folder are propriul README cu detaliile.

| Laborator | Tema | Ce conține | Versiunea evaluată |
|---|---|---|---|
| [Laborator1](Laborator1/) | Message broker | producer, broker, consumatori; protocol JSON peste TCP; ACK/NACK, retry limitat, deduplicare, dead-letter queue, persistență pe disc | tag `lab1-v1.1` (prima predare: `lab1-v1.0`) |
| [Laborator2](Laborator2/) | Message broker cu gRPC | același sistem, cu comunicarea pe gRPC + protobuf și un serviciu extern de plăți; versionare, deadline, cancellation, idempotency, fallback (outbox), răspuns pierdut după commit | tag `lab2-v1.0` |
| [Laborator3](Laborator3/) | Web proxy: transparența în distribuire | un data warehouse HTTP (JSON/XML) în 3 replici peste Cassandra, în spatele unui reverse proxy cu cache în Redis, load balancing (round-robin / least-connections), health check activ, circuit breaker și registrul conexiunilor clienților; noduri sursă cu PUT și pull | tag `lab3-v1.0` |

## Cerințe

- **Docker Desktop** (Windows sau macOS), pornit. Bazele de date, brokerul și celelalte servicii rulează în containere; nu se instalează nimic separat.
- Opțional, **Go 1.24+**, pentru rularea testelor fără Docker.

Lucrăm pe Windows și pe macOS. Fiecare laborator are scripturile demo atât în bash (`.sh`: macOS, Linux, WSL, Git Bash), cât și în PowerShell (`.ps1`) unde e indicat în README-ul lui.

## Pornire rapidă

Din folderul laboratorului:

| | Pornire | Teste | Demo |
|---|---|---|---|
| Lab 1 | `docker compose up --build` | `go test -race ./...` | `./scripts/demo.sh` |
| Lab 2 | `cp .env.example .env` (PowerShell: `Copy-Item .env.example .env`), apoi `docker compose up --build -d` | `docker compose run --rm tests` | `scripts/demo.ps1` (Windows), `./scripts/demo.sh` |
| Lab 3 | `docker compose up --build -d` | `docker compose run --rm tests` | verificare rapidă: `scripts\smoke.ps1` (Windows) / `./scripts/smoke.sh`; scenarii de eșec: `demo.ps1` / `demo.sh`; măsurători: `bench.ps1` / `bench.sh` |

Pe Windows, scripturile PowerShell se rulează cu `powershell -ExecutionPolicy Bypass -File scripts\<nume>.ps1`, iar pentru cereri manuale se folosește `curl.exe` (în PowerShell, `curl` e un alt program).

Oprire: `docker compose down -v` (șterge și datele).

## Ce livrează fiecare laborator

| Livrabil | Unde (în fiecare folder) |
|---|---|
| contracte (mesaje, API, erori, versionare) | `contracts/` |
| teste automate | `tests/` |
| diagrame | `docs/diagrams/` |
| deciziile, cu alternativele comparate (ADR) | `docs/adr/` |
| dovezile scenariilor de eșec (loguri corelate) | `docs/correlated-log.txt` (Lab 1); `docs/evidence/`, `docs/failure-scenarios.md` (Lab 2, 3) |
| garanții și limite | `docs/delivery-guarantees.md` (Lab 1, 2), `docs/guarantees-and-limits.md` (Lab 3) |
| instrucțiuni de rulare și diagnoză | README-ul laboratorului, `docs/runbook/` |
