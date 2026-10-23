# ADR 0003: Codul generat din contracte nu se comite

Status: acceptat (lab2-v1.0)

## Context

Regulile de predare cer ca fișierele generate să nu fie în repository. Codul Go din `.proto` (`*.pb.go`, `*_grpc.pb.go`) este generat. Fără el proiectul nu compilează.

## Decizie

- Sursa de adevăr sunt fișierele din `contracts/proto`. Directorul `gen/` este în `.gitignore`.
- `scripts/gen.sh` generează `gen/` cu versiuni fixate: protoc 27.2 (din `grpcio-tools==1.66.2`), `protoc-gen-go v1.35.1`, `protoc-gen-go-grpc v1.5.1`.
- `deployment/Dockerfile` rulează generarea într-o etapă proprie, deci `docker compose up --build` și `docker compose run --rm tests` funcționează pe un clone curat, fără unelte instalate local.
- Pentru IDE, `gen/` se poate exporta pe disc: `docker build -f deployment/Dockerfile --target gen-export --output type=local,dest=gen .`

## Alternative

- **Comiterea codului generat** (convenția obișnuită în Go): `go build` merge direct, dar încalcă regula de predare și poate rămâne desincronizat față de `.proto`.
- **buf**: o unealtă în plus, fără câștig la trei fișiere `.proto`.

## Consecințe

- Codul generat corespunde mereu contractelor din commit-ul evaluat.
- Fără Docker, sunt necesare uneltele de generare înainte de `go build` (instrucțiuni în `scripts/gen.sh`).

## Limita cunoscută

Un clone curat nu compilează cu `go build ./...` simplu până nu se rulează generarea (prin Docker sau `scripts/gen.sh`).
