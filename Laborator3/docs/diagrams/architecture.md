# Arhitectura

```mermaid
flowchart LR
    subgraph surse["Noduri sursă (employee nodes)"]
        SJ["source-json<br/>PUT JSON, pull JSON"]
        SX["source-xml<br/>PUT XML, pull XML"]
    end
    subgraph clienti["Clienți (data consumers)"]
        C["client CLI / loadgen / curl<br/>GET, POST"]
    end

    subgraph proxy["proxy :8080 (admin :8081)"]
        L["LimitListener<br/>keep-alive, limită/client"]
        CA["cache<br/>MGET intrare + contoare"]
        LB["balancer<br/>round-robin / least-conn"]
        HC["health check activ<br/>GET /readyz la 2 s"]
        BR["circuit breaker<br/>per replică"]
        REG["registry<br/>conexiuni + cereri"]
    end

    R[("Redis<br/>cache:*, gen:*<br/>client:*, conns:*, req:*")]

    subgraph dw["Data warehouse (replici fără stare)"]
        D1["dw-1"]
        D2["dw-2"]
        D3["dw-3"]
    end
    CS[("Cassandra<br/>pad_lab3.employees (LWT, SAI)<br/>pad_lab3.idempotency")]

    SJ & SX & C -->|HTTP/1.1 keep-alive| L
    L --> CA --> LB
    LB -->|pool keep-alive| D1 & D2 & D3
    HC -.->|/readyz| D1 & D2 & D3
    BR -.- LB
    CA <--> R
    REG -.->|asincron| R
    D1 & D2 & D3 --> CS
```

| Componentă | Cod | Rol |
|---|---|---|
| data warehouse | `src/datawarehouse` (`dw/controller.go` = Controller, `dw/service.go` = Service, `store/` = Data) | nodul informativ: JSON/XML, versiuni, compare-and-set, feed |
| proxy | `src/proxy` (`proxy/`, `balancer/`, `cache/`) | singurul punct de intrare: cache, load balancing, health check, breaker, retry, registrul clienților |
| nod sursă | `src/source-node` | trimite angajații cu PUT, preia modificările clienților prin pull |
| client | `src/client` | CLI pentru GET / list / PUT / POST / feed |
| loadgen | `src/loadgen` | încărcare concurentă, rezultate în fișiere |

Structura DW urmează schema din enunț: **container** (`net/http.Server`, o goroutine per cerere) → **Controller** (maparea rutelor HTTP pe operații, negocierea formatului) → **Service** (reguli, versiuni, idempotență) → **Data** (`store.Store`: Cassandra, sau `Memory` thread-safe în teste).

## Porturi

| Serviciu | Port | Publicat pe host |
|---|---|---|
| proxy (date) | 8080 | da: singura intrare pentru clienți |
| proxy (admin) | 8081 | doar `127.0.0.1` |
| dw-1..3 (date) | 8000 | **nu**: accesibile doar prin proxy |
| dw-1..3 (admin, injectarea eșecurilor) | 9000 | `127.0.0.1:9101..9103` |
| Redis, Cassandra | 6379, 9042 | nu |
