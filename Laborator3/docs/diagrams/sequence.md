# Diagrame de secvență

## 1. GET: MISS, apoi HIT

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Proxy
    participant R as Redis
    participant D as dw-2
    participant DB as Cassandra
    C->>P: GET /employees/a (Accept: application/json)
    P->>R: MGET cache:employees:GET:/employees/a?:json, gen:employees:all, gen:employees:item:a
    R-->>P: (nil), 0, 7
    P->>D: GET /employees/a (round-robin, pool keep-alive)
    D->>DB: SELECT ... WHERE id = 'a'
    DB-->>D: rând
    D-->>P: 200, ETag "v3-json", Cache-Control max-age=30
    P->>R: SET cache:... {body, gens:[0,7]} PX 30000
    P-->>C: 200, X-Cache: MISS, X-Upstream: dw-2
    C->>P: GET /employees/a
    P->>R: MGET ... 
    R-->>P: {body, gens:[0,7]}, 0, 7  (potrivire)
    P-->>C: 200, X-Cache: HIT, Age: 1
```

## 2. Cursa GET lent / PUT (remarca 1)

```mermaid
sequenceDiagram
    participant C1 as Client 1 (GET)
    participant C2 as Client 2 (PUT)
    participant P as Proxy
    participant R as Redis
    participant D as Replici
    C1->>P: GET /employees/a
    P->>R: MGET → contoare [0,7]
    P->>D: GET (lent)
    D-->>D: citește salary=100
    C2->>P: PUT /employees/a {salary:200}
    P->>R: INCR gen:item:a, gen:lists (înainte) → 8
    P->>D: PUT → commit version=4
    P->>R: INCR (după) → 9
    P-->>C2: 200
    D-->>P: 200 salary=100 (răspunsul lent)
    P->>R: SET cache:... {salary 100, gens:[0,7]}
    Note over R: intrarea are [0,7], contorul e 9:<br/>nu se mai potrivește niciodată
    C1->>P: GET /employees/a
    P->>R: MGET → [0,7] ≠ [0,9] → MISS
    P->>D: GET → salary=200
```

## 3. PUT expirat, dar aplicat

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Proxy
    participant R as Redis
    participant D2 as dw-2
    participant D1 as dw-1
    C->>P: PUT /employees/a
    P->>R: INCR (înainte)
    P->>D2: PUT (încercarea 1)
    D2-->>D2: commit version=10
    Note over P,D2: UPSTREAM_TIMEOUT (2 s) expiră
    P->>D1: PUT (încercarea 2, aceeași cerere)
    D1-->>D1: conținut identic → fără scriere (changed=false)
    Note over P,D1: și această încercare expiră (răspuns întârziat)
    P->>R: INCR (după, oricare ar fi rezultatul)
    P-->>C: 504, X-Outcome: unknown
    Note over P: după WRITE_GRACE (3 s)
    P->>R: INCR (invalidare întârziată)
```

## 4. Replică oprită: health check activ și retry

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Proxy
    participant D2 as dw-2 (oprit)
    participant D3 as dw-3
    Note over D2: docker compose stop dw-2
    C->>P: GET /employees/b
    P->>D2: GET (round-robin)
    D2--xP: connection refused
    P->>D3: retry (GET e idempotent)
    D3-->>P: 200
    P-->>C: 200, X-Attempts: 2
    loop la 2 s, fără legătură cu clienții
        P->>D2: GET /readyz
        D2--xP: eșec
    end
    Note over P: 2 eșecuri → dw-2 scos din rotație (backend_down)
```

## 5. Pull de la nodul sursă

```mermaid
sequenceDiagram
    participant S as source-xml
    participant C as Client
    participant P as Proxy
    participant D as Replici
    S->>P: PUT /employees/emp-x-003 (XML, From: source-xml@host)
    P->>D: PUT → version=1
    C->>P: POST /employees/emp-x-003 {salary:46000}, Idempotency-Key: k
    P->>D: POST → version=5, updated_by=demo-client
    loop la 2 s
        S->>P: GET /updates/employees?since=<cursor> (Accept: application/xml)
        P->>D: GET (fără cache: no-store)
        D-->>S: <updates cursor="..."> modificările din [cursor − 5 s, acum]
        S-->>S: aplică doar version > versiunea locală
    end
```
