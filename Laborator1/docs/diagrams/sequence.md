# Diagrame de secvență

Diagramele sunt scrise în Mermaid și se randează direct pe GitHub.

## 1. Fluxul normal: trimitere confirmată, apoi procesare confirmată

`puback` înseamnă doar că mesajul este pe disc în broker. `ack` înseamnă că procesarea s-a terminat. Sunt două confirmări diferite, pe două drumuri diferite.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client HTTP
    participant API as API + order-service
    participant B as Broker (log pe disc)
    participant I as Consumer inventory
    participant L as Consumer billing

    C->>API: POST /orders (X-Correlation-ID)
    API->>B: publish(OrderCreated, id, timestamp, correlation_id, schema_version=1)
    B->>B: append enqueue + fsync (o copie per grup)
    B-->>API: puback (mesaj persistat)
    API-->>C: 202 Accepted
    Note over C,API: 202 NU înseamnă "comandă procesată"
    B->>I: deliver (attempt=1)
    B->>L: deliver (attempt=1)
    I->>I: efect local (rezervă stoc)
    I-->>B: ack
    L->>L: efect local (creează factură)
    L-->>B: ack
    B->>B: append ack (mesajul iese din coadă)
```

## 2. Scenariul de eșec: consumatorul cade după efectul local, înainte de ACK

```mermaid
sequenceDiagram
    autonumber
    participant B as Broker
    participant I1 as inventory (proces 1)
    participant S as Efecte (fișier)
    participant I2 as inventory (repornit)

    B->>I1: deliver (attempt=1)
    I1->>S: apply efect (message_id salvat, fsync)
    Note over I1: CRASH înainte de ACK
    I1--xB: conexiunea cade, niciun ack
    B->>B: mesaj in-flight devine pending (consumer disconnected before ack)
    I2->>B: subscribe (după restart)
    B->>I2: deliver (attempt=2, redelivered=true)
    I2->>S: message_id există deja?
    S-->>I2: da (duplicat)
    I2->>I2: sare peste efect (idempotent)
    I2-->>B: ack
    B->>B: append ack
    Note over B,S: Efectul s-a produs o singură dată, livrarea de două ori
```

## 3. Eroare permanentă: direct în dead-letter queue

```mermaid
sequenceDiagram
    autonumber
    participant B as Broker
    participant L as billing

    B->>L: deliver (tag=7, attempt=1)
    L-->>B: nack (tag=7, permanent=true, amount <= 0)
    B->>B: append dlq (attempts=1, permanent=true)
    Note over B: fără retry: o comandă cu sumă negativă nu devine validă la a doua încercare
```

## 4. Eroare tranzitorie: retry pe alt consumator, apoi DLQ

```mermaid
sequenceDiagram
    autonumber
    participant B as Broker
    participant L1 as billing-1
    participant L2 as billing-2

    B->>L1: deliver (tag=1, attempt=1)
    L1-->>B: nack (tag=1, eroare tranzitorie)
    B->>B: backoff 1x, billing-1 marcat ca eșuat pentru acest mesaj
    B->>L2: deliver (tag=2, attempt=2)
    alt billing-2 reușește
        L2-->>B: ack (tag=2)
    else și billing-2 eșuează
        L2-->>B: nack (tag=2)
        B->>B: toți consumatorii au eșuat, backoff 2x
        B->>L1: deliver (tag=3, attempt=3)
        L1-->>B: nack (tag=3)
        B->>B: attempts = max-attempts, append dlq
    end
```
