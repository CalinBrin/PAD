# Diagrame de secvență

## 1. Flux nominal

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant O as order-service
    participant B as Broker
    participant I as inventory
    participant L as billing
    participant P as payment

    C->>O: POST /orders (Idempotency-Key k, X-Correlation-ID c)
    O->>O: salvează comanda (pending), message_id = hash(k)
    O->>B: Publish(envelope) [deadline 1s]
    B->>B: scrie enqueue în log, fsync
    B-->>O: OK (routed_queues = 2)
    O->>O: status = published
    O-->>C: 202 {status: published}
    B->>I: Delivery (tag 1, attempt 1)
    I->>I: efect local + message_id în effects.jsonl
    I-->>B: Ack (tag 1)
    B->>L: Delivery (tag 2, attempt 1)
    L->>P: Charge(key = order:ID) [deadline 800ms]
    P->>P: execută plata, fsync
    P-->>L: APPROVED
    L-->>B: Ack (tag 2)
```

## 2. Scenariul critic: răspunsul se pierde după efectuarea plății

```mermaid
sequenceDiagram
    autonumber
    participant B as Broker
    participant L as billing
    participant P as payment

    B->>L: Delivery (attempt 1)
    L->>P: Charge(key = order:ord-1) [deadline 800ms]
    P->>P: plata executată și salvată
    P--xL: răspunsul se pierde
    Note over L: DEADLINE_EXCEEDED: plata poate exista sau nu
    L->>P: GetCharge(key = order:ord-1)
    alt plata există
        P-->>L: APPROVED (replayed = true)
        L-->>B: Ack
    else NOT_FOUND
        L->>P: Charge(aceeași cheie)
        P-->>L: APPROVED
        L-->>B: Ack
    else serviciul indisponibil
        L-->>B: Nack (tranzitoriu)
        Note over B: retry cu backoff, aceeași cheie, DLQ după max-attempts
    end
    Note over P: o singură plată pentru ord-1, oricâte încercări
```

## 3. Răspunsul la Publish se pierde după commit

```mermaid
sequenceDiagram
    autonumber
    participant O as order-service
    participant B as Broker

    O->>B: Publish(message_id m) [deadline 500ms]
    B->>B: mesaj salvat în log și în cozi
    B--xO: răspunsul se pierde
    Note over O: DEADLINE_EXCEEDED: rezultat necunoscut
    O->>B: Publish(același message_id m)
    B->>B: m există, același conținut
    B-->>O: OK (duplicate = true)
    Note over B: mesajul există o singură dată în fiecare coadă
```

## 4. Brokerul indisponibil: fallback prin outbox

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant O as order-service
    participant B as Broker

    C->>O: POST /orders
    O->>O: salvează comanda (pending)
    O-xB: Publish: UNAVAILABLE (4 încercări cu backoff)
    O-->>C: 202 {status: pending}
    Note over B: brokerul repornește
    loop la fiecare OUTBOX_INTERVAL
        O->>B: Publish(același message_id)
    end
    B-->>O: OK
    O->>O: status = published
```

## 5. Consumatorul cade după efect, înainte de ACK

```mermaid
sequenceDiagram
    autonumber
    participant B as Broker
    participant I1 as inventory (procesul 1)
    participant S as effects.jsonl
    participant I2 as inventory (procesul 2)

    B->>I1: Delivery (tag 7, attempt 1)
    I1->>S: efect + message_id (fsync)
    Note over I1: cădere (exit 137), fără ack
    I1--xB: fluxul gRPC se închide
    B->>B: livrarea revine în coada grupului inventory
    I2->>B: Consume(subscribe, group = inventory)
    Note over B: coada aparține grupului, nu procesului
    B->>I2: Delivery (tag 9, attempt 2, redelivered)
    I2->>S: message_id există?
    S-->>I2: da: efectul nu se repetă
    I2-->>B: Ack (tag 9)
```
