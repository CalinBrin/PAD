# Arhitectura

```mermaid
flowchart LR
    C[Client HTTP] -->|"POST /orders<br/>Idempotency-Key, X-Correlation-ID"| API

    subgraph OS[order-service]
        API[API HTTP] --> SVC[Order service]
        SVC --> ODB[(orders.jsonl)]
        SVC -.->|outbox: republică comenzile pending| SVC
    end

    SVC ==>|"gRPC Publish<br/>deadline 1s, retry + jitter,<br/>idempotent după message_id"| B

    subgraph BR[integration-service: broker]
        B[Broker gRPC] --> LOG[(broker.log<br/>fsync, replay)]
        B --> QI[[coada orders.inventory]]
        B --> QB[[coada orders.billing]]
        QB --> DLQ[[DLQ orders.billing]]
    end

    QI ==>|"gRPC Consume (stream)<br/>deliver / ack / nack"| INV[inventory]
    QB ==>|"load balancing<br/>round-robin, prefetch"| B1[billing replica 1]
    QB ==> B2[billing replica 2]

    INV --> EFF[(effects.jsonl<br/>efect + cheie dedup)]
    B1 ==>|"gRPC Charge / GetCharge<br/>deadline 800ms, cheie order:ID"| PAY
    B2 ==> PAY

    subgraph EXT[serviciu extern]
        PAY[payment-service] --> CH[(charges.jsonl)]
    end
```

| Componentă | Director | Rol |
|---|---|---|
| API + order-service | `src/api`, `src/order-service` | validează cererea, salvează comanda, publică `OrderCreated`, outbox |
| Broker | `src/integration-service` | cozi durabile, livrare at-least-once, retry, DLQ, deduplicarea publicărilor |
| inventory | `src/consumers/inventory` | rezervă stocul (efect local idempotent) |
| billing | `src/consumers/billing` | încasează comanda la serviciul extern (idempotent, cu reconciliere) |
| payment-service | `src/payment-service` | serviciul extern: cheie de idempotență, `GetCharge`, token |

Toate apelurile dintre componente sunt gRPC cu contracte protobuf (`contracts/proto`). Correlation ID-ul trece prin headerul HTTP `X-Correlation-ID`, metadata gRPC `x-correlation-id`, câmpul `Envelope.correlation_id` și apare în fiecare linie de log.

Livrarea este push: brokerul trimite mesajul pe fluxul `Consume` imediat ce un consumator are capacitate, fără polling.
