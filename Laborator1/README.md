# PAD Lab 1: Message Broker (Go)

Flux producer, broker, consumers cu confirmări ACK/NACK, retry limitat, deduplicare, dead-letter queue și persistență pe disc. Scris în Go, fără dependențe externe.

## Structura

```
src/
  api/                  API HTTP al producer-ului (POST /orders)
  order-service/        producer-ul: creează comanda și publică OrderCreated
  integration-service/  broker-ul (broker/), protocolul (protocol/), SDK client (client/)
  consumers/            inventory, billing și codul comun (common/)
contracts/              schema mesajului, schema payload-ului, protocolul broker-ului
tests/                  unit, integration, contract, failure (+ testutil)
deployment/             Dockerfile
docs/                   diagrams/, adr/, runbook/, delivery-guarantees.md, correlated-log.txt
scripts/                demo.sh
docker-compose.yml
```

## Rulare

Necesită Go 1.22 sau mai nou.

```bash
go test -race ./...        # toate testele
./scripts/demo.sh          # demo cu procese reale (necesită curl și jq)
docker compose up --build  # alternativ, cu Docker
```

Demo-ul pornește broker-ul, doi consumatori și API-ul, apoi rulează: o comandă normală, scenariul de eșec (consumer-ul inventory cade după efect, înainte de ACK, este repornit și primește mesajul din nou) și o comandă otrăvită care ajunge în DLQ. Logurile corelate sunt în `logs/correlated.log` (o copie rulată este în `docs/correlated-log.txt`).

Trimite manual o comandă:

```bash
curl -X POST localhost:8080/orders -H "X-Correlation-ID: test-1" -d '{"customer_id":"c1","amount":10}'
curl localhost:8081/stats
```

Încărcare cu mai mulți publisheri concurenți: `go run ./src/order-service -load 100 -publishers 5`.

## Versiune evaluată

Versiunea evaluată este marcată în git cu tag-ul `lab1-v1.1`.

| Tag | Ce conține |
|---|---|
| `lab1-v1.0` | prima predare |
| `lab1-v1.1` | corecturile după evaluare (mai jos) |

Corecturi în `lab1-v1.1`:

1. Livrările in-flight sunt indexate după un `tag` unic per livrare, nu după `message.id`. Două publicări cu același id nu mai blochează consumatorul care a primit prima copie (`TestDuplicateMessageIDDoesNotBlockConsumer`, `TestDuplicateMessageIDSurvivesRestart`).
2. Un mesaj retrimis nu mai este dat înapoi consumatorului care tocmai l-a eșuat, cât timp grupul are alt consumator; nu mai ajunge în DLQ dacă un alt consumator este sănătos (`TestRetryPrefersAnotherConsumer`).
3. JSON invalid primește un frame `error` înainte de închiderea conexiunii (`TestInvalidJSONGetsErrorBeforeClose`).
4. Erorile permanente (payload invalid, schemă necunoscută, regulă de business) merg direct în DLQ, fără retry (`TestPermanentErrorIsNotRetried`, `TestProcessorClassifiesPermanentErrors`).

## Mesajul

Fiecare mesaj are `id`, `type`, `timestamp`, `correlation_id`, `schema_version` și `payload` (vezi `contracts/`). `correlation_id` vine din headerul `X-Correlation-ID` (sau se generează) și apare în logurile broker-ului, producer-ului și consumatorilor.

## Cum se acoperă baremul

| Nota | Cerință | Unde este realizată | Test |
|---|---|---|---|
| 5 | Un publisher, un subscriber | `client.Publisher`, `client.Subscriber` | `TestOnePublisherOneSubscriber` |
| 6 | Mai mulți publisheri și subscriberi concurent | fiecare conexiune are propriul goroutine; consumatori concurenți în grup (`-workers`), publisheri în `-load -publishers` | `TestManyPublishersManySubscribersConcurrently` |
| 7 | Adăugare și eliminare dinamică, thread-safe | conectare și deconectare oricând; starea broker-ului este protejată de un mutex | `TestDynamicSubscribersAndPublishers` (rulat cu `-race`) |
| 8 | Excepții: deconectări neașteptate | `dropConn` repune mesajele in-flight, `ack-timeout` pentru consumeri blocați, frame-uri rupte sau invalide nu afectează broker-ul | `tests/failure/*` |
| 9 | Persistență, erori în broker, retransmitere | log append-only cu `fsync`, replay la pornire, retry cu backoff pe alt consumator, erori permanente direct în DLQ, DLQ persistentă | `TestPersistence*`, `TestRetryThenDeadLetter`, `TestRetryPrefersAnotherConsumer`, `TestPermanentErrorIsNotRetried` |

Cerințe suplimentare ale laboratorului:

| Cerință | Realizare |
|---|---|
| Minim doi consumatori | `inventory` și `billing` (grupuri diferite, fiecare primește toate mesajele) |
| ACK/NACK | `client.Handler`: `nil` este ACK, eroare este NACK, `client.Permanent(err)` este NACK fără retry. ACK/NACK identifică livrarea prin `tag` |
| Retry limitat | `-max-attempts` (3), backoff exponențial, doar pentru erori tranzitorii |
| Deduplicare | consumer idempotent pe `message.id` (`common.EffectStore`), vezi ADR 0002 |
| Ordering declarat | `docs/delivery-guarantees.md` |
| Dead-letter queue | `GET /dlq?queue=orders.billing` |
| Scenariu de eșec | `tests/failure`: `TestCrashAfterEffectBeforeAck_*` și demo.sh |
| Diagramă de secvență | `docs/diagrams/sequence.md` |
| Log corelat | `docs/correlated-log.txt` |
| Explicația garanțiilor | `docs/delivery-guarantees.md` |

## Teste

```
tests/unit         envelope, validare, backoff, EffectStore
tests/integration  flux complet, două grupuri, concurență, dinamic, retry, DLQ, persistență, API HTTP,
                   id duplicat, retry pe alt consumator, erori permanente
tests/contract     mesajul serializat respectă schemele din contracts/, formatul frame-urilor
tests/failure      crash după efect înainte de ACK (cu și fără dedup), at-most-once, consumer
                   deconectat, ack timeout, publisher cu frame rupt sau garbage, JSON invalid
                   primește error înainte de închidere
```

## Documentație

- `docs/delivery-guarantees.md`: trimitere versus procesare confirmată, at-least-once, at-most-once, ordering, limite
- `docs/diagrams/sequence.md`: fluxul normal, scenariul de eșec, retry și DLQ
- `docs/adr/`: deciziile de arhitectură
- `docs/runbook/runbook.md`: operare și depanare
