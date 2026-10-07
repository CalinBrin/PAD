# Runbook

## Pornire

Local, cu Docker: `docker compose up --build`. Fără Docker: `./scripts/demo.sh` (pornește totul, rulează cele trei scenarii și scrie `logs/correlated.log`).

Porturi: broker 7000 (TCP), admin 8081 (HTTP), API 8080 (HTTP).

## Verificări rapide

| Ce vrei să știi | Comandă |
|---|---|
| Starea cozilor | `curl localhost:8081/stats` |
| Mesaje moarte (DLQ) | `curl "localhost:8081/dlq?queue=orders.billing"` |
| Trimite o comandă | `curl -X POST localhost:8080/orders -H "X-Correlation-ID: test-1" -d '{"customer_id":"c1","amount":10}'` |
| Urmărește un flux | `grep correlation_id=test-1` în logurile tuturor serviciilor |

Câmpurile din `/stats`: `pending` (în așteptare), `inflight` (livrate, fără ack), `retrying` (în backoff), `consumers` (conectați), `dlq`.

## Situații și acțiuni

**`pending` crește și `consumers` este 0.** Nu rulează niciun consumer pentru grup. Pornește-l. Mesajele sunt în siguranță în log și se livrează la conectare.

**`pending` crește, `consumers` > 0, `inflight` rămâne plin.** Consumer-ii sunt blocați (nu trimit ack). După `ack-timeout` mesajele sunt redate automat. Verifică logurile consumer-ului și mărește `-workers` sau `-prefetch` dacă procesarea este doar lentă.

**Un consumer a murit.** Nu este nevoie de nicio acțiune în broker: mesajele lui in-flight revin singure în coadă (`consumer disconnected before ack` în log). Repornește consumer-ul. Efectele deja aplicate sunt recunoscute prin deduplicare.

**Mesaje în DLQ.** Sunt mesaje care au eșuat de `max-attempts` ori. Citește `reason` din `/dlq`, corectează cauza (cod sau date), apoi republică mesajul corectat cu un `POST /orders` nou. Nu există replay automat din DLQ.

**Broker-ul a căzut.** `POST /orders` răspunde `503` (producer-ul încearcă o singură reconectare). La repornire, broker-ul reîncarcă din `broker.log` toate mesajele neconfirmate. Datele sunt în directorul `-data` (volumul `broker-data` în compose), nu îl șterge.

**`broker.log` pare corupt.** Liniile ilizibile sunt ignorate cu un avertisment `ignoring unreadable log record`. Dacă lipsesc mesaje, verifică acest avertisment la pornire.

## Parametri importanți

| Flag | Implicit | Efect |
|---|---|---|
| `-max-attempts` | 3 | livrări înainte de DLQ |
| `-ack-timeout` | 10s | timp până la redelivery pentru mesaj fără ack |
| `-retry-base` | 500ms | backoff: 1x, 2x, 4x (maxim 30s) |
| consumer `-workers` | 2 | abonați concurenți în proces |
| consumer `-dedup` | true | `false` arată duplicatele at-least-once |
| consumer `-ack-mode` | after | `before` arată at-most-once |
| consumer `-crash-once` | gol | `after_effect` sau `before_effect`, cade o singură dată |
