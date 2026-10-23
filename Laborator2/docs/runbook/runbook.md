# Runbook

## Pornire

```powershell
Copy-Item .env.example .env        # o singură dată; setează PAYMENT_API_TOKEN
docker compose up --build -d
docker compose ps                   # broker și payment: healthy
```

Porturi: broker gRPC 7000, admin broker 8081, API 8080, payment gRPC 9000, admin payment 8082.

## Verificări

| Ce vrei să știi | Comandă |
|---|---|
| Starea cozilor | `Invoke-RestMethod http://localhost:8081/stats` |
| DLQ | `Invoke-RestMethod "http://localhost:8081/dlq?queue=orders.billing"` |
| Broker gata de trafic | `Invoke-RestMethod http://localhost:8081/readyz` |
| API și starea brokerului | `Invoke-RestMethod http://localhost:8080/readyz` |
| Starea unei comenzi | `Invoke-RestMethod http://localhost:8080/orders/<order_id>` |
| Plățile executate | `Invoke-RestMethod http://localhost:8082/charges` |
| Fluxul unei comenzi | `docker compose logs \| Select-String "<correlation_id>"` |

Câmpurile din `/stats`: `pending` (în așteptare), `inflight` (livrate, fără ack), `retrying` (în backoff), `consumers` (conectați), `dlq`.

## Situații și acțiuni

**`/readyz` al API-ului arată `degraded`, `broker: transient_failure`.** Brokerul este indisponibil. Comenzile se acceptă în continuare ca `pending`; outbox-ul le publică după revenirea brokerului. Verifică `docker compose logs broker`.

**`/readyz` al brokerului dă 503 `storage unavailable`.** Scrierile pe disc eșuează. Publicările primesc `UNAVAILABLE` (nimic nu se salvează, retry-ul e sigur). Verifică spațiul pe disc al volumului `broker-data`.

**`pending` crește și `consumers` este 0.** Niciun consumator conectat pentru grup. Mesajele sunt în siguranță în coada durabilă. Pornește consumatorul.

**`inflight` rămâne plin.** Consumatorii nu trimit ack. După `BROKER_ACK_TIMEOUT` mesajele sunt relivrate. Dacă procesarea este doar lentă, mărește `BROKER_ACK_TIMEOUT` sau numărul de replici.

**Un consumator a căzut.** Nimic de făcut în broker: livrările lui revin în coadă. Docker îl repornește (`restart: unless-stopped`).

**Billing logează `charge outcome unknown`.** Serviciul de plăți nu răspunde în deadline. Billing reconciliază cu `GetCharge`; dacă nici asta nu merge, trimite `nack` și mesajul se relivrează. O plată dublă nu este posibilă (cheia `order:<order_id>`).

**Mesaje în DLQ.** Citește `reason` și `permanent` din `/dlq`. `permanent: true` înseamnă date invalide (de exemplu moneda nesuportată): corectează și trimite o comandă nouă. `permanent: false` înseamnă că eșecurile tranzitorii au epuizat `BROKER_MAX_ATTEMPTS` (de exemplu payment-service căzut prea mult timp). Nu există replay automat din DLQ.

**Un client primește `FAILED_PRECONDITION: no queue is bound to the topic`.** `BROKER_QUEUES` nu declară coada. Mesajul nu a fost salvat; corectează configurația.

## Injectarea eșecurilor (demo)

| Eșec | Cum |
|---|---|
| răspunsul plății pierdut după commit | `Invoke-RestMethod -Method Post "http://localhost:8082/faults?lose_responses=1"` |
| răspunsul la Publish pierdut după commit | `BROKER_LOSE_PUBLISH_RESPONSES=1` în `.env`, apoi `docker compose up -d broker` |
| inventory cade după efect | `$env:CRASH_ONCE="after_effect"; docker compose up -d inventory` |
| brokerul indisponibil | `docker compose stop broker`, apoi `docker compose start broker` |
| serviciul de plăți indisponibil | `docker compose stop payment` |
| latență la plăți | `PAYMENT_LATENCY=2s` în `.env`, apoi `docker compose up -d payment` |

`CRASH_ONCE` cade o singură dată: fișierul `crashed.marker` din volumul `inventory-data` împiedică o nouă cădere. Pentru a repeta scenariul de la zero: `docker compose down -v`.
