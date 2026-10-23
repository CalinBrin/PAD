# Dovezile scenariilor de eșec

Fiecare scenariu are un test automat (rulat cu `go test -race ./...`) și, pentru cele principale, un log real produs de `scripts/demo.sh` cu procese separate. Logurile complete, filtrate după correlation ID, sunt în `docs/evidence/`. Extrasele de mai jos păstrează doar câmpurile relevante (ora UTC, componenta, evenimentul).

## 1. Scenariul critic: răspunsul plății se pierde după efectuarea operației

| | |
|---|---|
| Stare inițială | broker, payment-service, două replici billing pornite; nicio plată pentru comanda `ord-ef8b492dc5fc` |
| Injectarea eșecului | `POST /faults?lose_responses=1` la payment-service: următoarea plată este executată și salvată, apoi răspunsul este reținut până expiră deadline-ul clientului |
| Comportament observat | billing primește `DeadlineExceeded` după 800 ms; nu știe dacă plata s-a făcut; nu trimite nici `ack`, nici nu presupune eșecul |
| Mecanism de recuperare | billing apelează `GetCharge(order:ord-ef8b492dc5fc)`, găsește plata executată și folosește rezultatul ei (`replayed=true`), apoi trimite `ack` |
| Stare finală | o singură plată (`ch_d6b203959912`), coada `orders.billing` goală, DLQ gol |
| Garanția | cel mult o plată per comandă, indiferent de câte ori se pierde răspunsul sau se relivrează mesajul |
| Nu acoperă | o cheie de idempotență ștearsă de serviciul extern; un serviciu extern care nu oferă `GetCharge` (atunci rămâne doar retry-ul cu aceeași cheie) |

Test: `TestChargeResponseLostAfterCommit_BillingReconcilesAndChargesOnce`. Log: `docs/evidence/demo-lost-charge-001.log`.

```
17:55:22.527 payment       charge executed  charge_id=ch_d6b203959912 status=approved
17:55:22.527 payment       FAULT INJECTED: operation committed, response dropped  method=Charge
17:55:23.323 billing       charge outcome unknown: it may have been executed, reconciling with GetCharge  attempt=1 result=DeadlineExceeded
17:55:23.323 billing       reconciled: the charge was executed, its answer had been lost  attempt=1 charge_id=ch_d6b203959912
17:55:23.323 billing       order charged  attempt=1 replayed=true charge_id=ch_d6b203959912
17:55:23.323 payment       rpc served  method=Charge result=Unavailable duration_ms=800
17:55:23.324 broker        message acked  queue=orders.billing attempt=1
```

## 2. Răspunsul la Publish se pierde după commit

| | |
|---|---|
| Stare inițială | broker pornit cu `BROKER_LOSE_PUBLISH_RESPONSES=1`, cozile `orders.inventory` și `orders.billing` goale |
| Injectarea eșecului | brokerul salvează mesajul, apoi reține răspunsul până expiră deadline-ul order-service (500 ms) |
| Comportament observat | order-service primește `DeadlineExceeded` și logează „publish outcome unknown” |
| Mecanism de recuperare | retry după backoff cu jitter, cu același `message_id`; brokerul găsește mesajul și răspunde `duplicate=true` |
| Stare finală | comanda `published`, clientul primește `202`, mesajul există o singură dată în fiecare coadă |
| Garanția | un eveniment per cerere, chiar dacă publicarea se repetă |
| Nu acoperă | un retry care sosește după fereastra de deduplicare (`BROKER_DEDUP_RETENTION`, implicit 24h) |

Teste: `TestLostPublishResponse_CallerCannotTellTheOutcome` (arată ambiguitatea), `TestPublishResponseLostAfterCommit_RetryIsDeduplicated`, `TestLostPublishResponse_OutboxDoesNotDuplicateTheEvent`. Log: `docs/evidence/demo-lost-publish-001.log`.

```
17:55:13.905 broker        message accepted
17:55:13.905 broker        FAULT INJECTED: operation committed, response dropped  method=Publish
17:55:14.404 order-service publish outcome unknown, the message may be stored; retrying with the same message_id  attempt=1 result=DeadlineExceeded
17:55:14.446 broker        duplicate publish, already stored
17:55:14.446 order-service message published  duplicate=true routed_queues=2
17:55:14.447 order-service http request  method=POST result=202 duration_ms=544
```

## 3. Consumatorul cade după efectul local, înainte de ACK

| | |
|---|---|
| Stare inițială | inventory pornit cu `CRASH_ONCE=after_effect` |
| Injectarea eșecului | inventory scrie efectul în `effects.jsonl`, apoi procesul se oprește brusc (exit 137), fără `ack` |
| Comportament observat | brokerul vede fluxul gRPC închis și mută livrarea înapoi în coada grupului |
| Mecanism de recuperare | procesul repornit se abonează la același grup; primește mesajul cu `attempt=2`, găsește `message_id` în `effects.jsonl` și doar trimite `ack` |
| Stare finală | un singur efect pentru mesaj, coada goală |
| Garanția | at-least-once la livrare, efect unic prin consumator idempotent |
| Nu acoperă | pierderea fișierului `effects.jsonl` (atunci efectul s-ar repeta) |

Livrările aparțin cozii grupului (`orders.inventory`), nu procesului: un proces nou, cu alt nume de instanță, primește livrarea neconfirmată. Nu rămân livrări „orfane” după un crash.

Teste: `TestCrashAfterEffectBeforeAck_WithDedup_EffectAppliedOnce` (1 efect), `..._WithoutDedup_EffectAppliedTwice` (2 efecte), `TestAckBeforeProcess_CrashLosesMessage_AtMostOnce` (0 efecte, mesaj pierdut). Log: `docs/evidence/demo-crash-001.log`.

```
17:55:19.500 broker        message delivered  queue=orders.inventory attempt=1 tag=7
17:55:19.502 inventory     local effect applied  attempt=1
17:55:19.502 inventory     SIMULATED CRASH after effect, before ACK  attempt=1
17:55:19.503 broker        message requeued  queue=orders.inventory attempt=1 reason="consumer disconnected before ack"
17:55:20.513 broker        message delivered  queue=orders.inventory attempt=2 tag=9
17:55:20.513 inventory     duplicate delivery, effect skipped, will ACK  attempt=2
17:55:20.514 broker        message acked  queue=orders.inventory attempt=2
```

## 4. Brokerul indisponibil: fallback prin outbox

| | |
|---|---|
| Stare inițială | order-service pornit, brokerul oprit |
| Injectarea eșecului | brokerul este oprit (proces terminat) când sosește comanda |
| Comportament observat | `Publish` dă `UNAVAILABLE` la toate încercările; comanda rămâne salvată ca `pending`; clientul primește `202 {"status":"pending"}`, nu o eroare și nici un succes fals |
| Mecanism de recuperare | outbox-ul republică la fiecare `OUTBOX_INTERVAL` cu același `message_id`; după repornirea brokerului publicarea reușește |
| Stare finală | comanda `published` (`publish_attempts=3`), comanda încasată o singură dată |
| Garanția | o comandă acceptată nu se pierde dacă brokerul este temporar indisponibil |
| Nu acoperă | pierderea discului order-service înainte de publicare |

Test: `TestBrokerDown_OrderKeptPendingAndPublishedByOutbox`. Log: `docs/evidence/demo-outbox-001.log`.

```
17:55:26.011 order-service publish outcome unknown or broker unavailable, order kept in the outbox  result=Unavailable
17:55:26.012 order-service http request  method=POST result=202 duration_ms=474
17:55:27.211 order-service publish outcome unknown or broker unavailable, order kept in the outbox  result=Unavailable
17:55:28.298 broker        message accepted
17:55:28.298 order-service order published  routed_queues=2
17:55:29.116 billing       order charged  attempt=1 replayed=false charge_id=ch_8838617ccf9f
```

## 5. Eroare permanentă: direct în DLQ

| | |
|---|---|
| Stare inițială | sistem pornit |
| Injectarea eșecului | comandă cu `currency=GBP` (format valid, dar nesuportată de billing) |
| Comportament observat | billing trimite `nack` cu `permanent=true`; inventory procesează normal (nu verifică moneda) |
| Mecanism de recuperare | brokerul mută mesajul în DLQ după o singură livrare, fără retry |
| Stare finală | `GET /dlq?queue=orders.billing` arată mesajul cu `attempts=1`, `permanent=true` |
| Garanția | un mesaj neprocesabil nu este repetat la infinit |
| Nu acoperă | replay automat din DLQ (se face manual, după corectarea cauzei) |

Teste: `TestPermanentErrorIsNotRetried`, `TestRetryThenDeadLetter` (eroare tranzitorie: 3 încercări, apoi DLQ), `TestPaymentDownForGood_MessageEndsInDLQ`. Log: `docs/evidence/demo-dlq-001.log`.

## 6. Cerere HTTP repetată (același Idempotency-Key)

Al doilea `POST /orders` cu aceeași cheie întoarce `200`, `Idempotent-Replayed: true` și aceeași comandă. Nu se creează a doua comandă și nici al doilea eveniment. Aceeași cheie cu alt corp dă `409`. Teste: `TestHTTPAPIContract`. Log: `docs/evidence/demo-idem-001.log`.

## 7. Alte eșecuri acoperite de teste

| Scenariu | Comportament | Test |
|---|---|---|
| Deadline-ul expiră înainte ca plata să fie executată | payment-service vede `ctx` anulat și nu execută; `GetCharge` dă `NOT_FOUND`: ambiguitatea se rezolvă ca „neexecutat” | `TestChargeDeadlineExpiresBeforeCommit_NotExecuted` |
| Deadline-ul expiră înainte de commit la broker | nimic nu se salvează | `TestPublishDeadlineExpiresBeforeCommit_NothingStored` |
| Scriere pe disc eșuată la broker | `UNAVAILABLE`, nimic salvat, `/readyz` raportează eroarea; retry-ul salvează mesajul (nu răspunde fals `duplicate`) | `TestStoreWriteFailure_IsReportedAndRetrySucceeds` |
| payment-service indisponibil, apoi revine | billing trimite `nack`, brokerul relivrează cu backoff, comanda este încasată o dată | `TestPaymentUnavailable_BillingNacksThenRecovers` |
| Două replici billing primesc aceeași comandă simultan | o singură plată (cheia de idempotență) | `TestTwoBillingReplicasChargeTheSameOrderOnce` |
| Consumator lent (peste ack timeout) | relivrare la alt consumator; efectul rămâne unic (verificare și scriere atomice) | `TestAckTimeoutDuplicateIsAbsorbedByDeduplication` |
| Sesiunea consumatorului anulată | livrarea neconfirmată merge imediat la alt consumator | `TestConsumerSessionCancelled_InflightGoesToAnotherConsumer` |
| Repornirea brokerului | mesajele neconfirmate, DLQ și fereastra de deduplicare se refac din log; subscriberii se reconectează | `TestPersistence*`, `TestSubscriberReconnectsAfterBrokerRestart` |
| Mesaj publicat fără consumator conectat | rămâne în coada durabilă și este livrat la conectare | `TestMessagePublishedWithoutConnectedConsumerIsDeliveredLater` |
| Topic fără coadă | refuz explicit `FAILED_PRECONDITION`, nimic pierdut în tăcere | `TestPublishToTopicWithoutQueueIsRefused` |
