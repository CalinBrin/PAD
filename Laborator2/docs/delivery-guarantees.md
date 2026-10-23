# Garanții și limite

## Ce garantează sistemul

| Proprietate | Garanție | Mecanism | Dovadă |
|---|---|---|---|
| Publicare | confirmată doar după `fsync` în toate cozile topicului | log append-only, `puback` după scriere | `TestStoreWriteFailure_IsReportedAndRetrySucceeds` |
| Publicare repetată | un mesaj per `message_id` (în fereastra de deduplicare) | `published` în log, compararea conținutului | `TestPublishIsIdempotentByMessageID`, `TestPersistenceDeduplicationSurvivesRestart` |
| Livrare | at-least-once | mesajul rămâne in-flight până la `ack`; `nack`, ack timeout și închiderea fluxului îl relivrează | `TestCrashAfterEffectBeforeAck_*` |
| Efect local (inventory) | o dată per mesaj | `effects.jsonl`: efect și cheie scrise atomic (`ApplyOnce`) | `TestAckTimeoutDuplicateIsAbsorbedByDeduplication` |
| Efect extern (plată) | cel mult o plată per comandă | cheia `order:<order_id>`, `GetCharge` pentru reconciliere | `TestChargeResponseLostAfterCommit_*`, `TestTwoBillingReplicas*` |
| Cerere HTTP repetată | o comandă per `Idempotency-Key` | id-uri derivate din cheie | `TestHTTPAPIContract` |
| Mesaj neprocesabil | nu se repetă la infinit | DLQ imediat pentru erori permanente, după `max-attempts` pentru cele tranzitorii | `TestPermanentErrorIsNotRetried`, `TestRetryThenDeadLetter` |
| Consumator repornit | primește livrările neconfirmate | livrările aparțin cozii grupului, nu procesului | `TestCrashAfterEffectBeforeAck_WithDedup_EffectAppliedOnce` |
| Mesaj fără consumator conectat | păstrat până la conectare | cozi durabile declarate (`BROKER_QUEUES`) | `TestMessagePublishedWithoutConnectedConsumerIsDeliveredLater` |
| Repornirea brokerului | nu pierde mesajele confirmate | replay din `broker.log` | `TestPersistence*` |

„Exactly once” se afirmă **doar pentru efecte** (o rezervare, o plată), nu pentru livrare. Livrarea este at-least-once, iar duplicatele sunt absorbite de consumatori idempotenți și de cheia de idempotență a serviciului extern.

`CONSUMER_ACK_MODE=before` (doar pentru inventory) arată varianta at-most-once: `ack` înainte de efect; o cădere între ele pierde mesajul (`TestAckBeforeProcess_CrashLosesMessage_AtMostOnce`).

## Deadline-uri, retry și idempotență pe fiecare apel

| Apel | Deadline per încercare | Încercări | Backoff | Idempotent după | Retry pe |
|---|---|---|---|---|---|
| order-service la `Broker.Publish` | `PUBLISH_TIMEOUT` (1s) | `PUBLISH_MAX_ATTEMPTS` (4), apoi outbox | full jitter, 100ms x 2^n, max 2s | `message_id` | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED` |
| billing la `Payment.Charge` | `PAYMENT_TIMEOUT` (800ms) | `PAYMENT_MAX_ATTEMPTS` (3), apoi `nack` | full jitter, 200ms x 2^n, max 2s | `order:<order_id>` | aceleași coduri, după `GetCharge` |
| billing la `Payment.GetCharge` | `PAYMENT_LOOKUP_TIMEOUT` (800ms) | 1 per încercare de plată | | operație de citire | |
| broker, relivrare după `nack` / ack timeout | `BROKER_ACK_TIMEOUT` (30s) | `BROKER_MAX_ATTEMPTS` (5) | 1s x 2^n, max 30s | consumator idempotent | orice eroare tranzitorie |
| subscriber, reconectare la broker | | nelimitat cât rulează procesul | full jitter, 200ms x 2^n, max 5s | sesiune nouă, același grup | flux închis |

Jitter-ul („full jitter”: întârziere aleatoare între 0 și fereastra curentă) evită ca mulți clienți să reîncerce simultan după aceeași cădere. Retry-ul nu se aplică niciodată pentru `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `ALREADY_EXISTS`, `UNAUTHENTICATED`.

## Anulare (cancellation)

- Deadline-ul clientului este propagat de gRPC. Brokerul și serviciul de plăți verifică `ctx` înainte de commit: un apel abandonat nu produce efect (`TestPublishDeadlineExpiresBeforeCommit_NothingStored`, `TestChargeDeadlineExpiresBeforeCommit_NotExecuted`).
- La oprirea unui consumator (SIGTERM), contextul handler-ului este anulat, apelul la serviciul de plăți se anulează, iar mesajul nu este confirmat: brokerul îl relivrează.
- Dacă clientul HTTP se deconectează în timpul publicării, contextul cererii se anulează; comanda rămâne `pending`, iar outbox-ul o publică.

## Ordinea mesajelor

- FIFO per coadă pentru prima livrare; după repornire ordinea inițială se păstrează.
- Nu există ordine între consumatorii concurenți ai aceluiași grup (load balancing) și nici între grupuri.
- Un mesaj reîncercat poate fi depășit de mesajele de după el: așteaptă un consumator care nu l-a eșuat.
- Pentru ordine strictă: un singur consumator per grup, `prefetch=1`.

## Load balancing

Consumatorii aceluiași grup (worker-ii unui proces și replicile `billing`) concurează pentru mesaje: brokerul alege round-robin un consumator cu capacitate liberă (`prefetch`). Un mesaj reîncercat merge, dacă se poate, la alt consumator decât cel care l-a eșuat. Testul `TestLoadBalancingAcrossConsumersOfAGroup` împarte 60 de mesaje între 3 consumatori (20/20/20); demo-ul împarte 10 comenzi între cele două replici billing (5/5).

## Limite cunoscute

- Brokerul este o singură instanță (single point of failure), fără replicare. Un mutex global protejează starea: corect, dar nu scalează pe mai multe nuclee.
- Câte un `fsync` per publicare: debit limitat de disc.
- Contorul de încercări și consumatorii care au eșuat un mesaj sunt în memorie; după repornirea brokerului se resetează.
- Fereastra de deduplicare a publicărilor este finită (`BROKER_DEDUP_RETENTION`); este curățată doar la pornire.
- Un consumator lent (peste `BROKER_ACK_TIMEOUT`) primește duplicate; efectul rămâne unic, dar munca se face de două ori. Valoarea implicită de 30s se alege peste timpul maxim de procesare.
- Fără TLS; tokenul serviciului de plăți circulă necriptat în rețeaua Docker.
- Un `ack` care nu poate fi scris pe disc este logat; după o repornire mesajul se relivrează, iar consumatorul idempotent îl ignoră.
