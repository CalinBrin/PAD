# Semantica erorilor (parte din contract)

Fiecare cod gRPC are un sens fix. Clientul decide după cod dacă reîncearcă, iar decizia este sigură doar pentru operațiile idempotente.

## Broker.Publish

| Cod | Când | S-a salvat mesajul? | Retry |
|---|---|---|---|
| `OK`, `duplicate=false` | mesaj nou, scris pe disc în toate cozile topicului | da | nu e nevoie |
| `OK`, `duplicate=true` | același `message_id` și același conținut fuseseră deja salvate | da, de publicarea anterioară | nu e nevoie |
| `INVALID_ARGUMENT` | plic invalid (lipsește `message_id`, `correlation_id`, `occurred_at` nu este UTC etc.) | nu | nu |
| `FAILED_PRECONDITION` | niciun grup (coadă) nu este legat de topic | nu | nu (eroare de configurare) |
| `ALREADY_EXISTS` | `message_id` folosit deja pentru alt conținut | nu | nu |
| `UNAVAILABLE` | scrierea pe disc a eșuat sau brokerul se oprește | nu | da, cu același `message_id` |
| `DEADLINE_EXCEEDED` / `CANCELED` | clientul a renunțat înainte de commit | nu | da, cu același `message_id` |

Atenție: dacă clientul primește `DEADLINE_EXCEEDED` sau `UNAVAILABLE` din rețea (nu de la broker), rezultatul este **necunoscut**: mesajul poate fi salvat sau nu. De aceea retry-ul se face întotdeauna cu același `message_id`; brokerul răspunde `duplicate=true` dacă mesajul exista.

`duplicate=true` nu este niciodată trimis pentru un mesaj care nu a fost salvat.

## Broker.Consume

| Cod | Când |
|---|---|
| `INVALID_ARGUMENT` | primul mesaj nu este `subscribe`, lipsește `topic`/`group`, `prefetch` > 1000 |
| `UNAVAILABLE` | brokerul se oprește; clientul se reconectează |

Un `ack`/`nack` cu un tag necunoscut sau expirat (după ack timeout) este ignorat și logat, nu închide sesiunea. Închiderea fluxului, din orice motiv, returnează în coadă livrările neconfirmate.

## Payment.Charge / Payment.GetCharge

| Cod / rezultat | Când | Retry |
|---|---|---|
| `OK`, `status=APPROVED` | plata executată | nu |
| `OK`, `status=DECLINED` | rezultat de business (peste limită), nu eroare | nu |
| `OK`, `replayed=true` | plata fusese executată de un apel anterior cu aceeași cheie | nu |
| `INVALID_ARGUMENT` | lipsește cheia, suma <= 0, monedă invalidă | nu (eroare permanentă) |
| `FAILED_PRECONDITION` | cheia folosită deja cu alți parametri | nu (eroare permanentă) |
| `UNAUTHENTICATED` | token lipsă sau greșit | nu (configurare) |
| `NOT_FOUND` (doar `GetCharge`) | nicio plată executată pentru cheie | se poate apela `Charge` |
| `DEADLINE_EXCEEDED` / `UNAVAILABLE` | rezultat necunoscut | da, cu aceeași cheie, după reconciliere cu `GetCharge` |

Dacă deadline-ul apelantului expiră înainte de commit, serviciul de plăți nu execută plata (verifică `ctx` înainte de scriere). Dacă expiră după commit, plata există și `GetCharge` o găsește.

## Consumatori: ce înseamnă rezultatul handler-ului

| Rezultat | Trimis la broker | Efect |
|---|---|---|
| succes | `ack` | mesajul este șters din coadă |
| eroare tranzitorie | `nack` | retry cu backoff, apoi DLQ după `max-attempts` |
| eroare permanentă (`client.Permanent`) | `nack` cu `permanent=true` | DLQ imediat |
| cădere / oprire | nimic | fluxul se închide, mesajul revine în coadă |

Sunt permanente: `message_type` sau `schema_version` necunoscute, payload ilizibil, monedă nesuportată, `INVALID_ARGUMENT` / `FAILED_PRECONDITION` de la serviciul de plăți.
