# ADR 0002: Tratarea explicită a rezultatului ambiguu

Status: acceptat (lab2-v1.0)

## Context

Scenariul critic al laboratorului: operația la distanță este executată, dar răspunsul se pierde (deadline depășit, conexiune ruptă). Apelantul vede o eroare deși efectul s-a produs. Dacă o tratează ca eșec și reîncearcă orbește, efectul se dublează (plată dublă, eveniment dublu). Dacă o tratează ca succes, poate pierde operații care chiar au eșuat. Situația apare în două locuri: order-service apelează `Broker.Publish`, iar billing apelează `Payment.Charge`.

## Decizie

1. **Identificator stabil pentru fiecare operație cu efect.** `message_id` este derivat din `Idempotency-Key` al cererii HTTP; cheia plății este `order:<order_id>`. Aceeași cerere produce mereu aceleași chei.
2. **Serverele sunt idempotente după cheie.** Brokerul răspunde `duplicate=true` pentru un `message_id` deja salvat (conținut identic) sau `ALREADY_EXISTS` pentru conținut diferit. Serviciul de plăți întoarce rezultatul inițial (`replayed=true`) sau `FAILED_PRECONDITION` pentru parametri diferiți.
3. **Clientul clasifică explicit fiecare rezultat.** Rezultatul poate fi cunoscut (`OK`, eroare definitivă) sau necunoscut (`DEADLINE_EXCEEDED`, `UNAVAILABLE`, `CANCELED`). Rezultatul necunoscut nu este tratat nici ca succes, nici ca eșec.
4. **Rezolvarea ambiguității.** Billing interoghează `GetCharge`: dacă găsește plata, folosește rezultatul existent; dacă primește `NOT_FOUND`, poate plăti din nou cu aceeași cheie. Order-service reîncearcă `Publish` cu același `message_id`.
5. **Retry limitat, cu backoff și jitter.** Retry-ul se face doar pentru codurile tranzitorii și doar pentru operațiile idempotente.
6. **Fallback când rezultatul rămâne necunoscut.** Order-service păstrează comanda `pending` și răspunde `202 pending`; outbox-ul o republică. Billing trimite `nack` tranzitoriu: brokerul relivrează cu backoff, iar după `max-attempts` mesajul ajunge în DLQ, nu se repetă la infinit.

## Alternative

- **Retry fără cheie de idempotență**: duplică efectul la fiecare răspuns pierdut.
- **Tranzacție distribuită (2PC) între broker, billing și plăți**: corectă, dar blocantă, complexă și nesuportată de un furnizor de plăți extern.
- **Deduplicare doar la consumator**: protejează efectul local, dar nu plata la serviciul extern și nici evenimentul publicat de două ori.

## Consecințe

- „Exactly once” este afirmat doar pentru **efect** (o plată per comandă, o rezervare per mesaj), nu pentru livrare. Livrarea rămâne at-least-once, iar testele demonstrează efectul unic.
- Fiecare serviciu cu efecte păstrează cheile într-o fereastră de deduplicare (broker: `BROKER_DEDUP_RETENTION`, implicit 24h).

## Limita cunoscută

Un retry care sosește după expirarea ferestrei de deduplicare este tratat ca mesaj nou. Cheia de plată este stabilă doar dacă `order_id` este stabil, adică dacă clientul retrimite cu același `Idempotency-Key`.
