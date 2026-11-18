# ADR 0006: Politica de retry și deadline a proxy-ului

Status: acceptat (lab3-v1.0)

## Context

Proxy-ul poate ascunde de client căderea unei replici trimițând cererea la alta. Un retry e însă sigur doar dacă repetarea nu produce un efect în plus. Retry-ul unei cereri cu efecte (remarca 2: un GET care închiria livrări) transformă un timeout al proxy-ului într-o pierdere de date.

## Decizie

| Cerere | Retry | De ce e sigur |
|---|---|---|
| `GET`, `HEAD` | da | citiri pure, inclusiv feed-ul (ADR 0005) |
| `PUT` | da | același corp ⇒ aceeași stare; DW nu creează o versiune nouă (`X-Changed: false`) |
| `POST` cu `Idempotency-Key` | da | DW reia răspunsul păstrat pentru cheie; patch-ul are valori absolute |
| `POST` fără cheie | **nu** | o repetare ar putea crea o modificare în plus (o versiune nouă) |

- **Când:** fără răspuns (conexiune, timeout), 502, 503, 504. Nu la 4xx și nici la 500 (eroarea cererii).
- **Unde:** pe altă replică decât cele încercate deja (dacă toate au fost încercate, oricare din nou).
- **Cât:** `MAX_ATTEMPTS` (3) încercări, fiecare cu deadline `UPSTREAM_TIMEOUT` (2 s), toate în `REQUEST_TIMEOUT` (6 s).
- **Pauza:** full jitter, aleator în `[0, RETRY_BACKOFF × 2^(n-1)]` (50 ms, 100 ms). Mulți clienți nu reîncearcă simultan.
- **Anulare:** dacă clientul închide conexiunea, contextul cererii spre DW se anulează. DW verifică contextul înainte de commit, deci o scriere abandonată înainte de commit nu produce efect.
- **Rezultat necunoscut:** o scriere fără răspuns după toate încercările primește `504` / `502` cu `X-Outcome: unknown`, iar cache-ul e invalidat oricum (ADR 0002).

## Alternative comparate

| Variantă | Problemă |
|---|---|
| Fără retry | căderea unei replici devine eroare la client, deși alte două replici sunt sănătoase |
| Retry pentru orice metodă | un POST fără cheie se poate aplica de două ori; un GET cu efecte (lease) se strică (remarca 2) |
| Retry nelimitat / fără deadline total | un client așteaptă la nesfârșit; retry storm când toate replicile sunt lente |
| Hedging (aceeași cerere trimisă în paralel la 2 replici) | dublează încărcarea; pentru scrieri e sigur doar cu idempotență |
| **Retry limitat, doar idempotent, pe altă replică, cu jitter** (ales) | |

## Dovezi

- `TestGetRetriedOnAnotherReplica`, `TestPostRetriedOnlyWithIdempotencyKey`, `TestPutRetryAfterLostAnswerIsNoOp`, `TestChangeFeedRetryHasNoSideEffect`.
- `demo-put-timeout-001.log`: PUT aplicat o singură dată (`version=10 changed=true`), cele 2 retry-uri `changed=false`, apoi 504 cu `X-Outcome: unknown` și invalidări înainte, după și întârziată.
- Benchmark-ul cu dw-2 oprit: 9 retry-uri, 0 erori la clienți.
