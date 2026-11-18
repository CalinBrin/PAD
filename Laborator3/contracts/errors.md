# Semantica erorilor (parte din contract)

Corpul unei erori este în formatul cerut:

```json
{"code":"not_found","message":"not found","correlation_id":"demo-1"}
```
```xml
<error><code>not_found</code><message>not found</message><correlation_id>demo-1</correlation_id></error>
```

## Răspunsurile replicii DW

| Status | `code` | Când | S-a aplicat scrierea? | Retry |
|---|---|---|---|---|
| 400 | `invalid` | câmp invalid, JSON/XML malformat, `id` din corp ≠ `id` din URL, `limit` în afara 1..100 | nu | nu |
| 404 | `not_found` | angajat inexistent (și `POST` pe un angajat inexistent) | nu | nu |
| 406 | `not_acceptable` | niciun format acceptabil în `Accept` / `format` | nu | nu |
| 409 | `conflict` | compare-and-set pierdut de 8 ori la rând (contenție extremă) | nu | da |
| 412 | `version_mismatch` | `If-Match` diferă de versiunea curentă | nu | nu (recitește) |
| 415 | `unsupported_media_type` | `Content-Type` nici JSON, nici XML | nu | nu |
| 422 | `idempotency_key_reused` | aceeași `Idempotency-Key` cu alt corp | nu | nu |
| 500 | `storage_error` | Cassandra a respins **această** cerere (problemă a cererii, nu a nodului) | nu | nu |
| 503 | `storage_unavailable` | Cassandra indisponibilă sau timeout | **necunoscut** pentru scrieri | da, doar pentru operații idempotente |
| 503 | `timeout` | `DW_STORE_TIMEOUT` depășit sau clientul a renunțat | nu, dacă s-a oprit înainte de commit | da |
| 503 | `unavailable` | eșec injectat (`fail=true`) | nu | da |

DW verifică `ctx` înainte de commit: o cerere la care proxy-ul a renunțat înainte de scriere nu produce efect (`TestCancelledWriteIsNotApplied`). Dacă scrierea era deja trimisă la Cassandra, ea se poate aplica după abandon. Acest caz e tratat de proxy prin invalidarea întârziată (`failure-scenarios.md`, scenariul 2).

## Răspunsurile proxy-ului

| Status | `code` | Când |
|---|---|---|
| 502 | `bad_gateway` | conexiune refuzată/ruptă la toate încercările |
| 503 | `no_backend` | nicio replică în rotație (toate scoase de health check sau cu breaker deschis) |
| 504 | `upstream_timeout` | nicio încercare nu a răspuns în `UPSTREAM_TIMEOUT` |
| 413 | `body_too_large` | corp > 1 MiB |
| orice status al DW | (al DW) | proxy-ul îl transmite neschimbat |

**Rezultat necunoscut.** Pentru o scriere care primește 502/503/504 de la proxy, antetul `X-Outcome: unknown` spune că scrierea **poate fi aplicată**. Clientul poate:

- repeta un `PUT` (idempotent: dacă se aplicase, răspunsul e `200`, `X-Changed: false`);
- repeta un `POST` cu **aceeași** `Idempotency-Key` (se reia răspunsul inițial);
- citi resursa cu `Cache-Control: no-cache`.

## Ce numără proxy-ul împotriva unei replici

| Semnal | Breaker | Retry pe altă replică |
|---|---|---|
| fără răspuns (conexiune, timeout) | eșec | da, dacă cererea e idempotentă |
| 502, 503, 504 | eșec | da, dacă cererea e idempotentă |
| 500 | neutru (nu deschide, nu închide) | nu: e o eroare a cererii, nu a nodului |
| 2xx, 3xx, 4xx | succes | nu |

Regula pentru 500 a apărut la rularea pe Cassandra reală: o interogare respinsă de Cassandra deschidea breaker-ele tuturor replicilor și oprea tot serviciul (`TestRequestErrorDoesNotTakeReplicasOutOfRotation`).

Cereri idempotente (reîncercate): `GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS` și `POST` **numai** cu `Idempotency-Key`.
