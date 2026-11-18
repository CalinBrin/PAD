# Contractul HTTP

Clienții și nodurile sursă vorbesc **numai cu proxy-ul** (`:8080`). Proxy-ul este transparent: aceleași rute, statusuri, corpuri și antete ca ale unei replici DW, plus antetele lui (mai jos). Replicile DW nu publică portul de date în afara rețelei Docker.

Testele care fixează contractul: `tests/contract` (aceleași verificări rulate direct pe o replică și prin proxy).

## Reprezentarea datelor (JSON / XML)

Clientul alege formatul:

1. `?format=json` sau `?format=xml` (are prioritate);
2. altfel antetul `Accept`: `application/json`, `text/json`, `application/xml`, `text/xml`, `*/*`, `application/*`, cu valori `q`. La `q` egal câștigă primul tip listat. Fără `Accept`, răspunsul e JSON;
3. nimic acceptabil (de exemplu `Accept: text/html`) → `406 not_acceptable`.

Corpul cererilor `PUT`/`POST` se citește după `Content-Type` (`application/json` sau `application/xml`; lipsă = JSON). Orice alt tip dă `415`. Limita corpului e 1 MiB. Câmpurile JSON necunoscute dau `400`.

### Angajatul

JSON:

```json
{"id":"emp-j-005","name":"Ana Rusu","department":"IT","position":"Engineer",
 "email":"ana@example.md","salary":12000,"version":3,
 "updated_at":"2026-11-18T20:42:07.398Z","updated_by":"source-json@host"}
```

XML:

```xml
<employee id="emp-j-005">
  <name>Ana Rusu</name>
  <department>IT</department>
  <position>Engineer</position>
  <email>ana@example.md</email>
  <salary>12000</salary>
  <version>3</version>
  <updated_at>2026-11-18T20:42:07.398Z</updated_at>
  <updated_by>source-json@host</updated_by>
</employee>
```

| Câmp | Tip | Cine îl scrie | Reguli |
|---|---|---|---|
| `id` | string | clientul (URL sau corp) | `^[A-Za-z0-9_-]{1,64}$` |
| `name` | string | clientul | obligatoriu, max 200 de caractere |
| `department`, `position` | string | clientul | max 200 |
| `email` | string | clientul | gol sau conține `@` |
| `salary` | întreg | clientul | `>= 0` |
| `version` | întreg | DW | crește cu 1 la fiecare modificare reală |
| `updated_at` | timestamp RFC 3339, UTC, ms | DW | momentul modificării |
| `updated_by` | string | DW, din antetul `From` | `anonymous` dacă lipsește |

Schemele formale: `employee.schema.json` (JSON Schema) și `employee.xsd` (XML Schema).

## Rute

| Metodă și rută | Cine | Ce face | Răspuns |
|---|---|---|---|
| `GET /employees/{id}` | client | un angajat | `200`, `404` |
| `GET /employee/?id={id}` | client | alias din enunț | ca mai sus |
| `GET /employees?offset=&limit=` (și `/employees/`) | client | o pagină (`offset` ≥ 0, implicit 0; `limit` 1..100, implicit 10) | `200` |
| `PUT /employees/{id}` | nod sursă | creează sau înlocuiește | `201` + `Location` (creat), `200` (actualizat sau neschimbat) |
| `PUT /employee/` | nod sursă | alias din enunț; `id` vine din corp | ca mai sus |
| `POST /employees/{id}` (și `/employee/?id=`) | client | modificare parțială (patch) | `200`, `404` |
| `GET /updates/employees?since={cursor}` (și `/update/employees/`) | nod sursă | modificările de după cursor (pull) | `200`, `304` |
| `GET /healthz` | Docker | procesul trăiește | `200` |
| `GET /readyz` | proxy (health check activ), Docker | replica poate servi (Cassandra răspunde) | `200` / `503` |

Pagina:

```json
{"offset":1,"limit":4,"total":40,"items":[ ... ]}
```
```xml
<employees offset="1" limit="4" total="40"><employee id="..."> ... </employee></employees>
```

Ordinea paginilor este stabilă cât timp datele nu se schimbă (ordinea token-urilor Cassandra). Nu e alfabetică.

### PUT: idempotent

- Același corp trimis din nou nu schimbă nimic: `200`, `X-Changed: false`, aceeași `version`. De aceea un retry după un răspuns pierdut e sigur.
- `If-Match: "v3-json"` (sau `"v3"`) aplică scrierea doar dacă versiunea curentă este 3; altfel `412`. Fără `If-Match`, ultima scriere câștigă.
- Două replici care scriu simultan același angajat nu pierd nicio modificare: fiecare scriere e un compare-and-set în Cassandra (`IF version = ?`, Paxos).

### POST: patch cu valori absolute

Corpul conține doar câmpurile modificate: `{"salary":15000}` sau `<employee><salary>15000</salary></employee>`. Valorile sunt absolute (nu incremente), deci același patch aplicat de două ori dă aceeași stare.

`Idempotency-Key: <cheie>` (recomandat): DW păstrează 24 h răspunsul pentru cheie. O repetare primește același răspuns cu `Idempotent-Replayed: true`, fără efect nou. Aceeași cheie cu alt corp dă `422`. **Proxy-ul reîncearcă un POST numai dacă are `Idempotency-Key`.**

### GET /updates/employees: pull cu cursor

```json
{"cursor":"2026-11-18T20:42:07.504Z","items":[{"id":"emp-x-003","version":4, ...}]}
```

- Prima cerere fără `since` întoarce tot. Următoarele trimit `since=<cursor>` din răspunsul anterior.
- Răspunsul **retrimite și ultimele `DW_FEED_OVERLAP` (5 s) dinaintea cursorului**: o scriere confirmată târziu, sau ștampilată de o replică cu ceasul puțin în urmă, nu se pierde. Cititorul aplică doar versiunile mai mari decât cele pe care le are (`src/source-node/source`).
- Alternativ, antetul `If-Modified-Since` (ca în enunț) ține loc de cursor. Dacă nu e nicio modificare în fereastră, răspunsul e `304`.
- Este o citire pură: nu închiriază și nu marchează nimic. Repetarea ei (retry-ul proxy-ului) nu are efect (ADR 0005).
- `Cache-Control: no-store`: proxy-ul nu o pune niciodată în cache.

## Antete

| Antet | Direcție | Sens |
|---|---|---|
| `X-Correlation-ID` | cerere și răspuns | dacă lipsește, îl generează proxy-ul; apare în logurile tuturor componentelor |
| `From` | cerere | numele nodului (`source-json@host`), devine `updated_by` |
| `ETag` | răspuns | `"v<version>-<format>"`; `If-None-Match` dă `304` |
| `Last-Modified` | răspuns | `updated_at` |
| `Cache-Control` | răspuns DW | `public, max-age=30` la citiri, `no-store` la scrieri și la feed |
| `Cache-Control: no-cache` | cerere | clientul cere o valoare proaspătă: proxy-ul nu citește din cache (dar îl reîmprospătează) |
| `Cache-Control: no-store` | cerere | proxy-ul nici nu citește, nici nu scrie cache-ul |
| `Vary: Accept` | răspuns | JSON și XML sunt intrări separate în cache |
| `X-Served-By` | răspuns DW | replica care a calculat răspunsul (și pentru un HIT: cea care l-a calculat inițial) |
| `X-Changed` | răspuns la PUT/POST | `true` dacă s-a creat o versiune nouă |
| `Idempotent-Replayed` | răspuns la POST | `true` pentru o repetare cu aceeași `Idempotency-Key` |

Antetele adăugate de proxy:

| Antet | Valori |
|---|---|
| `X-Cache` | `HIT`, `MISS`, `BYPASS` (cache oprit, Redis indisponibil, `no-store`, rută necache-uibilă), `INVALIDATE` (scriere) |
| `X-Upstream` | replica care a răspuns (lipsește la `HIT`) |
| `X-Attempts` | numărul de încercări (retry) |
| `X-Outcome: unknown` | o scriere fără răspuns de la DW: poate fi aplicată sau nu (vezi `errors.md`) |
| `Age` | la `HIT`: secundele de când e în cache |
| `Via` | `1.1 pad-proxy` |
| `X-Client-ID` (cerere) | opțional, identificatorul clientului în registrul proxy-ului |

## Porturile de administrare (numai pe 127.0.0.1)

Proxy, `:8081`:

| Rută | Ce face |
|---|---|
| `GET /healthz`, `GET /readyz` | proxy-ul trăiește / are cel puțin o replică în rotație |
| `GET /proxy/stats` | contoare: cache, retry, conexiuni noi și reutilizate spre DW, starea fiecărei replici (healthy, breaker, cereri) |
| `GET /proxy/clients` | registrul clienților (din Redis, comun tuturor replicilor de proxy) |
| `GET /proxy/requests/{correlation-id}` | înregistrarea unei cereri (10 minute) |
| `POST /proxy/cache?enabled=true\|false` | pornește/oprește cache-ul la rulare (benchmark) |
| `POST /proxy/balancer?algorithm=round_robin\|least_conn` | schimbă algoritmul la rulare |

Replicile DW, `:9101`, `:9102`, `:9103` (injectarea eșecurilor):

`POST /admin/faults?latency=200ms&read_delay=1s&write_delay=3s&fail=true&ready=false`, `?reset=1`; `GET /admin/faults`.
