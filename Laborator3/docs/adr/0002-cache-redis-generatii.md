# ADR 0002: Cache în Redis, invalidat prin contoare de generație

Status: acceptat (lab3-v1.0)

## Context

Proxy-ul ține în cache răspunsurile `GET` ca să micșoreze timpul de răspuns și numărul de cereri la DW. Problema grea este **invalidarea corectă**. Remarcile primite pe o soluție anterioară:

1. „Un GET început înainte de un PUT poate pune în cache răspunsul vechi după ce PUT-ul a invalidat. Valoarea veche rămâne până la TTL. Nu există contor de generație și nu există test pentru asta.”
2. „Un PUT care expiră, dar a fost aplicat, nu invalidează deloc.”

Problema 1 apare cu orice schemă „șterge cheia la scriere”: ștergerea se face înaintea scrierii în cache a GET-ului lent.

## Decizie

**Unde:** cache-ul e în Redis (recomandarea enunțului), nu în memoria proxy-ului. E comun tuturor replicilor de proxy, supraviețuiește repornirii proxy-ului și are TTL nativ.

**Cheia** unei intrări: ruta, query-ul canonic (sortat, fără `format`) și formatul negociat (`json`/`xml`). Două spelling-uri ale lui `Accept` care dau același format împart intrarea; JSON și XML sunt intrări separate.

**Contoarele de generație.** Fiecare intrare depinde de câteva contoare Redis:

| Răspuns | Depinde de |
|---|---|
| `GET /employees/a`, `GET /employee/?id=a` | `gen:employees:all`, `gen:employees:item:a` |
| `GET /employees?offset=&limit=` | `gen:employees:all`, `gen:employees:lists` |

| Scriere | Incrementează |
|---|---|
| `PUT`/`POST /employees/a` | `gen:employees:item:a`, `gen:employees:lists` |
| `PUT /employee/` cu `id` doar în corp | idem (proxy-ul citește `id` din corpul JSON/XML) |
| scriere al cărei angajat nu se poate determina | `gen:employees:all` (tot) |

**Citirea (HIT/MISS).** Un singur `MGET` aduce intrarea și contoarele ei. Intrarea se folosește numai dacă valorile salvate în ea sunt egale cu cele curente. La MISS, proxy-ul trimite cererea la DW și salvează răspunsul **cu valorile contoarelor citite înainte de cerere**.

**Scrierea.** Proxy-ul incrementează contoarele:

1. înainte de a trimite scrierea;
2. după ce scrierea se termină, **oricare ar fi rezultatul** (succes, eroare, timeout), deci și un PUT expirat dar aplicat invalidează (remarca 2);
3. dacă rezultatul e necunoscut (fără răspuns sau 5xx), încă o dată după `WRITE_GRACE` (3 s). Până atunci, o scriere care era deja în drum spre Cassandra când proxy-ul a renunțat s-a terminat sigur (`DW_STORE_TIMEOUT` = 2 s).

**De ce rezolvă cursa (remarca 1).** GET-ul lent a citit contoarele `[0,14]` înainte de PUT. PUT-ul le duce la `[0,16]`. Răspunsul vechi ajunge în Redis cu `[0,14]` și nu se mai potrivește niciodată, indiferent când e scris. Demonstrat în `docs/evidence/demo-race-001.log` (sistemul real) și în `TestSlowGetStartedBeforePut_DoesNotCacheStaleValue`.

**Redis indisponibil.** După prima eroare, Redis e marcat căzut și **nicio cerere nu-l mai apelează**; o singură goroutină îl verifică la 500 ms. Cererile trec direct la DW (`X-Cache: BYPASS`). Invalidările care nu s-au putut scrie sunt reținute și scrise **înainte** ca cache-ul să fie folosit din nou: dacă Redis revine cu datele vechi (partiție de rețea, nu repornire), intrările vechi nu sunt servite (`TestRedisDown_..._LostInvalidationIsReplayed`, `demo-redis-down-001.log`).

**Evicție.** Redis rulează cu `maxmemory-policy volatile-lru`: evacuează doar cheile cu TTL (intrările de cache, înregistrările cererilor), **niciodată contoarele** (fără TTL). Cu `allkeys-lru`, un contor evacuat ar reveni la 0 și o intrare veche cu contorul 0 ar redeveni validă.

## Alternative comparate

| Variantă | Cursa GET lent / PUT | PUT expirat dar aplicat | Rata de HIT la 2% scrieri | Runde Redis la HIT | Probleme |
|---|---|---|---|---|---|
| Doar TTL, fără invalidare | valoare veche până la TTL | idem | mare | 1 | date vechi garantat |
| `DEL` la scriere | **valoare veche până la TTL** (remarca 1) | fără invalidare dacă `DEL` e doar pe succes | mare | 1 | cursa |
| `DEL` + „write lock” / lease pe cheie | rezolvată | rezolvată dacă lease-ul expiră | mare | 2–3 | complex, blocaje la lease pierdut |
| O generație pe colecție, în cheie | rezolvată | rezolvată | **41%** (măsurat) | 2 secvențiale | orice scriere golește tot; în benchmark, cache-ul era **mai lent** decât fără cache |
| **Contoare per angajat + liste, salvate în intrare** (ales) | rezolvată | rezolvată | **87%** (măsurat) | 1 (`MGET`) | un contor per angajat scris (fără TTL) |
| Invalidare push de la DW (pub/sub) | depinde de ordinea mesajelor | rezolvată dacă DW publică după commit | mare | 1 | DW trebuie să cunoască proxy-ul; mesaj pierdut = valoare veche |

Varianta „o generație pe colecție” a fost implementată prima. Benchmark-ul a arătat că e mai lentă decât lipsa cache-ului (7300 vs 10 100 req/s), așa că am înlocuit-o. Cifrele: `docs/evidence/README.md`.

## Consecințe

- HIT = un `MGET`; MISS = un `MGET` + cererea la DW + un `SET`.
- Valoarea veche servită este mărginită: `0` după o scriere încheiată prin proxy; cel mult `WRITE_GRACE` după o scriere cu rezultat necunoscut.
- TTL-ul (`min(CACHE_TTL, max-age al DW)`, 30 s) nu mai are rol de corectitudine, doar de memorie.

## Limite cunoscute

- O scriere făcută **ocolind proxy-ul** (direct la DW sau în Cassandra) nu invalidează: valoarea veche poate rămâne până la TTL. În Docker, porturile DW nu sunt publicate, tocmai de aceea.
- Dacă procesul proxy cade între commit-ul în DW și incrementarea de după scriere, intrarea veche poate rămâne până la TTL.
- Cu mai multe replici de proxy și o partiție care izolează **doar una** de Redis, celelalte pot servi valoarea veche până când replica izolată își scrie invalidările.
