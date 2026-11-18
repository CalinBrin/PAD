# ADR 0005: Nodurile sursă preiau modificările prin pull cu cursor

Status: acceptat (lab3-v1.0)

## Context

Enunțul: nodurile de stocare trimit date cu `PUT`; clienții trimit modificări cu `POST`; „nodul sursă, prin interogări repetate, să ceară eventualele modificări (`GET update/employees/` — metoda pull)”. PUSH-ul (DW inițiază o cerere spre sursă) e opțional.

Remarca primită pe o soluție anterioară: „GET .../deliveries este reîncercat, dar închiriază livrări. Dacă încercarea expiră după ce DW a închiriat, receiverul nu primește nimic, iar livrările revin abia după lease-ul de 30 s. După 5 astfel de cazuri ajung în DLQ doar din cauza timeout-urilor proxy-ului.” Un GET cu efecte e reîncercat ca și cum ar fi sigur.

## Decizie

- **Pull cu cursor, nu cu lease.** `GET /updates/employees?since=<cursor>` întoarce angajații modificați după cursor și un cursor nou. Starea citirii (cursorul) o ține **cititorul**, nu DW. Cererea e o citire pură: nu închiriază, nu marchează, nu numără încercări.
- **Repetarea e inofensivă.** Un retry al proxy-ului după timeout întoarce aceleași date. Dacă răspunsul se pierde, sursa nu-și avansează cursorul și cere din nou la următorul poll. Nu există lease de așteptat și nicio DLQ (`TestChangeFeedRetryHasNoSideEffect`).
- **Fereastra de suprapunere.** DW retrimite și ultimele `DW_FEED_OVERLAP` (5 s) dinaintea cursorului. O scriere ștampilată cu `updated_at` mai mic decât cursorul (confirmată târziu, sau de o replică cu ceasul puțin în urmă) apare totuși. Cititorul aplică doar `version` > versiunea locală, deci duplicatele sunt ignorate (`TestChangeFeedResendsOverlapWindow`, `TestSourceNodesPublishAndPullClientModification`).
- **Feed de stare, nu de evenimente.** Se întoarce ultima stare a fiecărui angajat modificat, nu fiecare modificare. Sursa are nevoie de starea curentă; două modificări rapide ale aceluiași angajat ajung ca una singură.
- Formatul cererii ține de sursă: `source-json` cere JSON, `source-xml` cere XML. `If-Modified-Since` e acceptat ca alternativă la cursor, ca în exemplul din enunț.

## Alternative comparate

| Variantă | Retry sigur? | Răspuns pierdut | Stare în DW per cititor | Cuplare |
|---|---|---|---|---|
| **Pull cu cursor ținut de cititor** (ales) | da, citire pură | se recere la următorul poll | niciuna | DW nu știe de surse |
| Pull cu lease (coadă: GET închiriază, apoi ack) | **nu**: retry-ul închiriază din nou sau pierde lease-ul (remarca) | livrarea revine după lease, încercări consumate, DLQ | lease-uri, încercări | DW gestionează livrări per cititor |
| Pull cu lease, făcut cu `POST` (ne-idempotent, fără retry) | nu se reîncearcă | idem, dar fără încercări consumate de proxy | idem | idem |
| PUSH (DW apelează sursa la modificare) | DW trebuie să reîncerce spre sursă | DW trebuie să rețină modificarea până reușește | coadă per sursă | DW cunoaște adresele surselor; sursa trebuie să fie server |
| Long polling | da, dacă e cu cursor | idem pull | conexiuni ținute deschise | |

Pull-ul cu cursor e singura variantă în care DW rămâne fără stare per cititor. Asta contează aici: replicile DW sunt interschimbabile (ADR 0001), iar o cerere de feed poate ajunge la oricare.

## Consecințe

- Latența propagării = intervalul de poll (`SOURCE_POLL_INTERVAL`, 2 s).
- Fiecare poll retrimite fereastra de 5 s. Costul e mic: doar angajații modificați în ultimele 5 s dinaintea cursorului.
- Feed-ul nu e pus în cache de proxy (`Cache-Control: no-store`): altfel o sursă ar primi un cursor vechi.

## Limita cunoscută

Ipoteza ferestrei de suprapunere: diferența de ceas dintre replicile DW plus întârzierea confirmării unei scrieri sunt sub 5 s. Pe același host Docker, ceasul e comun. `DW_STORE_TIMEOUT` (2 s) limitează durata unei scrieri. Dacă ipoteza e încălcată (de exemplu, o replică pe alt host cu ceasul cu 10 s în urmă), o modificare poate fi sărită. Soluția completă ar fi un număr de secvență global atribuit atomic cu scrierea, pe care Cassandra nu îl oferă fără o partiție unică (punct fierbinte).

O ștergere nu apare în feed: contractul nu are `DELETE`.
