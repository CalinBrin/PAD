# ADR 0001: Cassandra comună tuturor replicilor DW

Status: acceptat (lab3-v1.0). Decizia principală privind datele.

## Context

Etapa 2 cere mai multe noduri DW în spatele proxy-ului, iar proxy-ul trimite o cerere la oricare dintre ele. Orice replică trebuie deci să vadă aceleași date, inclusiv imediat după o scriere făcută de altă replică. Diagrama din enunț arată un `SyncService` între warehouse-uri. Recomandarea din enunț e o instanță Cassandra comună, „pentru evitarea sincronizării datelor”.

Remarca primită pe o soluție anterioară: „storage-ul rămâne un SQLite comun, valabil doar pe același host”.

## Decizie

- Toate replicile DW folosesc **același keyspace Cassandra** (`pad_lab3`) prin rețea. Replicile sunt fără stare și interschimbabile; nu există `SyncService`.
- Fiecare scriere este un **compare-and-set** (lightweight transaction, Paxos): `INSERT ... IF NOT EXISTS` pentru creare, `UPDATE ... IF version = ?` pentru modificare. La conflict, replica recitește și reîncearcă (`dw.Service.write`). Două replici care scriu concurent același angajat nu pierd nicio modificare (`TestCassandraConcurrentReplicasLoseNoUpdate`, pe Cassandra reală).
- Citirile și scrierile folosesc consistența `QUORUM`, iar LWT-urile `LOCAL_SERIAL`. Cu un singur nod, `QUORUM` = 1, dar configurația rămâne corectă la 3 noduri cu `RF=3`.
- Feed-ul de modificări folosește un index SAI pe `updated_at` (Cassandra 5), fără scanarea tabelului.
- Schema e creată o singură dată de serviciul `schema` (`datawarehouse -migrate`), nu concurent de replici: modificările concurente de schemă în Cassandra produc dezacorduri de schemă.

## Alternative comparate

| Criteriu | **Cassandra comună** (ales) | SQLite comun (fișier pe volum) | PostgreSQL comun | O bază per replică + SyncService |
|---|---|---|---|---|
| Replici pe host-uri diferite | da (rețea) | **nu**: un fișier pe un singur host | da | da |
| Sincronizare între replici | nu e nevoie | nu e nevoie | nu e nevoie | da, cod propriu, consistență eventuală |
| Scrieri concurente pe același rând | LWT (`IF version = ?`) | blocare pe fișier, un singur scriitor | `UPDATE ... WHERE version = ?` / tranzacții | conflicte rezolvate de SyncService |
| Scalare orizontală a stocării | da (noduri, RF) | nu | verticală (replici de citire) | da, dar cu divergențe |
| Punct unic de cădere | nu, cu ≥ 3 noduri și RF=3 | da | da (fără failover) | nu |
| Recomandat în enunț | da | nu | nu | parțial (diagrama) |
| Cost în laborator | ~1 GB RAM, ~60 s la pornire | zero | mic | mare (cod de sincronizare) |
| Interogări flexibile (offset, count) | slabe: fără `OFFSET`, `COUNT(*)` scanează | bune | foarte bune | depinde |

PostgreSQL ar fi fost mai simplu pentru paginare și numărare, dar nu scalează orizontal și nu e recomandarea enunțului. SQLite comun e exclus: nu funcționează cu replici pe host-uri diferite. SyncService adaugă exact problema pe care enunțul cere să o evităm.

## Consecințe

- Replicile DW pot fi adăugate sau oprite fără nicio migrare de date. Proxy-ul le tratează identic.
- Paginarea (`offset`/`limit`) citește și sare rândurile dinaintea `offset`, în ordinea token-urilor (stabilă, dar nu alfabetică). `total` folosește `COUNT(*)`, care scanează. Sunt acceptabile la sute sau mii de angajați, nu la milioane. Pentru volume mari: paginare după `paging state` (token opac) în loc de `offset`.
- LWT costă 4 runde Paxos. Scrierile sunt rare față de citiri (care trec mai ales prin cache), deci costul e acceptabil.

## Limita cunoscută

În Docker rulează **un singur nod** Cassandra (RF=1), din cauza memoriei. Cassandra rămâne astfel punct unic de cădere. Când cade, toate replicile răspund `503 storage_unavailable`, `/readyz` pică pe toate și proxy-ul servește doar ce are în cache. Pentru producție: 3 noduri, `CASSANDRA_RF=3`, `QUORUM`.

Cassandra 5.0.2 are un bug în indexul SAI pentru o limită de interval înaintea epocii (anul 1, timpul zero din Go): `NullPointerException`. Prima citire a feed-ului (fără cursor) citește deci tot tabelul, fără `WHERE`. Bug-ul a fost găsit la rularea pe Cassandra reală, nu în testele cu stocarea în memorie.
