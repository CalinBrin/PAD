# Garanții de livrare: at-least-once și at-most-once

## Diferența dintre trimitere și procesare confirmată

Sunt două confirmări independente:

- **puback** (producer către broker): broker-ul a scris mesajul în log și a făcut `fsync`. Producer-ul știe că mesajul nu se pierde dacă broker-ul pică. Nu știe nimic despre procesare. Endpoint-ul `POST /orders` răspunde `202 Accepted` exact în acest moment.
- **ack** (consumer către broker): consumer-ul a terminat de procesat. Abia acum broker-ul șterge mesajul din coadă.

Între cele două momente mesajul poate fi livrat de mai multe ori, întârziat sau trimis în DLQ.

## Ce garantează sistemul: at-least-once

Mesajul este considerat livrat doar după `ack`, deci consumer-ul trimite `ack` DUPĂ efectul local (`ack-mode=after`, valoarea implicită). Orice eveniment care împiedică `ack` produce o nouă livrare:

| Eveniment | Reacția broker-ului |
|---|---|
| `nack` explicit | retry cu backoff exponențial, apoi DLQ după `max-attempts` |
| consumer-ul nu răspunde în `ack-timeout` | tratat ca eșec, retry |
| conexiunea consumer-ului cade | mesajele in-flight revin imediat în coadă |
| broker-ul pică și repornește | mesajele neconfirmate se reîncarcă din log |

Consecința: mesajul NU se pierde, dar poate ajunge de mai multe ori. Aceasta este garanția at-least-once.

## Scenariul de eșec din laborator

Consumer-ul aplică efectul local (rezervă stoc), apoi cade înainte de `ack`.

1. Broker-ul vede doar conexiunea închisă. Nu poate ști dacă efectul s-a produs sau nu.
2. Alegerea sigură este să livreze din nou (`attempt=2`, `redelivered=true`).
3. Fără protecție, efectul ar fi aplicat de două ori (testul `WithoutDedup_EffectAppliedTwice` arată duplicatul: 2 efecte).
4. Cu deduplicare, consumer-ul găsește `message_id` în fișierul de efecte, sare peste efect și trimite `ack` (testul `WithDedup_EffectAppliedOnce`: 1 efect).

Efectul și cheia de deduplicare sunt aceeași scriere (o linie în `effects.jsonl` care conține `message_id`). Dacă ar fi două scrieri separate, un crash între ele ar reintroduce fereastra de duplicat.

Concluzie: transportul este at-least-once, iar efectul observabil devine exactly-once prin consumer idempotent (at-least-once plus deduplicare). Broker-ul singur nu poate oferi exactly-once.

## Cum arată at-most-once

Dacă `ack` se trimite ÎNAINTE de procesare (`-ack-mode before`), broker-ul șterge mesajul imediat. Dacă consumer-ul cade după `ack` și înainte de efect, mesajul este pierdut definitiv: nu se mai livrează, nu apare în DLQ. Testul `TestAckBeforeProcess_CrashLosesMessage_AtMostOnce` demonstrează exact acest caz (0 efecte, coadă goală).

| | ack după efect | ack înainte de efect |
|---|---|---|
| Garanție | at-least-once | at-most-once |
| Crash între ack și efect | nu există această fereastră | mesaj pierdut |
| Crash între efect și ack | duplicat (tratat prin dedup) | nu există această fereastră |
| Potrivit pentru | comenzi, plăți, stoc | notificări, metrici unde pierderea e acceptabilă |

## Ordering declarat

- FIFO per coadă pentru primele livrări (inclusiv după restart, testul `Persistence...SurviveBrokerRestart`).
- Mesajul retrimis revine pe poziția lui inițială în coadă (ordonare după numărul de secvență), nu la coadă.
- NU există ordine între consumatorii concurenți ai aceluiași grup (`-workers 2` sau mai multe procese): doi consumatori procesează în paralel, deci ordinea de terminare nu e garantată.
- NU există ordine între cozi (grupuri) diferite.
- Dacă o aplicație are nevoie de ordine strictă, folosește un singur consumer per grup cu `prefetch=1`.

## Limite cunoscute

- Contorul de încercări este în memorie: după restartul broker-ului revine la 0 (un mesaj otrăvit mai primește încă `max-attempts` încercări).
- Dacă broker-ul scrie `ack` pe disc după ce a eșuat `fsync`, mesajul poate fi livrat din nou după restart (tot at-least-once).
- Un consumer lent care trimite `ack` după `ack-timeout` este ignorat (mesajul a fost deja redat altcuiva): alt motiv pentru care consumer-ii trebuie să fie idempotenți.
- Deduplicarea este locală unui consumer (fișierul lui de efecte). Două procese cu fișiere diferite nu își văd reciproc efectele.
- Mesajele publicate pe un topic fără nicio coadă legată sunt aruncate (`puback` cu `routed=0`). De aceea cozile se pre-declară cu `-queues`.
