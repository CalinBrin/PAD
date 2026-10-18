# Protocolul broker-ului

Transport: TCP. Format: un obiect JSON pe linie (frame). Un client poate fi producer (trimite `publish`) sau consumer (trimite `subscribe`, apoi `ack` / `nack`).

| Frame | Sens | Câmpuri | Semnificație |
|---|---|---|---|
| `publish` | client la broker | `topic`, `message` | Cere publicarea unui mesaj |
| `puback` | broker la client | `id`, `routed` | Mesajul este scris pe disc (fsync) în `routed` cozi. NU înseamnă că a fost procesat |
| `subscribe` | consumer la broker | `topic`, `group`, `name`, `prefetch` | Se alătură grupului (coada `topic.group`) |
| `suback` | broker la consumer | - | Abonare reușită |
| `deliver` | broker la consumer | `tag`, `message`, `attempt`, `redelivered` | Livrare. `tag` este unic pentru fiecare livrare. Mesajul rămâne in-flight până la ack |
| `ack` | consumer la broker | `tag`, `id` | Procesare confirmată pentru livrarea `tag`, mesajul este șters din coadă |
| `nack` | consumer la broker | `tag`, `id`, `reason`, `permanent` | Procesare eșuată. Cu `permanent=true` mesajul merge direct în DLQ, altfel broker-ul decide retry sau DLQ |
| `error` | broker la client | `id`, `reason` | Frame invalid sau eroare internă |

Reguli:

- Mesajul respectă `message-envelope.v1.schema.json`; pentru `type = OrderCreated` payload-ul respectă `order-created.v1.schema.json`.
- Un mesaj publicat pe un topic ajunge în câte o copie pentru fiecare grup. În interiorul unui grup consumatorii concurează.
- `ack` și `nack` identifică livrarea prin `tag`, nu prin `id`. Două mesaje publicate cu același `id` sunt două livrări cu tag-uri diferite și se confirmă separat. Un `ack`/`nack` fără `tag` primește `error`.
- Un mesaj fără `ack` în `ack-timeout`, sau a cărui conexiune de consumer s-a închis, este livrat din nou.
- Un mesaj retrimis după `nack` sau ack timeout nu este dat înapoi consumatorului care a eșuat, cât timp grupul are alt consumator conectat (chiar dacă acesta este ocupat, mesajul îl așteaptă). Doar dacă toți consumatorii grupului au eșuat mesajul, se reîncearcă pe unul dintre ei.
- Erorile permanente (`nack` cu `permanent=true`: payload invalid, schemă necunoscută, regulă de business încălcată) nu se reîncearcă: mesajul ajunge în DLQ după o singură livrare, cu `permanent: true`.
- După `max-attempts` livrări eșuate (erori tranzitorii), mesajul este mutat în coada DLQ `topic.group` (vizibilă la `GET /dlq?queue=topic.group` pe portul admin).
- JSON invalid: broker-ul trimite `error` cu motivul (`invalid JSON: ...`) și apoi închide conexiunea, pentru că fluxul nu mai poate fi resincronizat. JSON valid cu un câmp de tip greșit primește `error`, iar sesiunea continuă.
- Ordering: FIFO per coadă pentru livrările inițiale. Nu există ordine garantată între consumatorii concurenți ai aceluiași grup și nici pentru mesajele retrimise.
