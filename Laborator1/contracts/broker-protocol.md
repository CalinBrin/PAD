# Protocolul broker-ului

Transport: TCP. Format: un obiect JSON pe linie (frame). Un client poate fi producer (trimite `publish`) sau consumer (trimite `subscribe`, apoi `ack` / `nack`).

| Frame | Sens | Câmpuri | Semnificație |
|---|---|---|---|
| `publish` | client la broker | `topic`, `message` | Cere publicarea unui mesaj |
| `puback` | broker la client | `id`, `routed` | Mesajul este scris pe disc (fsync) în `routed` cozi. NU înseamnă că a fost procesat |
| `subscribe` | consumer la broker | `topic`, `group`, `name`, `prefetch` | Se alătură grupului (coada `topic.group`) |
| `suback` | broker la consumer | - | Abonare reușită |
| `deliver` | broker la consumer | `message`, `attempt`, `redelivered` | Livrare. Mesajul rămâne in-flight până la ack |
| `ack` | consumer la broker | `id` | Procesare confirmată, mesajul este șters din coadă |
| `nack` | consumer la broker | `id`, `reason` | Procesare eșuată, broker-ul decide retry sau DLQ |
| `error` | broker la client | `id`, `reason` | Frame invalid sau eroare internă |

Reguli:

- Mesajul respectă `message-envelope.v1.schema.json`; pentru `type = OrderCreated` payload-ul respectă `order-created.v1.schema.json`.
- Un mesaj publicat pe un topic ajunge în câte o copie pentru fiecare grup. În interiorul unui grup consumatorii concurează.
- Un mesaj fără `ack` în `ack-timeout`, sau a cărui conexiune de consumer s-a închis, este livrat din nou.
- După `max-attempts` livrări eșuate, mesajul este mutat în coada DLQ `topic.group` (vizibilă la `GET /dlq?queue=topic.group` pe portul admin).
- Ordering: FIFO per coadă pentru livrările inițiale. Nu există ordine garantată între consumatorii concurenți ai aceluiași grup și nici pentru mesajele retrimise.
