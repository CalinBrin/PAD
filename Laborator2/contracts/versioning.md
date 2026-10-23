# Versionare și compatibilitate

## Unde sunt contractele

| Fișier | Ce descrie |
|---|---|
| `proto/broker/v1/broker.proto` | serviciul `Broker`, plicul `Envelope` |
| `proto/orders/v1/orders.proto` | payload-ul `orders.v1.OrderCreated` |
| `proto/payment/v1/payment.proto` | serviciul extern `Payment` |
| `http-api.md` | API-ul HTTP al order-service |
| `errors.md` | semantica erorilor (face parte din contract) |

## Două niveluri de versiune

1. **Versiunea serviciului** este în pachetul proto: `broker.v1`, `payment.v1`. O schimbare incompatibilă (câmp redenumit, renumerotat, tip schimbat, RPC eliminat, semantica unei erori schimbată) se face într-un pachet nou, `broker.v2`, servit în paralel cu `v1` cât timp există clienți vechi.
2. **Versiunea schemei payload-ului** este în `Envelope.schema_version`, pentru fiecare `message_type`.

## Reguli pentru schimbări compatibile în aceeași versiune

- Numerele și tipurile câmpurilor nu se schimbă niciodată. Un câmp eliminat devine `reserved`.
- Se pot adăuga câmpuri noi. Un cititor vechi le ignoră (protobuf le păstrează ca „unknown fields”), un cititor nou trebuie să aibă o valoare implicită documentată pentru mesajele vechi care nu le au.
- Câmpurile noi nu pot deveni obligatorii pentru mesajele vechi.

## Exemplul din proiect: `orders.v1.OrderCreated`

| `schema_version` | Câmpuri | Producer | Consumatori |
|---|---|---|---|
| 1 | `order_id`, `customer_id`, `amount_cents` | (versiunea veche) | citit; `currency` lipsă devine `MDL` |
| 2 | + `currency` | versiunea curentă | citit |
| 3 sau mai mare | necunoscut | | refuzat ca eroare permanentă (DLQ) |

Consumatorii acceptă intervalul `[1, 2]` (`envelope.OrderCreatedMinSchema`..`OrderCreatedSchema`). Ordinea corectă de lansare a unei versiuni noi de schemă: întâi consumatorii care o înțeleg, apoi producer-ul care o scrie.

## Cum se verifică

`tests/contract`:

- `TestProtoFieldNumbersAndTypesAreStable`: tabelul „golden” cu numărul și tipul fiecărui câmp. O schimbare incompatibilă face testul să pice.
- `TestNewerPayloadIsReadByOlderConsumer`: un payload v2 citit cu descriptorul v1 (fără `currency`), fără eroare.
- `TestOlderPayloadIsReadByNewerConsumer`: un payload v1 citit de codul curent, cu `currency = MDL`.
- `TestUnsupportedSchemaVersionIsAPermanentError`: versiunea 3 și un tip necunoscut sunt refuzate.
- `TestPublishErrorSemantics`, `TestChargeErrorSemantics`, `TestHTTPAPIContract`: semantica erorilor, prin servere reale.
