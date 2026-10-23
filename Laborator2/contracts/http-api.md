# API HTTP (order-service, port 8080)

Toate răspunsurile sunt JSON și conțin headerul `X-Correlation-ID`.

## POST /orders

Headere:

| Header | Obligatoriu | Sens |
|---|---|---|
| `Idempotency-Key` | da (1..128 caractere) | identificatorul stabil al cererii; aceeași cheie înseamnă aceeași comandă |
| `X-Correlation-ID` | nu | se generează dacă lipsește și se propagă în toate componentele |

Corp:

```json
{"customer_id": "c1", "amount_cents": 12345, "currency": "EUR"}
```

`amount_cents` este întreg > 0 (sume în bani, fără virgulă mobilă). `currency` este opțional (implicit `MDL`), format ISO 4217 cu 3 litere mari. Câmpurile necunoscute sunt ignorate (cititor tolerant).

| Răspuns | Când |
|---|---|
| `202` cu `status=published` | comanda salvată, evenimentul salvat de broker |
| `202` cu `status=pending` | comanda salvată, rezultatul publicării necunoscut sau brokerul indisponibil; outbox-ul o republică |
| `200` + `Idempotent-Replayed: true` | aceeași cheie, aceeași cerere: se întoarce comanda existentă, nu se creează alta |
| `400` | cheie lipsă, JSON invalid, câmp invalid |
| `409` | aceeași cheie, altă cerere |
| `502` cu `status=failed` | brokerul a refuzat definitiv evenimentul (de exemplu nicio coadă pentru topic) |
| `503` | comanda nu a putut fi salvată |

`202` înseamnă „înregistrat”, nu „procesat”: plata și rezervarea stocului se fac asincron.

## GET /orders/{id}

`200` cu starea comenzii (`pending`, `published`, `failed`), `404` dacă nu există.

## GET /healthz, GET /readyz

`/healthz`: procesul rulează (mereu `200`). `/readyz`: `200` cu `status=ready`, sau `status=degraded` când brokerul este temporar indisponibil (comenzile se acceptă în continuare, prin outbox), `503` când comenzile nu pot fi salvate.
