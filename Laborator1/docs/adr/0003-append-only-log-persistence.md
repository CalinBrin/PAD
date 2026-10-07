# ADR 0003: Persistență prin log append-only cu replay și compactare

Status: acceptat

## Context

Nota 9 cere stocarea persistentă a mesajelor și retransmiterea celor nelivrate.

## Decizie

Broker-ul scrie în `broker.log` câte o înregistrare JSON pe linie: `declare`, `enqueue`, `ack`, `dlq`. Fiecare scriere este urmată de `fsync`, iar `puback` se trimite abia după aceea. La pornire log-ul este reluat (replay): mesajele cu `enqueue` și fără `ack`/`dlq` revin în coadă în ordinea inițială. Apoi fișierul este rescris compact, doar cu starea vie (scriere într-un fișier temporar, apoi `rename`).

## Motive

- Simplu de înțeles și de verificat cu `cat`.
- O linie ruptă la final (crash în timpul scrierii) este ignorată la replay, deci nu blochează pornirea.
- Un singur `enqueue` acoperă toate cozile unui topic, deci un singur `fsync` per publish.

## Consecințe

- Contorul de încercări nu este persistat (revine la 0 după restart).
- Log-ul crește între restarturi (compactarea se face doar la pornire).
- Un singur mutex global protejează starea broker-ului: simplu și corect, dar nu scalează pe mai multe nuclee.
