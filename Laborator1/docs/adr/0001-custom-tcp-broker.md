# ADR 0001: Broker didactic propriu, protocol JSON pe TCP

Status: acceptat

## Context

Laboratorul cere un broker real sau didactic minimal, cu publisheri și subscriberi dinamici, persistență și retransmitere. Baremul (nota 9) cere tratarea erorilor pe partea de broker, deci comportamentul broker-ului trebuie să fie vizibil și testabil.

## Decizie

Implementăm broker-ul în Go, fără dependențe externe. Protocolul este un obiect JSON pe linie, peste TCP. Un client este producer sau consumer după primul frame trimis.

## Motive

- TCP real permite deconectări neașteptate reale (testele taie conexiunea la jumătatea unui frame).
- JSON pe linie se depanează cu `nc` și se citește în loguri.
- Zero dependențe înseamnă că `go test ./...` rulează oriunde.

## Consecințe

- Performanța nu este un obiectiv (un `fsync` per publish).
- Nu există autentificare, TLS sau clustering.
- Mapare pe structura repo-ului: `integration-service` este broker-ul, `order-service` și `api` sunt producer-ul, `consumers` sunt abonații.
