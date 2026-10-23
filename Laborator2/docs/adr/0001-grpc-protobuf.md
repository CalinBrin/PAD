# ADR 0001: gRPC cu protobuf în locul protocolului JSON pe socket

Status: acceptat (lab2-v1.0). Decizia principală a laboratorului 2.

## Context

În laboratorul 1 componentele comunicau printr-un protocol propriu: obiecte JSON pe linie, peste TCP. Framing-ul, corelarea răspunsurilor, deadline-urile și codurile de eroare erau scrise de mână. Laboratorul 2 cere un RPC, o reprezentare a datelor (proto/avro/thrift) și tratarea explicită a deadline-urilor, anulării, versionării și a rezultatului ambiguu.

## Decizie

- Toate apelurile dintre componente sunt gRPC, cu contracte în `contracts/proto` (`broker.v1`, `orders.v1`, `payment.v1`).
- `Publish` este unar. Consumul este un flux bidirecțional `Consume`: clientul trimite `subscribe`, apoi `ack`/`nack`; brokerul trimite livrările. Închiderea fluxului returnează livrările neconfirmate în coadă.
- Fiecare apel are un deadline explicit, setat de client per încercare și propagat automat de gRPC la server (`grpc-timeout`). Serverele verifică `ctx` înainte de commit, deci un apel abandonat nu produce efect.
- Erorile sunt coduri gRPC cu sens fix (`contracts/errors.md`); retry-ul se decide după cod, prin politica din `rpcx`.
- Correlation ID-ul circulă în metadata `x-correlation-id`, prin interceptori pe client și pe server, care scriu și câte o linie de log per apel (metodă, cod, durată).
- Nucleul brokerului (cozi, retry, DLQ, persistență) este independent de transport; pachetul `server` este doar adaptorul gRPC.

## Alternative

- **Apache Thrift**: RPC și IDL comparabile, dar fără fluxuri bidirecționale, cu deadline-uri gestionate manual și mai puțin folosit în Go.
- **JSON pe HTTP/REST**: simplu de depanat, dar fără contract tipizat verificat la compilare și fără streaming nativ pentru livrare push.
- **Păstrarea socketurilor din lab 1**: nu răspunde cerinței de RPC.

## Consecințe

- Tipurile sunt generate din `.proto`, iar numerele câmpurilor sunt contractul pe fir. Un test „golden” le fixează.
- Livrarea este push pe flux, fără polling: latența nu depinde de un interval de interogare.
- Codul generat necesită `protoc` și pluginuri; generarea este automatizată în Docker (ADR 0003).

## Limita cunoscută

Conexiunile nu sunt criptate (fără TLS), iar tokenul serviciului de plăți circulă în clar în rețeaua Docker. Brokerul este o singură instanță (single point of failure), fără replicare.
