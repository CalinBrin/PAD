# ADR 0002: Livrare at-least-once și consumer idempotent

Status: acceptat

## Context

Scenariul de eșec al laboratorului: consumer-ul cade după efectul local și înainte de ACK. Broker-ul nu poate distinge "efect aplicat" de "efect neaplicat".

## Decizie

- Broker-ul livrează at-least-once: mesajul rămâne in-flight până la ACK.
- Consumer-ul trimite ACK după efect (implicit) și este idempotent: cheia de deduplicare este `message.id`, salvată în aceeași scriere cu efectul.
- Modul `-ack-mode before` există doar pentru a demonstra at-most-once.
- Deduplicarea se face la consumer, nu la broker, pentru că doar consumer-ul știe ce efect a produs.

## Alternative respinse

- At-most-once ca mod implicit: pierde comenzi la un crash.
- Exactly-once în broker: necesită tranzacții distribuite între broker și efectul local, mult peste scopul laboratorului.

## Consecințe

- Duplicatele de livrare sunt normale și trebuie tratate de orice consumer nou.
- Fișierul de efecte crește monoton (fără curățare, acceptabil pentru laborator).
