# ADR 0004: Tag de livrare, retry pe alt consumator, erori permanente

Status: acceptat (lab1-v1.1, după evaluare)

## Context

La evaluare s-au găsit patru probleme:

1. Livrările in-flight erau indexate după `message.id`. Dacă același id era publicat de două ori (de exemplu un producer care reîncearcă după un `puback` pierdut), a doua livrare o suprascria pe prima, iar ACK-ul primului consumator era ignorat. Contorul lui de prefetch nu mai scădea, deci consumatorul nu mai primea nimic.
2. Retry-ul alegea următorul consumator liber, care putea fi chiar cel care tocmai eșuase. Mesajul putea ajunge în DLQ deși alt consumator al grupului era sănătos, doar ocupat.
3. La JSON invalid conexiunea se închidea fără nicio explicație pentru client.
4. Toate erorile erau reîncercate, inclusiv cele care nu se pot rezolva prin retry (sumă negativă, schemă necunoscută).

## Decizie

1. Fiecare livrare primește un `tag` unic (contor al broker-ului). `in-flight` este indexat după `tag`, iar `ack`/`nack` trimit `tag`. În log, un `ack` scoate o singură copie cu acel id, deci copiile duplicate se confirmă separat și după restart.
2. Fiecare mesaj ține minte consumatorii care l-au eșuat (`nack` sau ack timeout). La retry este ales un consumator care nu l-a eșuat; dacă acesta e ocupat, mesajul îl așteaptă, iar mesajele de după pot trece înainte. Doar când toți consumatorii conectați au eșuat mesajul, se reîncearcă pe unul dintre ei.
3. La `json.SyntaxError` broker-ul trimite `error` cu motivul, golește coada de scriere și abia apoi închide conexiunea. La un câmp de tip greșit (JSON valid) trimite `error` și sesiunea continuă.
4. Consumer-ul clasifică eroarea: `client.Permanent(err)` trimite `nack` cu `permanent=true`, iar broker-ul mută mesajul direct în DLQ (`attempts=1`, `permanent=true`). Sunt permanente: tip sau versiune de schemă necunoscută, payload ilizibil, regulă de business încălcată. Este tranzitorie scrierea eșuată a efectului local.

## Consecințe

- Protocolul are două câmpuri noi: `tag` (deliver, ack, nack) și `permanent` (nack). Un client vechi, care confirmă doar după id, primește `error`.
- Broker-ul nu deduplică publicările. Copia duplicată este livrată, iar consumatorul idempotent nu repetă efectul (ADR 0002).
- Mulțimea consumatorilor care au eșuat un mesaj este în memorie; după restartul broker-ului se pierde, la fel ca numărul de încercări.
