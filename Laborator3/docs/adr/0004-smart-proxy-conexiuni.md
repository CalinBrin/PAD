# ADR 0004: Ce înseamnă „smart-proxy, pentru păstrarea conexiunilor spre clienți”

Status: acceptat (lab3-v1.0)

## Context

Enunțul: „Smart-proxy, pentru păstrarea conexiunilor spre clienți”, iar recomandarea: „Utilizarea Redis ca sistem de stocare a conexiunilor”. Diagrama arată în proxy două depozite: `CLIENTS` și `CACHE`.

Remarca primită pe o soluție anterioară: „Smart-proxy înseamnă aici un HttpClient cu conexiuni reutilizate spre DW. Enunțul vorbește de păstrarea conexiunilor spre clienți, care vin implicit de la Kestrel, deci formularea trebuie argumentată. Redis lipsește.”

Referința din enunț, *Smart Proxy* (Enterprise Integration Patterns), descrie un intermediar care **urmărește fiecare cerere** ca să poată trimite răspunsul înapoi clientului corect: păstrează adresa de retur și corelarea cerere-răspuns.

## Ce face un server HTTP implicit și ce adăugăm

| | `net/http` implicit (ca Kestrel) | Proxy-ul acestui laborator |
|---|---|---|
| Keep-alive HTTP/1.1 spre clienți | da, cu idle timeout implicit | da, `IdleTimeout` explicit (`CLIENT_IDLE_TIMEOUT`, 120 s), `ReadHeaderTimeout`, `MaxHeaderBytes` |
| Limită de conexiuni per client | nu | da: `MAX_CONNS_PER_CLIENT`, conexiunile peste limită sunt închise la accept și numărate (`TestPerClientConnectionLimit`) |
| Evidența conexiunilor fiecărui client | nu | da: deschise acum (per replică de proxy), total, refuzate |
| Evidența fiecărei cereri (client, conexiune, replica, cache, status, durată) | nu | da: `req:<correlation-id>` în Redis, 10 minute (`GET :8081/proxy/requests/{id}`) |
| Comun mai multor replici de proxy, persistent | nu | da: Redis cu AOF |
| Conexiuni spre DW reutilizate | client HTTP, la alegere | da: pool keep-alive (`MaxIdleConnsPerHost` 128), măsurat |

## Decizie

**Spre clienți (partea „păstrarea conexiunilor spre clienți”).**

- Conexiunile sunt păstrate deschise între cereri (keep-alive) până la `CLIENT_IDLE_TIMEOUT`. Demonstrat: 5 cereri curl pe o singură conexiune TCP (`num_connects=1` doar la prima, `demo-smart-proxy-001.log`). În benchmark, 264 651 de cereri au folosit 15 conexiuni de la client.
- Un `net.Listener` propriu (`proxy.LimitListener`) vede fiecare conexiune acceptată și închisă, aplică limita per client și o înregistrează.
- **Registrul clienților în Redis** (`proxy.Registry`) e partea `CLIENTS` din diagramă și răspunde recomandării „Redis ca sistem de stocare a conexiunilor”:

  ```
  client:<ip>        first_seen, last_seen, requests, conns_total, conns_rejected, last_path
  conns:<proxy-id>   <ip> -> conexiuni deschise acum pe această replică de proxy
  req:<corr-id>      client, client_id, remote_addr, method, path, backend, cache, status, attempts, duration_ms, state
  ```

  `req:<corr-id>` este exact ce descrie EIP Smart Proxy: pentru fiecare cerere în curs, cine a trimis-o, pe ce conexiune (`remote_addr`), la ce replică a ajuns și cu ce rezultat. Starea `in_progress` → `done`.
- Scrierile în Redis sunt **asincrone**: un canal cu buffer și o goroutină care le trimite în pipeline la 100 ms. Redis nu e niciodată pe drumul cererii. Dacă Redis e lent sau căzut, evenimentele se pierd și se numără (`registry_dropped_events`). O copie în memorie rămâne (`/proxy/clients` răspunde cu `source: local`).

**Spre DW.** Un singur `http.Transport` cu pool de conexiuni keep-alive. Benchmark-ul cu cache: 38 952 de cereri spre DW pe 6 conexiuni noi. Fără cache: 203 642 de cereri pe 3 conexiuni noi. Contoarele sunt `conns_new` / `conns_reused` în `/proxy/stats` (măsurate cu `httptrace`).

## Alternative comparate

| Interpretare | Ce ar însemna | De ce nu (sau de ce parțial) |
|---|---|---|
| Doar pool spre DW | `HttpClient` reutilizat | e „păstrarea conexiunilor”, dar spre servere, nu spre clienți; nu folosește Redis |
| Doar keep-alive implicit | nimic de scris | vine implicit din serverul HTTP, nu e o funcție a proxy-ului |
| Sesiuni lipicioase (sticky) în Redis: client → replica DW | clientul ajunge mereu la aceeași replică | replicile nu au stare (ADR 0001), deci sticky-ul nu aduce nimic și strică echilibrarea |
| Păstrarea conexiunilor TCP în Redis | | imposibil: un socket nu se poate serializa în Redis |
| **Registrul conexiunilor și cererilor în Redis + keep-alive gestionat + pool spre DW** (ales) | EIP Smart Proxy + recomandarea Redis | |

## Consecințe

- Fiecare cerere produce 2 evenimente asincrone în Redis. Benchmark-ul arată costul: cu Redis oprit (registrul nu mai scrie), proxy-ul servește ~26% mai multe cereri fără cache (12 860 vs 10 180 req/s pe aceeași mașină, unde și Redis, și Cassandra concurează pentru același CPU).
- `conns:<proxy-id>` se golește la pornirea replicii de proxy: conexiunile unei rulări anterioare nu mai există.

## Limita cunoscută

Clientul e identificat după IP (sau `X-Client-ID`, declarat de client, deci neautentificat). În spatele unui NAT, mai mulți clienți împart limita de conexiuni.
