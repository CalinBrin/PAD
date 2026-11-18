# Remarcile primite pe o soluție anterioară: cum sunt evitate aici

| Remarca | Ce s-a făcut | Dovada |
|---|---|---|
| Un GET început înainte de un PUT pune valoarea veche în cache; fără contor de generație; fără test | contoare de generație citite **înainte** de cererea la DW și salvate în intrare; intrarea veche nu se mai potrivește | `TestSlowGetStartedBeforePut_DoesNotCacheStaleValue`, `docs/evidence/demo-race-001.log` |
| Un PUT care expiră, dar a fost aplicat, nu invalidează | invalidare după scriere oricare ar fi rezultatul, plus una întârziată (`WRITE_GRACE`) la rezultat necunoscut | `TestPutTimesOutButApplied_*`, `TestWriteCommittedAfterProxyGaveUp_*`, `demo-put-timeout-001.log` |
| Un GET reîncercat care închiriază (are efecte) | niciun GET nu are efecte; pull-ul folosește cursor ținut de cititor, fără lease; retry doar pentru operații idempotente | ADR 0005, ADR 0006, `TestChangeFeedRetryHasNoSideEffect` |
| Fără health check activ; documentația spune altceva decât codul | `HealthChecker` interoghează `/readyz` la 2 s, plus breaker pasiv; funcționează fără trafic | `TestActiveHealthCheckWithoutTraffic`, `demo-health-001.log` |
| Fără ADR, fără tag, un singur commit | 6 ADR-uri cu alternative comparate; commit-uri pe etape; tag `lab3-v1.0` | `docs/adr/`, `git log` |
| Dovezi doar într-un tabel, fără loguri și fișiere, fără comparație cu / fără cache | loguri reale per scenariu, fișiere JSON/CSV per rulare, comparație cu / fără cache × sănătos / lent / oprit / Redis oprit | `docs/evidence/` |
| „Smart proxy” neargumentat; fără Redis; SQLite valabil doar pe același host | ADR 0004 compară interpretările; Redis pentru cache și registru; Cassandra prin rețea | ADR 0001, ADR 0004 |

Fiecare remarcă are un ADR (decizia și alternativele), un test automat și un log de pe sistemul real; detaliile scenariilor sunt în `failure-scenarios.md`.
