# Rezultatele măsurătorilor

Generat de `loadgen -summarize` din fișierele `*.json` ale acestui director.

| Rulare | Scenariu | Cache | Algoritm | Cereri | Erori | RPS | p50 ms | p95 ms | p99 ms | HIT | MISS | Cereri la DW / cerere client | Retry | Distribuția pe DW |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 01-healthy-cache-on | 3 replici sănătoase | da | round_robin | 264651 | 0 | 13231.9 | 0.89 | 3.36 | 4.83 | 225713 | 33563 | 0.147 | 0 | dw-1:12986 dw-2:12986 dw-3:12986 |
| 02-healthy-cache-off | 3 replici sănătoase | nu | round_robin | 203625 | 0 | 10180.5 | 1.48 | 2.61 | 3.53 | 0 | 0 | 1.000 | 0 | dw-1:67881 dw-2:67882 dw-3:67882 |
| 03-slow-cache-on | dw-2 lent (+200 ms) | da | round_robin | 58551 | 0 | 2899.3 | 0.14 | 1.70 | 202.78 | 54019 | 3361 | 0.078 | 0 | dw-1:1518 dw-2:1517 dw-3:1517 |
| 04-slow-cache-off | dw-2 lent (+200 ms) | nu | round_robin | 4689 | 0 | 232.3 | 1.78 | 202.91 | 205.76 | 0 | 0 | 1.004 | 0 | dw-1:1570 dw-2:1570 dw-3:1569 |
| 05-slow-cache-off-lc | dw-2 lent (+200 ms), least_conn | nu | least_conn | 159923 | 0 | 7929.9 | 1.16 | 2.15 | 3.05 | 0 | 0 | 1.000 | 0 | dw-1:79271 dw-2:588 dw-3:80084 |
| 06-down-cache-on | dw-2 oprit (docker stop) la începutul rulării | da | round_robin | 273949 | 0 | 13696.1 | 0.88 | 3.15 | 4.49 | 234652 | 33727 | 0.144 | 9 | dw-1:19658 dw-2:9 dw-3:19659 |
| 07-down-cache-off | dw-2 oprit (docker stop) | nu | round_robin | 204611 | 0 | 10230.0 | 1.47 | 2.59 | 3.47 | 0 | 0 | 1.000 | 0 | dw-1:102316 dw-2:0 dw-3:102315 |
| 08-redis-down | Redis oprit: proxy fără cache (degradat) | da | round_robin | 257200 | 0 | 12859.3 | 1.14 | 2.06 | 2.66 | 0 | 0 | 1.000 | 0 | dw-1:85740 dw-2:85740 dw-3:85740 |
| 09-healthy-cache-on-uniform | 3 replici sănătoase, 500 de angajați, acces uniform | da | round_robin | 336084 | 0 | 16801.2 | 0.77 | 2.13 | 4.94 | 320821 | 8425 | 0.045 | 0 | dw-1:5094 dw-2:5094 dw-3:5095 |
