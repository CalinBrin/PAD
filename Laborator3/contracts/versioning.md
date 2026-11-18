# Versionare și compatibilitate

## Versiunea resursei

Fiecare angajat are `version`. Ea crește cu 1 numai când se schimbă conținutul (un `PUT` identic nu o schimbă). Versiunea servește la:

- `ETag` (`"v<version>-<format>"`) și `If-None-Match` → `304`;
- `If-Match` → scriere condiționată, `412` la nepotrivire (concurență optimistă);
- nodul sursă: aplică din feed doar versiunile mai mari decât cea locală (feed-ul retrimite fereastra de suprapunere);
- Cassandra: `UPDATE ... IF version = ?`, deci două replici nu pot scrie aceeași versiune.

## Evoluția contractului

- **Câmpuri noi în răspuns** se pot adăuga: clienții acestui laborator citesc răspunsurile tolerant (`DecodeBytesLoose`: câmpuri JSON necunoscute ignorate; `encoding/xml` ignoră elemente necunoscute).
- **Câmpuri noi în cereri** nu se pot trimite unei replici vechi: DW respinge câmpurile JSON necunoscute (`400`). E o alegere deliberată: o greșeală de scriere în numele unui câmp nu trece neobservată. De aceea, la o schimbare, replicile DW se actualizează înaintea clienților.
- **O schimbare incompatibilă** (redenumire, schimbarea tipului) primește un prefix nou de rută (`/v2/employees`). Proxy-ul și cache-ul tratează orice rută nouă separat (cheia de cache conține ruta).
- Testul de contract fixează cele 9 câmpuri JSON și elementele XML (`TestContract`): o schimbare accidentală a contractului pică testul.

## Versiunea evaluată

Tag-ul git `lab3-v1.0`.
