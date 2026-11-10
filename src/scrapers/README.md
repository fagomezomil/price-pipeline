# Scrapers de cadenas — estado

Primera corrida completa: 2026-10-13 (verificación e2e de `run_diario.py` EXIT 0, idempotente).

| Cadena | Script | Estado | Vía |
|---|---|---|---|
| Cordiez | vtex.py (config `cordiez`) | **PAUSADO** (14/10, decisión Fede: consume ~65 min) — última data dt=2026-10-14, 6.165 SKUs (99,8% de 6.176). Reactivar en SCRAPERS de run_diario.py | API VTEX |
| Super Toledo | vtex.py (config `toledo`) | **PAUSADO** (14/10, decisión Fede: consume ~20 min) — última data dt=2026-10-13, 6.856 SKUs (76% de 8.963, nodos mixtos truncados a 1.250) | API VTEX |
| California | california.py | OK — 1.078 SKUs (EAN = sku, precios en centavos ÷100) | Woo store API v1 |
| Maxiconsumo | maxiconsumo.py | **PAUSADO** (14/10, ~54 min) — última data dt=2026-10-13, 10.350 SKUs con MAX_CAT=300 (sin EAN) | Magento server-side (verify=False) |
| Super Mami | supermami.py | **PAUSADO** (14/10, ~20 min) — última data dt=2026-10-13, 11.608 SKUs (466 categorías nav, array GA "items") | Oracle Commerce SSR |
| Super TODO | — | **PENDIENTE** (14/10, decisión Fede: va a la general): exige login (todoatucasa.xrp.net, plataforma XRP). Recon hecho (login.xrp.net); falta que Fede cree cuenta cliente → implementar scraper | XRP/ASP |
| La Gallega | — | **PENDIENTE** (14/10, decisión Fede: va a la general): exige login (POST /login.asp `Alias`+`Clave`, sin captcha — trivial httpx). Falta cuenta cliente → implementar scraper | ASP custom |
| Yaguar | — | FUERA de fase 0 (folletos PDF, mayorista). Posible: parseo de `yaguar.com.ar/catalogos/*.pdf`. | Woo + PDF |

Total capa scraper activa: California ~1.000 SKUs/día + SEPA ~14M. Cordiez/Toledo/Maxiconsumo/Super Mami conservan sus parquets en `data/scrape/` (visibles en `v_scrape`) pero no se crawlean a diario.

- **imagen_url** al esquema unificado (14/10): VTEX/Woo/Magento/GA items la extraen gratis en el mismo crawl. Tabla catálogo EAN→imagen para fase 1. Parquets viejos (dt≤13) quedan sin la columna; `v_scrape` lee con union_by_name.

Notas:
- Esquema de salida unificado en `base.py` (ESQUEMA). Precio online = lista de cadena, no por sucursal.
- Idempotencia: TODOS los scrapers chequean `parquet_existe()` al inicio (skip temprano, no re-crawleado). `-1` = skip = exit 0.
- VTEX estrategia de crawl (reescrita 2026-10-14, verificada: Cordiez 6.171 SKUs en 241s vs 65 min antes):
  - El search de un nodo **incluye los productos de sus subcategorías** (verificado en vivo: union padre == union hijos). Quantity del facets cuenta productos, no variantes (itemId puede inflar 3-4x).
  - Nodo con Quantity ≤ 1000 (cap `_from`) → una pasada, sin descender. Con hijos que exceden el cap → hijos + padre capado si es mixto (suma hijos < q). Hojas > cap → truncadas.
  - Windows: si `_from` da 500 determinístico 3/3 (= r None), **saltar el window** y seguir (2 huecos seguidos = cortar nodo). Los 500 sin esto abandonaban nodos enteros (Almacén Cordiez).
  - `imagen_url` = `items[].images[].imageUrl` (100% coverage). Retries VTEX con backoff 1.5.
- VTEX responde HTTP 206 (Partial Content) en búsquedas — es normal, no es error.
- DuckDB gotcha: `duckdb.sql` usa una conexión global; un `IO Error: No files found` aborta la transacción y envenena TODO el resto (`TransactionContext Error`). Usar una conexión nueva por consulta (ver `contar()` en run_diario.py).
- Frescura de Cordiez mejor de lo temido: yerba Rosamonte 1kg $3.519 (el $2.249 era de 500g). Toledo/Super Mami precios de góndola 2026-10-13 verificados a ojo vs SEPA.
- Maxiconsumo: solo lista categorías con id ≤ MAX_CAT; sin EAN, key = id interno. Complementar con SEPA Mayorista.