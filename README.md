# price-pipeline

**EN** — Daily supermarket price warehouse for Argentina: national SEPA ingestion, scrapers for regional retail chains, a data-quality ("curado") layer, and a product-image pipeline to Cloudflare R2. In active development — the daily cycle runs against real stores and the companion viewer is live.

**ES** — Almacén diario de precios de supermercados: ingesta SEPA nacional, scrapers de cadenas regionales, capa de calidad ("curado") y pipeline de imágenes de producto a Cloudflare R2. En desarrollo activo — el ciclo diario corre contra tiendas reales y el visor complementario está en producción.

**Status / Estado:** in progress · en desarrollo. Real outputs from a recent working day: **SEPA ingestion live, 5 chain scrapers built (platforms: WooCommerce store API, VTEX, Magento, Oracle Commerce), mediana por EAN, image catalog**. Real data and credentials are not included — see `sample/`.

## What it does / Qué hace

1. **SEPA ingestion / Ingesta SEPA** — downloads the national SEPA dataset (store-level prices from retail chains across the country, millions of rows/day), filters the garbage rows the source ships (non-numeric store IDs), and loads commerce / sucursales / productos tables into DuckDB, daily-partitioned Parquet.
   *Descarga el dataset nacional SEPA (precios por sucursal de cadenas de todo el país, millones de filas/día), filtra las líneas basura del origen y carga las tablas comercio/sucursales/productos en DuckDB con Parquet particionado por día.*

2. **Chain scrapers / Scrapers por cadena** — one scraper per e-commerce platform with a shared `RateLimitedClient` (retries + backoff + rate limit) writing to a **unified PyArrow schema**: `dt, fuente, cadena, sucursal_nombre, ean, descripcion, marca, cantidad, unidad, precio_lista, promo1, promo2, imagen_url`.
   *Un scraper por plataforma de e-commerce con una base `RateLimitedClient` compartida y esquema unificado de salida.*

3. **Curado / Quality layer** — clean-price view over raw data, duplicate-promo and suspicious-price flags, `es_pesable` detection via GS1 prefix, and a **median price table per (date, retailer, EAN)** to benchmark chain vs chain. Raw data stays intact.
   *Vista de precios curada sobre la data cruda (que queda intacta), flags de promo sospechosa y peso variable (prefijo GS1), y **mediana por (día, cadena, EAN)** para comparar cadenas.*

4. **Image pipeline / Pipeline de imágenes** — extracts product image URLs directly from each chain's pricing API (Woo/VTEX/Magento/GA items) in the same crawl, uploads the catalog to **Cloudflare R2** (EAN → image, backed by the viewer).
   *Extrae las URL de imagen de cada API de precios en el mismo crawl, sube el catálogo EAN → imagen a R2.*

## Data flow / Flujo de datos

```
SEPA national dataset ──► sepa_daily.py ──┐
                                          │     partitioned Parquet /
Cordiez ──► vtex.py ──────────────────────┤───► DuckDB warehouse
Super Toledo ──► vtex.py (config) ────────┤          │
California ──► california.py (Woo) ───────┤          ▼
Maxiconsumo ──► maxiconsumo.py (Magento) ─┤      curado.py ──► cleaned views + medianas
Super Mami ──► supermami.py (Oracle) ─────┘          │
                                                     ▼
                                     img_*.py ──► push_r2.py ──► Cloudflare R2
                                                     │
                                              price-web (viewer) ◄── R2 sync
```

## Repo map / Mapa del repo

| Path | What / Qué |
|---|---|
| `ingest_sepa.py` | SEPA full-dataset loader into DuckDB / cargador del dataset full a DuckDB |
| `src/sepa_daily.py` | Daily SEPA cycle: download → extract → Parquet (+DuckDB views), idempotent / ciclo diario SEPA, idempotente |
| `src/sonda_ckan.py` | CKAN portal probe / sonda del portal CKAN |
| `src/curado.py` | Quality layer: clean views, medianas por EAN, GS1 pesable flag / capa de calidad |
| `src/scrapers/` | Per-chain scrapers + shared `base.py` (unified schema, rate limiting, retries) |
| `src/recon/` | Scheme probes and recon scripts written while reverse-engineering each platform / probes y recon hechos al revertir cada plataforma |
| `src/img_*.py`, `push_r2.py` | Product image pipeline → R2 / imágenes de producto → R2 |
| `sample/` | Synthetic CSVs of both output schemas / esquemas de salida en CSV sintético |
| `docs/next-build-checklist.md` | Build checklist (Next 16 + memory-leak lessons) / checklist de build |

## Run / Cómo correr

```bash
pip install httpx duckdb pyarrow
python src/sepa_daily.py --date 2026-10-17   # daily SEPA cycle, idempotent / ciclo diario, idempotente
python src/curado.py  --date 2026-10-17      # quality layer / capa de curación
```

Store scrapers are pointed at each chain's public endpoints from `src/scrapers/`; image upload needs R2 credentials in a local `.env` (never committed).
*Los scrapers usan los endpoints públicos de cada cadena; la subida de imágenes requiere credenciales R2 en un `.env` local (no commit).*

## Notes / Notas

- Raw scraped and SEPA data are never modified — quality rules live in views on top / *la data cruda nunca se modifica; las reglas de calidad viven en vistas.*
- Idempotent by design: re-running a day won't duplicate rows / *idempotente: re-corre de un día no duplican.*
- The production viewer reading this warehouse is built in the companion repo [price-web](https://github.com/fagomezomil/price-web).

## License

MIT
