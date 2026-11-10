# Checklist build Next 16 — Fase 1 precios (back + front)

Aprendidos cross-project (verificados en producción en QN y otros proyectos) mapeados a decisiones
de este build. Fuente: global core + caso QN 2026-08→11 (leak de Buffers nativos).

## 1. Fetch / memory leaks (lo más caro aprendido)

- **TODA respuesta `fetch` se drena**: `res.json()/text()` o `await res.body?.cancel()` en TODOS
  los paths, incluidos error-early-return y fire-and-forget. undici NO libera conexiones con GC.
  El back de Fase 1 fetchea: Meilisearch, Supabase REST, R2 (aws-sdk), notificaciones.
- El leak real de Next standalone es en **Buffers nativos** (`arrayBuffers`/`external`), no en
  heap JS — un heap snapshot normal puede verse limpio con 500MB de Buffers retenidos.
- **Monitor de RSS desde el deploy (día 1)**: cron 5-10 min que loguee `pid + rss + uptime`
  (patrón QN `/opt/quenoticia/scripts/memory-sample.sh`) + `systemd MemoryMax` + restart
  prolijo (`RuntimeMaxUSec` o timer de restarts fijos). Con datos desde siempre, el diagnóstico
  toma horas, no semanas.
- Regla ESLint `no-unconsumed-fetch` (sanity-io): el paquete está UNPUBLISHED en npm — si se
  adopta, compilar `npx tsc` del GitHub clone y COMMITEAR `dist/` a `tools/eslint-rules/`.
  En flat config va como ELEMENTO del array, no con spread.
- **Nada de crons/tasks DENTRO del server**: el ciclo de datos es externo (schtask local ahora /
  cron VPS después). Los tasks internos del next-server son sospechosos principales en leaks
  timer-driven — mantener esa separación.
- Si el leak es del framework (verificado QN: issue vercel/next.js#92287, colas de Web Streams):
  NINGÚN fix de código propio lo mueve. Mitigación (restarts + MemoryMax) mientras llega fix
  upstream.

## 2. Next standalone + assets

- `public/` se sirve con los archivos existentes en el **BOOT** del server. Agregados después
  → 404 → el catch-all sirve HTML del app. **Fotos de producto: R2/CDN, nunca `public/` dinámico.**
- OG images si se generan (satori/resvg): binario → R2 o servicio en el deploy, no archivos
  escritos a `public/` en runtime.

## 3. Supabase (auth/favoritos/alertas solamente)

- **14M precios NO van a Supabase**: rompe free tier/egress. DuckDB/Parquet en el server via
  route handlers + ISR. Supabase queda solo auth + favoritos + alertas con RLS.
- Egress free 5.5GB/ciclo → binario grande → R2 siempre.
- Service key `sb_secret_...` la acepta PostgREST pero **NO Storage** (`/storage/v1` valida JWT
  legacy → 403). Si alguna vez se opera Storage: legacy JWT key o dashboard.
- PostgREST no expone schemas internos (`storage.objects` → 404 PGRST205): limpieza de Storage
  por dashboard (SQL editor o UI).
- Billing: Spend Cap ON. Sobre quota con cap ON = proyecto pausado (402), mejor que overage.

## 4. R2

- Todo binario ahí: catálogo EAN→imagen, OG images, backups de parquets. `@aws-sdk/client-s3`.
- Supabase queda DB+Auth únicamente.

## 5. Cloudflare (si CF queda delante del dominio)

- Edge congela headers: `cf-cache-status: REVALIDATED` conserva los headers de la PRIMERA
  respuesta aunque el origen los cambie (CSP, cache-control). Para cambiar: cache-bust
  `?v=` o Cache Rule Bypass.
- Sin override de Browser Cache TTL, CF respeta el max-age del origen — verificar con
  `cf-cache-status` en cada depósito de headers.

## 6. Front

- **Tailwind v4 layers**: CSS custom sin `@layer` le GANA a las utilities siempre (un display
  en CSS suelto mata un `hidden lg:flex`). Lo que debe desaparecer por breakpoint: clase
  Tailwind (`flex lg:hidden`), no display en CSS custom. Bug real en QN (tabbar).
- **Mapas** (Leaflet / react-simple-maps para el mapa de sucursales del PDP): dynamic import
  con `{ ssr: false }`.
- **`"use client"`** solo donde hay hooks/state/browser APIs. Home + PDP deben ser
  server-rendered con ISR (menos JS, mejor SEO — el SEO es parte del plan: páginas producto).
- Fechas como **ISO strings** server→client, formatear en client (ART, UTC-3 sin DST).
- **Zod** en límites: input de search (normalización/sinónimos), payloads de alertas/email.
- **Next 16 no es el Next de training data**: leer `node_modules/next/dist/docs/` de la feature
  antes de escribir (ISR, route handlers, sitemap, Image). Respetar deprecation notices.
- TypeScript strict, sin `any`; types en `src/types/` una sola vez.
- es-AR en todo el UI copy; semantic keys si entra i18n.

## 7. Data layer (DuckDB en el server — aprendidos de Fase 0)

- Conexión **read_only** desde el server de la web. Los parquets son append-only:
  escritura solo desde el pipeline.
- `read_parquet` multi-archivo SIN `union_by_name=true` (DuckDB 1.5.6 → INTERNAL Error
  "Unsupported type for NumericValueUnionToValue"). Esquemas ya normalizados.
- `arg_max()` no existe (usar `max()`); `con.sql(sql, [params])` no existe →
  `con.execute(sql, [params])`.
- Un `IO Error: No files found` en `duckdb.sql` global **envenena la transacción/connection**
  → conexión nueva por consulta si la web lee multi-día con dt variables.
- Conteo de sucursales por producto: `COUNT(DISTINCT sucursal_id)` (N descripciones por EAN).
- La web nunca abre el DuckDB de gestación local: el VPS tendrá **su propia copia sincronizada**
  (R2 como medio, ver plan de sync del pipeline).

## 8. Deploy VPS (cuando toque)

- Playwright: browsers por usuario — servicios que scrapeen del VPS corren como el mismo
  usuario que instaló (`~/.cache/ms-playwright`).
- Monitor de memoria + `MemoryMax` desde el arranque (punto 1).
- Sitemap + IndexNow: páginas producto por EAN — el patrón de QN (IndexNow) aplica igual.
