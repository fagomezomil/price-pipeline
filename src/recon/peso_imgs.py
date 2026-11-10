import json
import statistics
import urllib.request
import ssl

import duckdb

con = duckdb.connect("D:/Proyectos/precios/data/duckdb/precios.duckdb", read_only=True)

n_eans, n_con_img = con.sql(
    "SELECT count(DISTINCT ean), count(DISTINCT CASE WHEN imagen_url IS NOT NULL AND imagen_url != '' THEN ean END) "
    "FROM v_scrape"
).fetchone()[0]
print(f"EANs distintos en scrape: {n_eans} · con imagen: {n_con_img}")

dt = con.sql("SELECT CAST(max(dt) AS VARCHAR) FROM precio_mediana").fetchone()[0]
total_hoy = con.sql(f"SELECT count(DISTINCT ean) FROM v_precio_curado WHERE CAST(dt AS VARCHAR) = '{dt}'").fetchone()[0]
print(f"catalogo con precios ({dt}): {total_hoy} EANs · cobertura imagen: {100*n_con_img/total_hoy:.2f}%")

# muestra por fuente/CDN
fuente = con.sql("""
  SELECT regexp_extract(imagen_url, 'https?://([^/]+)', 1) AS host, count(*) AS n
  FROM (SELECT DISTINCT ean, imagen_url FROM v_scrape WHERE imagen_url IS NOT NULL AND imagen_url != '')
  GROUP BY host ORDER BY n DESC
""").fetchall()
print("\nhosts de imágenes:")
for h, n in fuente:
    print(f"  {h}: {n}")

# HEAD sobre una muestra de 24 URLs para estimar peso medio
urls = con.sql("""
  SELECT imagen_url FROM (
    SELECT imagen_url, row_number() OVER (PARTITION BY regexp_extract(imagen_url, 'https?://[^/]+') ORDER BY ean) AS rn
    FROM (SELECT DISTINCT ean, imagen_url FROM v_scrape WHERE imagen_url IS NOT NULL AND imagen_url != '')
  ) WHERE rn <= 6
""").fetchall()

ctx = ssl.create_default_context()
pesos: dict[str, list[int]] = {}
for (u,) in urls:
    host = urllib.parse.urlparse(u).netloc
    try:
        req = urllib.request.Request(u, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            cl = r.headers.get("Content-Length")
            if cl:
                pesos.setdefault(host, []).append(int(cl))
    except Exception as e:
        print(f"  fail {host}: {type(e).__name__}")

print("\nnuestra muestra (HEAD Content-Length):")
todos = []
for h, v in pesos.items():
    m = statistics.mean(v)
    print(f"  {h}: n={len(v)} media={m/1024:.0f} KB")
    todos.extend(v)
if todos:
    m = statistics.mean(todos)
    print(f"\nmedia muestral global: {m/1024:.0f} KB por imagen")
    raw_gb = n_con_img * m / 1e9
    webp = n_con_img * (m * 0.35) / 1e9  # re-encode ~35%
    print(f"estimación catálogo scrapeado: raw ≈ {raw_gb:.1f} GB · re-encoded ~35% ≈ {webp:.2f} GB")
con.close()
