"""_sync_carrefour.py — sincroniza dim_imagen desde carrefour_matches.json (set-based)."""
import json
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "duckdb" / "precios.duckdb"

matches = json.loads((ROOT / "data" / "img" / "carrefour_matches.json").read_text(encoding="utf-8"))
manifest = json.loads((ROOT / "data" / "img" / "manifest.json").read_text(encoding="utf-8"))

con = duckdb.connect(str(DB))
try:
    tablas = [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
    print("tablas:", tablas)
    total_antes = con.execute("SELECT count(*) FROM dim_imagen").fetchone()[0]

    con.execute("CREATE OR REPLACE TEMP TABLE m2 (ean VARCHAR, key VARCHAR, bytes BIGINT, fuente VARCHAR)")
    rows = [(e, v["key"], v.get("bytes", 0), "carrefour")
            for e, v in manifest.items()
            if v.get("key") and not v.get("error") and f"img/{e}.webp" in v["key"]]
    con.executemany("INSERT INTO m2 VALUES (?,?,?,?)", rows)
    con.execute("""
        INSERT INTO dim_imagen (ean, key, bytes, fuente, actualizado)
        SELECT ean, key, bytes, fuente, current_date
        FROM m2 WHERE ean NOT IN (SELECT ean FROM dim_imagen)
    """)
    total = con.execute("SELECT count(*) FROM dim_imagen").fetchone()[0]
    print(f"dim_imagen: {total_antes} -> {total} (carrefour nuevos: {total - total_antes})")
    por_fuente = con.execute("SELECT fuente, count(*) FROM dim_imagen GROUP BY 1 ORDER BY 2 DESC").fetchall()
    print("por fuente:", por_fuente)
finally:
    con.close()
