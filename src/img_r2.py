"""img_r2.py — imágenes v1 (F-imágenes, Fede aprobó, 09/11).

Paso 1: descargar las imagen_url del scrape (11.7k EANs), re-encode a
webp ~500px y subir a R2 bucket `precios-img` (PÚBLICO, se crea solo)
y registrar dim_imagen en la duckdb.
Idempotente: manifest local data/img/manifest.json (ean -> {key, bytes, url}).
Uso: python src/img_r2.py [--limit N]
Creds: precios-web/.env.local (igual que push_r2.py).
"""
import argparse
import concurrent.futures as cf
import io
import json
import logging
import sys
import urllib.parse
from pathlib import Path

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

import boto3
import duckdb
import urllib3
from botocore.config import Config
from PIL import Image

urllib3.disable_warnings()

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "duckdb" / "precios.duckdb"
DATA_IMG = ROOT / "data" / "img"
BUCKET = "precios-img"
IMG_BASE = "https://pub-c478db7ccd01488da4952346e75618c5.r2.dev"  # bucket precios-img public dev URL (activada 09/11)

log = logging.getLogger("img_r2")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

http = urllib3.PoolManager(cert_reqs="CERT_NONE", maxsize=8)
cli = None


def cargar_creds() -> dict[str, str]:
    queridas = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY", "R2_SECRET")
    vals: dict[str, str] = {}
    src = None
    for cand in (Path("D:/Proyectos/precios-web/.env.local"), ROOT / ".env"):
        if cand.exists():
            src = cand
            for line in cand.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    if k.strip() in queridas and k.strip() not in vals:
                        vals[k.strip()] = v.strip()
            if all(vals.get(k) for k in queridas):
                break
    faltan = [k for k in queridas if not vals.get(k)]
    if faltan:
        raise SystemExit(f"faltan R2_* en {src}: {faltan}")
    return vals


def recodificar_webp(raw: bytes) -> bytes | None:
    try:
        im = Image.open(io.BytesIO(raw)).convert("RGB")
        im.thumbnail((500, 500), Image.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, "WEBP", quality=82, method=4)
        return buf.getvalue()
    except Exception:
        return None


def procesar(row):
    """(ean, url) -> (ean, webp|None, err|None). 'salta' = ya estaba."""
    ean, url = str(row[0]), str(row[1])
    if ean in manifest and manifest[ean].get("key") and not manifest[ean].get("error"):
        return ean, None, "salta"
    try:
        r = http.request(
            "GET",
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=urllib3.Timeout(connect=15, read=25),
            retries=urllib3.Retry(total=2, backoff_factor=1),
        )
        if r.status != 200 or not r.data or len(r.data) < 300:
            return ean, None, f"desc:{r.status}"
        webp = recodificar_webp(r.data)
        if not webp:
            return ean, None, "encode"
        cli.put_object(Bucket=BUCKET, Key=f"img/{ean}.webp", Body=webp, ContentType="image/webp")
        return ean, webp, None
    except Exception as e:
        return ean, None, f"desc:{type(e).__name__}"


def main() -> int:
    global cli, manifest
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    creds = cargar_creds()
    cli = boto3.client(
        "s3",
        endpoint_url=f"https://{creds['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=creds["R2_ACCESS_KEY"],
        aws_secret_access_key=creds["R2_SECRET"],
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=30, read_timeout=120),
    )
    try:
        cli.head_bucket(Bucket=BUCKET)
    except Exception:
        log.info("creando bucket %s…", BUCKET)
        cli.create_bucket(Bucket=BUCKET)

    DATA_IMG.mkdir(parents=True, exist_ok=True)
    man_path = DATA_IMG / "manifest.json"
    manifest = {}
    if man_path.exists():
        manifest = json.loads(man_path.read_text(encoding="utf-8"))

    con = duckdb.connect(str(DB), read_only=True)
    filas = con.sql("""
        SELECT ean, imagen_url FROM (
            SELECT ean, imagen_url,
                   row_number() OVER (PARTITION BY ean ORDER BY count(*) DESC) AS rn
            FROM (SELECT DISTINCT ean, imagen_url FROM v_scrape
                  WHERE imagen_url IS NOT NULL AND imagen_url != '')
            GROUP BY ean, imagen_url
        ) WHERE rn = 1
        ORDER BY ean
    """).fetchall()
    con.close()
    if a.limit:
        filas = filas[: a.limit]
    log.info("EANs a procesar: %d (ya OK en manifest: %d)", len(filas), sum(1 for v in manifest.values() if v.get("key")))

    nuevos: dict[str, dict] = {}
    fallos: dict[str, str] = {}
    nproc = 0
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for ean, webp, err in ex.map(procesar, filas):
            nproc += 1
            if webp is not None:
                nuevos[ean] = {"key": f"img/{ean}.webp", "bytes": len(webp), "url": ""}
            elif err != "salta":
                fallos[ean] = err
            if nproc % 1000 == 0:
                log.info("… %d procesados (%d nuevos, %d fallos)", nproc, len(nuevos), len(fallos))

    manifest.update({k: {**v, "error": fallos.get(k)} for k, v in nuevos.items()})
    for e, err in fallos.items():
        manifest.setdefault(e, {})["error"] = err
    man_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=0), encoding="utf-8")
    log.info("FIN: nuevos=%d, fallidos=%d, manifest=%s", len(nuevos), len(fallos), man_path)

    if nuevos:
        log.info("escribiendo dim_imagen: %d filas", len(nuevos))
    # sync total: todo el manifest OK entra a dim_imagen (cubre corridas
    # previas donde el write final falló por lock, con manifest ya guardado)
    ok = {e: v for e, v in manifest.items() if v.get("key") and not v.get("error")}
    try:
        w = duckdb.connect(str(DB))
    except Exception as e:
        log.error("dim_imagen sync falló (lock): %s", e)
        return 0
    try:
        w.execute(
            "CREATE TABLE IF NOT EXISTS dim_imagen (ean VARCHAR PRIMARY KEY, key VARCHAR, "
            "bytes BIGINT, fuente VARCHAR, actualizado DATE DEFAULT current_date)"
        )
        ya = {str(r[0]) for r in w.execute("SELECT ean FROM dim_imagen").fetchall()}
        faltan = [(e, v["key"], v["bytes"]) for e, v in ok.items() if e not in ya]
        w.executemany(
            "INSERT OR REPLACE INTO dim_imagen (ean, key, bytes, fuente, actualizado) "
            "VALUES (?, ?, ?, 'scraper', current_date)",
            faltan,
        )
        log.info("dim_imagen: %d escritas (faltaban, de %d OK en manifest)", len(faltan), len(ok))
    finally:
        w.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
