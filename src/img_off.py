"""img_off.py — paso 2 de imágenes: backfill desde Open Food Facts (F-imágenes).

Fase SCAN: descarga el CSV gz de OFF (~0.9GB, una vez) a data/off/ y matchea
los EANs faltantes del catálogo contra `code` (normalizado strip de ceros)
sacando image_front_url -> data/img/off_matches.json  {ean13: url}.
Fase UPLOAD: descarga las imágenes matched, re-encode webp 500px, sube a R2
bucket `precios-img` y escribe dim_imagen con fuente='off' (idempotente,
mismo manifest data/img/manifest.json que img_r2.py).

Uso: python src/img_off.py [--scan] [--limit N]
Creds: precios-web/.env.local (igual que img_r2.py/push_r2.py).
"""
import argparse
import concurrent.futures as cf
import csv
import gzip
import io
import json
import logging
import sys
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
DATA_OFF = ROOT / "data" / "off"
DUMP_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
DUMP_FILE = DATA_OFF / "off_products.csv.gz"
MATCHES = DATA_IMG / "off_matches.json"
BUCKET = "precios-img"

log = logging.getLogger("img_off")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

http = urllib3.PoolManager(cert_reqs="CERT_NONE", maxsize=8)
UA = {"User-Agent": "precios-tuc/0.1 (buscador de precios tucumano; python urllib3)"} # identificamos la app como pide OFF


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


def eans_faltantes() -> dict[str, str]:
    """{normalizado_strip_ceros: ean_original} — EANs del catálogo sin fila en dim_imagen."""
    con = duckdb.connect(str(DB), read_only=True)
    filas = con.sql("""
        SELECT DISTINCT c.ean
        FROM (SELECT DISTINCT ean FROM v_precio_curado) c
        LEFT JOIN dim_imagen d ON c.ean = d.ean
        WHERE d.ean IS NULL
    """).fetchall()
    con.close()
    out: dict[str, str] = {}
    for (ean,) in filas:
        ean = str(ean)
        if ean:
            out.setdefault(ean.lstrip("0"), ean)
    return out


def descargar_dump() -> None:
    if DUMP_FILE.exists() and DUMP_FILE.stat().st_size > 100_000_000:
        log.info("dump ya presente: %s (%.1f MB)", DUMP_FILE, DUMP_FILE.stat().st_size / 1e6)
        return
    DATA_OFF.mkdir(parents=True, exist_ok=True)
    part = DUMP_FILE.with_suffix(".part")
    log.info("descargando dump OFF (%s)…", DUMP_URL)
    r = http.request(
        "GET",
        DUMP_URL,
        headers=UA,
        preload_content=False,
        redirect=True,
        timeout=urllib3.Timeout(connect=30, read=600),
    )
    if r.status != 200:
        raise SystemExit(f"descarga dump: HTTP {r.status}")
    n = 0
    with open(part, "wb") as f:
        for chunk in r.stream(1 << 20):
            f.write(chunk)
            n += 1
            if n % 200 == 0:  # ~200MB
                log.info("… %.0f MB", n)
    r.release_conn()
    part.rename(DUMP_FILE)
    log.info("dump OK: %.1f MB", DUMP_FILE.stat().st_size / 1e6)


def escanear(limit: int) -> dict[str, str]:
    """Escaneo streaming del CSV gz: {ean_original: url_imagen} de los faltantes."""
    objetivo = eans_faltantes()
    log.info("faltantes en catálogo: %d", len(objetivo))
    if limit:
        objetivo = dict(list(objetivo.items())[:limit])
    got: dict[str, str] = {}
    leidos = 0
    cols = ("code", "image_front_url", "image_small_url")
    with gzip.open(DUMP_FILE, "rt", encoding="utf-8", errors="replace", newline="") as f:
        rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(rd)
        idx = {k: header.index(k) for k in cols if k in header}
        if "code" not in idx:
            raise SystemExit("CSV sin columna code")
        ncols = len(header)
        for row in rd:
            leidos += 1
            if len(row) < ncols:
                continue
            code = row[idx["code"]].strip().lstrip("0")
            ean = objetivo.get(code)
            if not ean or ean in got:
                continue
            for col in cols[1:]:
                if col in idx and row[idx[col]].strip():
                    got[ean] = row[idx[col]].strip()
                    break
            if not leidos % 500_000:
                log.info("… %d filas, %d matcheados", leidos, len(got))
    log.info("FIN scan: %d filas leídas, %d EANs matcheados de %d objetivo", leidos, len(got), len(objetivo))
    return got


def recodificar_webp(raw: bytes) -> bytes | None:
    try:
        im = Image.open(io.BytesIO(raw)).convert("RGB")
        im.thumbnail((500, 500), Image.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, "WEBP", quality=82, method=4)
        return buf.getvalue()
    except Exception:
        return None


def procesar(args):
    ean, url = args
    try:
        r = http.request(
            "GET",
            url,
            headers=UA,
            timeout=urllib3.Timeout(connect=15, read=25),
            retries=urllib3.Retry(total=2, backoff_factor=1),
        )
        if r.status != 200 or not r.data or len(r.data) < 300:
            return ean, None, f"desc:{r.status}"
        webp = recodificar_webp(r.data)
        if not webp:
            return ean, None, "encode"
        return ean, webp, None
    except Exception as e:
        return ean, None, f"desc:{type(e).__name__}"


def main() -> int:
    global cli
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true", help="fase 1: matchear EANs contra el dump (no sube nada)")
    ap.add_argument("--limit", type=int, default=0, help="cap de EANs a procesar")
    args = ap.parse_args()

    cli = None
    if not args.scan:
        creds = cargar_creds()
        cli = boto3.client(
            "s3",
            endpoint_url=f"https://{creds['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=creds["R2_ACCESS_KEY"],
            aws_secret_access_key=creds["R2_SECRET"],
            region_name="auto",
            config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=30, read_timeout=120),
        )

    DATA_IMG.mkdir(parents=True, exist_ok=True)
    man_path = DATA_IMG / "manifest.json"
    manifest = json.loads(man_path.read_text(encoding="utf-8")) if man_path.exists() else {}

    # fase match (se re-corre con --scan para refrescar, ej. tras ciclo nuevo)
    got_all = {}
    if args.scan or not MATCHES.exists():
        descargar_dump()
        got_all = escanear(args.limit)
        MATCHES.write_text(json.dumps(got_all, ensure_ascii=False, indent=0), encoding="utf-8")
        log.info("matches escritos: %d -> %s", len(got_all), MATCHES)
        if args.scan:
            return 0
    matches = json.loads(MATCHES.read_text(encoding="utf-8"))

    pend = [(e, u) for e, u in matches.items() if not manifest.get(e, {}).get("key")]
    if args.limit:
        pend = pend[: args.limit]
    log.info("a subir: %d de %d matches (resto con key en manifest)", len(pend), len(matches))

    nuevos: dict[str, dict] = {}
    fallos: dict[str, str] = {}
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for ean, webp, err in ex.map(procesar, pend):
            if webp is not None:
                cli.put_object(Bucket=BUCKET, Key=f"img/{ean}.webp", Body=webp, ContentType="image/webp")
                nuevos[ean] = {"key": f"img/{ean}.webp", "bytes": len(webp), "url": ""}
            else:
                fallos[ean] = err
            if not (len(nuevos) + len(fallos)) % 1000:
                log.info("… %d subidos, %d fallos", len(nuevos), len(fallos))

    for e, v in nuevos.items():
        manifest.setdefault(e, {}).update(v)
        manifest[e].pop("error", None)
    for e, err in fallos.items():
        manifest.setdefault(e, {})["error"] = err
    man_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=0), encoding="utf-8")
    log.info("FIN upload: nuevos=%d, fallidos=%d", len(nuevos), len(fallos))

    # dim_imagen: inserta solo los fuente=off que no estén aún (idempotente)
    ok = {e: (manifest[e]["key"], manifest[e]["bytes"]) for e in matches if manifest.get(e, {}).get("key") and not manifest[e].get("error")}
    if not ok:
        log.info("nada para dim_imagen")
        return 0
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
        ya = {str(r[0]) for r in w.execute("SELECT ean FROM dim_imagen WHERE fuente = 'off'").fetchall()}
        ins = [(e, v[0], v[1]) for e, v in ok.items() if e not in ya]
        w.executemany(
            "INSERT OR REPLACE INTO dim_imagen (ean, key, bytes, fuente, actualizado) "
            "VALUES (?, ?, ?, 'off', current_date)",
            ins,
        )
        total = w.execute("SELECT count(*) FROM dim_imagen").fetchone()[0]
        log.info("dim_imagen: %d insertadas (fuente off) — total %d", len(ins), total)
    finally:
        w.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
