"""img_upload.py — uploader común de imágenes a R2 + dim_imagen (F-imágenes).

Usado por img_off.py (Open Food Facts), img_carrefour.py (VTEX Carrefour) y
futuras fuentes. Recibe {ean: url}, descarga, re-encode webp 500px, sube a
bucket `precios-img` y sincroniza dim_imagen con fuente dada.
Idempotente vía manifest data/img/manifest.json.
"""
import concurrent.futures as cf
import io
import json
import logging
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
MANIFEST = DATA_IMG / "manifest.json"
BUCKET = "precios-img"
UA = {"User-Agent": "precios-tuc/0.1 (buscador de precios tucumano; python urllib3)"}

WORKERS = 24  # AVG proxy agrega ~6s por request: 8 threads rendían solo ~1.3/s

_cli = None  # cliente s3 compartido por los workers (seteado en subir)

http = urllib3.PoolManager(cert_reqs="CERT_NONE", maxsize=WORKERS)


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


def cliente_s3() -> boto3.client:
    creds = cargar_creds()
    return boto3.client(
        "s3",
        endpoint_url=f"https://{creds['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=creds["R2_ACCESS_KEY"],
        aws_secret_access_key=creds["R2_SECRET"],
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=30, read_timeout=120),
    )


def recodificar_webp(raw: bytes) -> bytes | None:
    try:
        im = Image.open(io.BytesIO(raw)).convert("RGB")
        im.thumbnail((500, 500), Image.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, "WEBP", quality=82, method=4)
        return buf.getvalue()
    except Exception:
        return None


def _procesar(row):
    """(ean, url) -> (ean, bytes_webp_subidos|None, err|None). El put a R2 va
    ACÁ dentro del worker (serial en el thread principal = 12 min/1000, bug
    encontrado 09/11)."""
    ean, url = row
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
        _cli.put_object(Bucket=BUCKET, Key=f"img/{ean}.webp", Body=webp, ContentType="image/webp")
        return ean, webp, None
    except Exception as e:
        return ean, None, f"desc:{type(e).__name__}"


def subir(
    matches: dict[str, str],
    fuente: str,
    log: logging.Logger,
    cli: "boto3.client | None" = None,
    limit: int = 0,
) -> int:
    """Sube {ean_original: url} a R2 y escribe dim_imagen (fuente). Devuelve subidas ok."""
    log.info("subir [%s]: %d urls en matches", fuente, len(matches))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    pend = [(e, u) for e, u in matches.items() if not manifest.get(e, {}).get("key")]
    if limit:
        pend = pend[:limit]
    log.info("[%s] a subir: %d (resto con key en manifest)", fuente, len(pend))

    nuevos: dict[str, dict] = {}
    fallos: dict[str, str] = {}
    if pend:
        global _cli
        _cli = cli if cli is not None else cliente_s3()
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for ean, webp, err in ex.map(_procesar, pend):
                if webp is not None:
                    nuevos[ean] = {"key": f"img/{ean}.webp", "bytes": len(webp), "url": ""}
                else:
                    fallos[ean] = err
                if not (len(nuevos) + len(fallos)) % 1000:
                    log.info("[%s] … %d subidos, %d fallos", fuente, len(nuevos), len(fallos))

    for e, v in nuevos.items():
        manifest.setdefault(e, {}).update(v)
        manifest[e].pop("error", None)
    for e, err in fallos.items():
        manifest.setdefault(e, {})["error"] = err
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=0), encoding="utf-8")
    log.info("[%s] FIN upload: nuevos=%d, fallidos=%d", fuente, len(nuevos), len(fallos))

    ok = {e: (manifest[e]["key"], manifest[e]["bytes"]) for e in matches if manifest.get(e, {}).get("key") and not manifest[e].get("error")}
    if not ok:
        log.info("[%s] nada para dim_imagen", fuente)
        return len(nuevos)
    try:
        w = duckdb.connect(str(DB))
    except Exception as e:
        log.error("[%s] dim_imagen sync falló (lock): %s", fuente, e)
        return len(nuevos)
    try:
        w.execute(
            "CREATE TABLE IF NOT EXISTS dim_imagen (ean VARCHAR PRIMARY KEY, key VARCHAR, "
            "bytes BIGINT, fuente VARCHAR, actualizado DATE DEFAULT current_date)"
        )
        # ya con imagen de CUALQUIER fuente: no pisapear (una imagen por ean)
        ya = {str(r[0]) for r in w.execute("SELECT ean FROM dim_imagen").fetchall()}
        ins = [(e, v[0], v[1], fuente) for e, v in ok.items() if e not in ya]
        w.executemany(
            "INSERT OR REPLACE INTO dim_imagen (ean, key, bytes, fuente, actualizado) "
            "VALUES (?, ?, ?, ?, current_date)",
            ins,
        )
        total = w.execute("SELECT count(*) FROM dim_imagen").fetchone()[0]
        log.info("[%s] dim_imagen: %d insertadas — total %d", fuente, len(ins), total)
    finally:
        w.close()
    return len(nuevos)
