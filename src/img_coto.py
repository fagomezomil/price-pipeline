"""img_coto.py — paso 2 vía 2c: imágenes de Coto via API pública de Constructor.io.

El catálogo de Coto está indexado en Constructor (95.322 items, búsqueda por
EAN literal funciona al 100%, item trae product_main_ean + image_url
verificado 09/11). browse/group viene vacío por API pública, así que la vía
es SEARCH POR EAN: 1 request por EAN faltante del catálogo (~47k).
Escribe data/img/coto_matches.json {ean: url}, filtra contra el catálogo SEPA
(cache data/img/sepa_eans.json) y sube con fuente='coto'.

Uso: python src/img_coto.py [--limit N] [--rps 10]
"""
import argparse
import concurrent.futures as cf
import json
import sys
import threading
import time
from pathlib import Path

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

import urllib3

import duckdb
import img_upload  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
MATCHES_PATH = img_upload.DATA_IMG / "coto_matches.json"
EANS_CACHE = img_upload.DATA_IMG / "sepa_eans.json"

KEY = "key_r6xzz4IAoTWcipni"  # pública del SPA (main.js), no es secret
BASE = "https://ac.cnstrc.com"

import logging

log = logging.getLogger("img_coto")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

http = urllib3.PoolManager(cert_reqs="CERT_NONE", maxsize=16)
UA = {"User-Agent": "precios-tuc/0.1 (buscador de precios; python urllib3)"}

RPS = 10.0
_lock = threading.Lock()
_slot = [0.0]  # token bucket global


def throttle() -> None:
    while True:
        with _lock:
            now = time.monotonic()
            espera = _slot[0] - now
            if espera <= 0:
                _slot[0] = max(now, _slot[0]) + 1.0 / RPS
                return
        time.sleep(min(espera, 0.5))


def buscar_ean(ean: str):
    """GET search/{ean} -> (ean, url|None). Exact-match por product_main_ean."""
    throttle()
    for intento in range(2):
        try:
            r = http.request(
                "GET",
                f"{BASE}/search/{ean}?key={KEY}&section=Products&num_results_per_page=5",
                headers=UA,
                timeout=urllib3.Timeout(connect=10, read=20),
                retries=urllib3.Retry(total=1, backoff_factor=1),
            )
            if r.status != 200:
                return ean, None
            hits = (json.loads(r.data.decode("utf-8")).get("response") or {}).get("results") or []
            for hit in hits:
                d = hit.get("data") or {}
                ean_hit = str(d.get("product_main_ean") or "").strip()
                url = str(d.get("image_url") or d.get("product_large_image_url") or "").strip()
                if ean_hit == ean and url:
                    return ean, url
            return ean, None
        except Exception as e:
            if intento == 1:
                log.warning("%s: %s", ean[:6], type(e).__name__)
                return ean, None


def eans_faltantes() -> list[str]:
    """Catálogo SEPA histórico (cache) sin imagen, por POPULARIDAD (n precios del dt último desc)."""
    hist = set(json.loads(EANS_CACHE.read_text(encoding="utf-8"))) if EANS_CACHE.exists() else None
    if hist is None:
        con = duckdb.connect(str(img_upload.DB), read_only=True)
        hist = {str(r[0]) for r in con.execute("SELECT DISTINCT ean FROM v_precio_curado").fetchall()}
        con.close()
        EANS_CACHE.write_text(json.dumps(sorted(hist)), encoding="utf-8")
    con = duckdb.connect(str(img_upload.DB), read_only=True)
    con_img = {str(r[0]) for r in con.execute("SELECT ean FROM dim_imagen").fetchall()}
    orden = [str(r[0]) for r in con.execute("""
        SELECT ean FROM v_precio_curado
        WHERE dt = (SELECT max(dt) FROM v_precio_curado)
        GROUP BY ean ORDER BY count(*) DESC
    """).fetchall()]
    con.close()
    falt = hist - con_img
    return [e for e in orden if e in falt] + sorted(falt - set(orden))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--rps", type=float, default=10.0)
    args = ap.parse_args()
    global RPS
    RPS = args.rps

    falt = eans_faltantes()
    log.info("faltantes: %d", len(falt))
    if not MATCHES_PATH.exists():
        MATCHES_PATH.write_text("{}", encoding="utf-8")
    matches = json.loads(MATCHES_PATH.read_text(encoding="utf-8"))
    pend = [e for e in falt if e not in matches]
    if args.limit:
        pend = pend[: args.limit]
    log.info("a buscar: %d (%d ya guardados en matches)", len(pend), len(falt) - len(pend))

    n_ok = n_proc = 0
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        for ean, url in ex.map(buscar_ean, pend):
            n_proc += 1
            if url:
                matches[ean] = url
                n_ok += 1
            if not n_proc % 1000:
                MATCHES_PATH.write_text(json.dumps(matches, ensure_ascii=False, indent=0), encoding="utf-8")
                log.info("… %d/%d consultados, %d matcheados", n_proc, len(pend), n_ok)
    MATCHES_PATH.write_text(json.dumps(matches, ensure_ascii=False, indent=0), encoding="utf-8")
    log.info("FIN search: %d matcheados de %d consultados", n_ok, len(pend))

    if not matches:
        return 0
    hist = set(json.loads(EANS_CACHE.read_text(encoding="utf-8")))
    filtrado = {e: u for e, u in matches.items() if e in hist}
    log.info("matches %d → %d en catálogo SEPA", len(matches), len(filtrado))
    cli = img_upload.cliente_s3()
    n = img_upload.subir(filtrado, "coto", log, cli=cli)
    log.info("coto subidas nuevas: %d", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
