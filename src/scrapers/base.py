"""Base de scrapers: cliente HTTP con retries/backoff/rate-limit y salida Parquet esquema unificado."""
import logging
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "scrape"
LOG = logging.getLogger("scraper")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

ESQUEMA = pa.schema([
    ("dt", pa.string()), ("fuente", pa.string()), ("cadena", pa.string()),
    ("sucursal_nombre", pa.string()), ("ean", pa.string()), ("descripcion", pa.string()),
    ("marca", pa.string()), ("cantidad", pa.string()), ("unidad", pa.string()),
    ("precio_lista", pa.float64()), ("promo1", pa.float64()), ("promo2", pa.float64()),
    ("imagen_url", pa.string()),
])


class RateLimitedClient:
    """httpx Client con rate limit + retries."""

    def __init__(self, base_url: str, rate: float = 0.3, verify: bool = True, backoff_base: float = 2.0):
        self.c = httpx.Client(
            base_url=base_url,
            headers={"User-Agent": UA, "Accept-Language": "es-AR,es"},
            timeout=httpx.Timeout(60, read=120),
            verify=verify,
            follow_redirects=True,
        )
        self.rate = rate
        self.backoff_base = backoff_base

    def get(self, path: str, **kw) -> httpx.Response | None:
        for intento in range(3):
            try:
                time.sleep(self.rate)
                r = self.c.get(path, **kw)
                if r.status_code in (429, 500, 502, 503):
                    raise httpx.HTTPStatusError(str(r.status_code), request=r.request, response=r)
                return r
            except (httpx.HTTPError, httpx.TransportError) as e:
                LOG.warning("retry %d: %s %s", intento + 1, path, e)
                time.sleep(self.backoff_base ** intento * self.backoff_base)
        return None

    def close(self):
        self.c.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def parquet_existe(cadena: str, dt: str) -> bool:
    """True si el parquet del dt ya está escrito (idempotencia temprana: no crawlear al pedo)."""
    return (DATA / cadena / f"dt={dt}" / "precios.parquet").exists()


def escribir_parquet(cadena: str, dt: str, filas: list[dict]) -> int:
    """Escribe data/scrape/{cadena}/dt={dt}.parquet. Skips si ya existe."""
    out_dir = DATA / cadena / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "precios.parquet"
    if dest.exists() and dest.stat().st_size > 0:
        LOG.info("%s %s: ya existe, skip", cadena, dt)
        return -1
    # normalizar claves faltantes al esquema
    colmap = {k: [r.get(k) for r in filas] for k in ESQUEMA.names}
    tabla = pa.Table.from_pydict(colmap, schema=ESQUEMA)
    pq.write_table(tabla, dest, compression="zstd")
    LOG.info("%s %s: %d precios -> %s", cadena, dt, len(filas), dest)
    return len(filas)


def _num(v) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None