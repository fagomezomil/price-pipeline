"""Scraper California S.A. (Posadas/Misiones) — WooCommerce Store API.

GET /wp-json/wc/store/v1/products?per_page=100&page=N ; `sku` = EAN; `prices.price` en centavos ARS.
Uso: python src/scrapers/california.py [--date YYYY-MM-DD]
"""
import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from base import LOG, RateLimitedClient, parquet_existe, escribir_parquet  # noqa: E402

URL = "https://www.californiasa.com.ar"


def _moneda_divisor(pr) -> float:
    """Divisor según currency_minor_unit de la propia respuesta (ARS = 2 -> centavos)."""
    return 10.0 ** int(pr.get("currency_minor_unit") or 0)


def scrapear(dt: str, rate: float = 0.3) -> int:
    if parquet_existe("california", dt):
        LOG.info("california %s: parquet ya existe, skip", dt)
        return -1
    filas = []
    divisor = 100.0
    with RateLimitedClient(URL, rate=rate) as c:
        page = 1
        while True:
            r = c.get("/wp-json/wc/store/v1/products", params={"per_page": 100, "page": page})
            if r is None:
                LOG.error("sin respuesta en página %d", page)
                break
            r.raise_for_status()
            items = r.json()
            if not items:
                break
            for it in items:
                pr = it.get("prices") or {}
                p = pr.get("price")
                if p in (None, ""):
                    continue
                precio = float(p) / _moneda_divisor(pr)
                filas.append({
                    "dt": dt, "fuente": "scraper", "cadena": "california",
                    "sucursal_nombre": "California (online)", "ean": str(it.get("sku") or "") or None,
                    "descripcion": it.get("name") or "", "marca": None,
                    "cantidad": None, "unidad": None,
                    "precio_lista": precio,
                    "promo1": _p2float(pr.get("sale_price"), _moneda_divisor(pr)), "promo2": None,
                    "imagen_url": ((it.get("images") or [{}])[0].get("src") or None),
                })
            total_pages = int(r.headers.get("X-WP-TotalPages", "0") or 0)
            LOG.info("California página %d/%s (%d items acumulados)", page, total_pages or "?", len(filas))
            if total_pages and page >= total_pages:
                break
            page += 1
            if page > 200:
                break
    return escribir_parquet("california", dt, filas)


def _p2float(v, divisor: float):
    try:
        return float(v) / divisor if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    a = ap.parse_args()
    import logging
    LOG.setLevel(logging.INFO)
    LOG.addHandler(logging.StreamHandler())
    try:
        scrapear(a.date)
        sys.exit(0)
    except SystemExit:
        raise
    except Exception:
        sys.exit(1)