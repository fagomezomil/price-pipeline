"""Scraper Super Mami (Córdoba) — Oracle Commerce/Endeca (Plataforma Grupo Dinosaurio).

- Taxonomía: /super/categoria/<slug>/_/N-nodes colectados del nav del homepage (~466).
- Cada categoría pagina con No=offset (12 por página SSR).
- Precios de góndola en el array GA "items" (item_id prodN, item_name, item_brand, price).
Uso: python src/scrapers/supermami.py [--date YYYY-MM-DD]
"""
import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from base import LOG, RateLimitedClient, parquet_existe, escribir_parquet  # noqa: E402

URL = "https://www.supermami.com.ar"
UA_OVERRIDE = None


def _taxonomia(c) -> list[str]:
    r = c.get("/")
    r.raise_for_status()
    rutas = sorted(set(re.findall(r'super/categoria(?:/[\w\d-]+)?/_/N-[-\w]+', r.text)))
    LOG.info("categorías encontradas en nav: %d", len(rutas))
    return rutas


def _items_pagina(html: str) -> list[dict]:
    out = []
    match = re.search(r'"items"\s*:\s*\[(.*?)\]', html, flags=re.S)
    if not match:
        return out
    try:
        arr = json.loads("[" + match.group(1) + "]")
    except ValueError:
        return out
    for it in arr:
        if it.get("price") is None:
            continue
        out.append({
            "ean": None,
            "descripcion": it.get("item_name") or "",
            "marca": it.get("item_brand") or "",
            "precio_lista": float(it["price"]),
            "producto_id": it.get("item_id") or "",
            "imagen_url": it.get("item_image") or it.get("image") or None,
        })
    return out


def scrapear(dt: str, rate: float = 0.35) -> int:
    if parquet_existe("supermami", dt):
        LOG.info("supermami %s: parquet ya existe, skip", dt)
        return -1
    filas = []
    visto = set()
    with RateLimitedClient(URL, rate=rate) as c:
        rutas = _taxonomia(c)
        for r in rutas:
            page = 0
            while page < 100:
                r2 = c.get(f"/{r}" if not r.startswith("/") else r,
                           params={"No": page * 12, "Nrpp": 12})
                if r2 is None or r2.status_code != 200:
                    break
                items = _items_pagina(r2.text)
                if not items:
                    break
                nuevos = 0
                for it in items:
                    if it["producto_id"] in visto:
                        continue
                    visto.add(it["producto_id"])
                    filas.append({
                        "dt": dt, "fuente": "scraper", "cadena": "supermami",
                        "sucursal_nombre": "Super Mami (online)",
                        "ean": it["ean"], "descripcion": it["descripcion"],
                        "marca": it["marca"], "cantidad": None, "unidad": None,
                        "precio_lista": it["precio_lista"], "promo1": None, "promo2": None,
                        "imagen_url": it.get("imagen_url"),
                    })
                    nuevos += 1
                if nuevos == 0:
                    break
                page += 1
    LOG.info("Mami: %d productos", len(filas))
    return escribir_parquet("supermami", dt, filas)


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