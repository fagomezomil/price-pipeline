"""Scraper Maxiconsumo (mayorista) — Magento.

- Descubre categorías por fuerza bruta de id (1..MAX_CAT) en /catalog/category/view/id/N.
- Crawl paginado ?p=N de cada categoría; precios en data-price-amount, nombre en product-item-link.
- No expone EAN en el listado: key = id interno de producto en la URL. Complementar con SEPA Mayorista.
Uso: python src/scrapers/maxiconsumo.py [--date YYYY-MM-DD]
"""
import argparse
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from base import LOG, RateLimitedClient, parquet_existe, _num, escribir_parquet  # noqa: E402

URL = "https://www.maxiconsumo.com"
MAX_CAT = 300


def _leer_pagina(r) -> list[dict]:
    h = r.text
    out = []
    cards = re.findall(
        r'"product-item-link"\s+href="([^"]+)"[^>]*>\s*([^<]{5,120})', h)
    # el listado intercala precios; matchear por orden es frágil: extraer por bloque de card
    blocks = re.findall(r'<li class="item product product-item[^"]*">(.*?)</li>', h, flags=re.S)
    if blocks:
        for b in blocks:
            link = re.search(r'"product-item-link"\s+href="([^"]+)"', b)
            nombre = re.search(r'"product-item-link"[^>]*>\s*([^<]{5,120})', b)
            precio = re.search(r'data-price-amount="([\d.]+)"', b)
            if not (link and precio):
                continue
            m = re.search(r"-(\d+)\.html", link.group(1))
            img = None
            im = re.search(r'(?:data-src|src)="([^"]*/media/[^"]+)"', b)
            if im:
                img = im.group(1)
            out.append({
                "ean": None,
                "descripcion": (nombre.group(1).strip() if nombre else ""),
                "precio_lista": _num(precio.group(1)),
                "producto_id": m.group(1) if m else link.group(1),
                "url": link.group(1),
                "imagen_url": img,
            })
        return out
    for link, nombre in cards:
        out.append({"ean": None, "descripcion": re.sub(r"\s+", " ", nombre).strip(),
                    "precio_lista": None, "producto_id": link, "url": link})
    return out


def scrapear(dt: str, rate: float = 0.35) -> int:
    if parquet_existe("maxiconsumo", dt):
        LOG.info("maxiconsumo %s: parquet ya existe, skip", dt)
        return -1
    filas = []
    vistos = set()
    with RateLimitedClient(URL, rate=rate, verify=False) as c:
        # descubrir categorías
        cats = []
        for i in range(1, MAX_CAT + 1):
            r = c.get(f"/catalog/category/view/id/{i}")
            if r is None or r.status_code != 200 or "product-item" not in r.text:
                # 404 => no existe; 200 sin productos => subcategoría vacía
                continue
            titulo = re.search(r"<title>([^<]+)</title>", r.text)
            cats.append((i, titulo.group(1).strip() if titulo else f"id{i}"))
        LOG.info("categorías descubiertas: %d %s", len(cats), [c[1] for c in cats])
        for i, titulo in cats:
            page = 1
            while True:
                r = c.get(f"/catalog/category/view/id/{i}", params={"p": page, "product_list_limit": 48})
                if r is None or r.status_code != 200:
                    break
                items = _leer_pagina(r)
                if not items:
                    break
                for it in items:
                    if not it["precio_lista"]:
                        continue
                    k = it.get("url") or it["descripcion"]
                    if k in vistos:  # categorías padre repiten productos de sus hijas
                        continue
                    vistos.add(k)
                    filas.append({
                        "dt": dt, "fuente": "scraper", "cadena": "maxiconsumo",
                        "sucursal_nombre": "Maxiconsumo (mayorista online)",
                        "ean": it["ean"], "descripcion": it["descripcion"],
                        "marca": None, "cantidad": None, "unidad": None,
                        "precio_lista": it["precio_lista"], "promo1": None, "promo2": None,
                        "imagen_url": it.get("imagen_url"),
                    })
                if not re.search(r"[?&]p=(%d)" % (page + 1), r.text):
                    break
                page += 1
                if page > 60:
                    LOG.warning("cat %s: >60 páginas, cortando", i)
                    break
            LOG.info("cat %s (%s): listo, %d filas acumuladas", i, titulo, len(filas))
    # dedupe hecho en colecta (vistos); los cat padre ven después de las hijas
    return escribir_parquet("maxiconsumo", dt, filas)


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