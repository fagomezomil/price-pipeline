"""Scraper de tiendas VTEX: Cordiez, Super Toledo.

API pública: /api/catalog_system/pub/facets/search (departamentos con Quantity)
y /api/catalog_system/pub/products/search?map=c&_from&_to (50 por página).
Uso: python src/scrapers/vtex.py [--cadena cordiez|toledo|all] [--date YYYY-MM-DD]
"""
import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from base import LOG, DATA, RateLimitedClient, escribir_parquet  # noqa: E402

CADENAS = {
    "cordiez": {"url": "https://www.cordiez.com.ar", "sucursal": "Cordiez (todas)"},
    "toledo": {"url": "https://www.toledodigital.com.ar", "sucursal": "Toledo (todas)"},
}

CAP_QUERY = 1000  # VTEX rechaza _from > 1000


def _walk(nodos: list, out: list, log=None) -> None:
    """Colecciona nodos a consultar. El search de un nodo VTEX INCLUYE los productos
    de sus subcategorías (verificado en vivo 2026-10-14: intersección padre/hijos = 100%).
    Estrategia: nodo con Quantity <= CAP se baja entero en una pasada, sin descender
    (los hijos están dentro). Nodos con hijos que exceden el CAP: se desciende a los
    hijos; si además son "mixtos" (suma hijos < Quantity) se consulta al padre capado
    para no perder los productos que no cuelgan de ninguna hija (dedupe por itemId filma
    el solape). Hojas que exceden el CAP: truncadas (tope físico de la API)."""
    for n in nodos:
        ch = n.get("Children") or []
        q = n.get("Quantity") or 0
        if ch and q > CAP_QUERY:
            suma = sum(c.get("Quantity") or 0 for c in ch)
            if log and suma < q:
                log.info("nodo mixto grande %s (q=%d, hijos=%d): hijos + padre capado", n.get("Name"), q, suma)
            _walk(ch, out, log)
            if suma < q:
                out.append(n)
        else:
            out.append(n)


def _imagen(it) -> str | None:
    """Primera URL de imagen del SKU VTEX (campo imageUrl; url como fallback)."""
    for im in it.get("images") or []:
        if im.get("imageUrl") or im.get("url"):
            return im.get("imageUrl") or im.get("url")
    return None


def scrapear(cadena: str, dt: str, rate: float = 0.3) -> tuple[int, int, int]:
    """Devuelve (skus, precios_ok, ean_vacios). Idempotente por dt parquet."""
    cfg = CADENAS[cadena]
    if (DATA / cadena / f"dt={dt}" / "precios.parquet").exists():
        LOG.info("[%s] %s: parquet ya existe, skip", cadena, dt)
        return -1, 0, 0
    filas = []
    vistos = set()
    esperado = 0
    with RateLimitedClient(cfg["url"], rate=rate, backoff_base=1.5) as c:
        r = c.get("/api/catalog_system/pub/facets/search", params={"_from": 0, "_to": 0})
        r.raise_for_status()
        facet = r.json()
        tree = facet["CategoriesTrees"]
        esperado = sum(x["Quantity"] for x in tree)
        nodos: list = []
        _walk(tree, nodos, LOG)
        LOG.info("[%s] nodos a consultar: %d (SKUs esperados: %d)", cadena, len(nodos), esperado)
        for n in nodos:
            link = (n.get("Link") or "").split("?")[0]        # /Bebidas/Vinos
            mapa = n.get("Map") or "c,c"                      # map del link
            desde = 0
            pag = 0
            windows_fallados = 0
            while True:
                r = c.get(f"/api/catalog_system/pub/products/search{link}",
                          params={"map": mapa, "_from": desde, "_to": desde + 49})
                if r is None:
                    # VTEX a veces 500ea determinístico un offset: saltar el window
                    # y seguir con el siguiente (mejor un hueco que perder el nodo).
                    windows_fallados += 1
                    LOG.warning("[%s] window %d de %s falló tras retries — saltando", cadena, desde, link)
                    if windows_fallados >= 2:
                        break
                    desde += 50
                    if desde > CAP_QUERY - 50:
                        break
                    continue
                windows_fallados = 0
                r.raise_for_status()
                items = r.json()
                if not items:
                    break
                for p in items:
                    for it in p["items"]:
                        sellers = it.get("sellers") or []
                        if not sellers:
                            continue
                        co = sellers[0].get("commertialOffer") or {}
                        precio = co.get("Price")
                        if not precio:
                            continue
                        # los nodos del tree-walk se solapan: dedupe por SKU (itemId)
                        if it.get("itemId") in vistos:
                            continue
                        vistos.add(it.get("itemId"))
                        # promo: en VTEX las promos van por teasers/otro endpoint — por ahora lista+pago
                        filas.append({
                            "dt": dt, "fuente": "scraper", "cadena": cadena,
                            "sucursal_nombre": cfg["sucursal"],
                            "ean": str(it.get("ean") or "") or None,
                            "descripcion": (p.get("productName") or ""),
                            "marca": (p.get("brand") or ""),
                            "cantidad": None, "unidad": None,
                            "precio_lista": float(precio),
                            "promo1": None, "promo2": None,
                            "imagen_url": _imagen(it),
                        })
                pag += 1
                desde += 50
                if not items or pag > 24:  # 24*50 = 1200 SKUs por subcat (VTEX tope real ~1000)
                    break
            if pag > 20:
                LOG.warning("[%s] nodo %s llenó %d páginas — puede faltar si >1200 SKUs", cadena, n["Name"], pag)
    total = escribir_parquet(cadena, dt, filas)
    ean_vacios = sum(1 for f in filas if not f["ean"])
    LOG.info("[%s] total filas: %s | skus=%d ean_vacios=%d", cadena, total, len(filas), ean_vacios)
    if total > 0 and total < esperado * 0.9:
        LOG.warning("[%s] cobertura baja: %s < 90%% de %s", cadena, total, esperado)
    return len(filas), total, ean_vacios


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cadena", default="all")
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--rate", type=float, default=0.3)
    a = ap.parse_args()
    import logging
    LOG.setLevel(logging.INFO)
    LOG.addHandler(logging.StreamHandler())
    objetivo = CADENAS if a.cadena == "all" else {a.cadena: CADENAS[a.cadena]}
    rc = 0
    for nombre in objetivo:
        try:
            scrapear(nombre, a.date, a.rate)
        except Exception as e:
            LOG.error("[%s] falló: %s", nombre, e)
            rc = 1
    sys.exit(rc)