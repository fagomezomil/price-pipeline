"""img_carrefour.py — paso 2 vía 2: imágenes desde el catálogo VTEX de Carrefour AR (F-imágenes).

Fase CRAWL: recorre el árbol de categorías (facets fq=C:{id}/ devuelve subárbol
anidado con Quantity, el tree/{depth} público viene truncado a 21 departamentos
verificado 09/11) y pagina /products/search?fq=C:{id}/ en ventanas de 50 hasta
vacío (mismo patrón window-skip que scrapers/vtex.py). Colecciona ean ->
imageUrl (dimensiones -500-500 inyectadas en ids/{imgid}).
Escribe data/img/carrefour_matches.json {ean13: url} SIN tocar duckdb.
Fase UPLOAD: reusa img_upload.subir (R2 + dim_imagen fuente='carrefour').

Uso: python src/img_carrefour.py [--crawl] [--limit N] [--rate 0.35]
Nota: no graba precios ni parquet — solo el mapa ean->imagen (paso 2).
"""
import argparse
import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scrapers"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import img_upload  # noqa: E402
from base import LOG, RateLimitedClient  # noqa: E402

CAP_QUERY = 1000  # VTEX rechaza _from > 1000

log = logging.getLogger("img_carrefour")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

MATCHES_PATH = img_upload.DATA_IMG / "carrefour_matches.json"
RX_IDS = re.compile(r"(ids/)(\d+)(/)")


def nid(n: dict) -> str:
    """id de nodo VTEX (trae 'Id' en facets y 'id' en otros)."""
    return str(n.get("Id") or n.get("id") or "")


def arbol(c, cid: str) -> dict | None:
    """Nodo con subárbol desde facets fq=C:{id}/. None si falla/queda vacío."""
    r = c.get(f"/api/catalog_system/pub/facets/search", params={"fq": f"C:{cid}/"})
    if r is None or r.status_code != 200:
        log.warning("facets %s falló (%s) — nodo descartado", cid, getattr(r, "status_code", "None"))
        return None
    trees = (r.json() or {}).get("CategoriesTrees") or []
    if not trees:
        return None
    nodo = trees[0]
    ch = nodo.get("Children") or []
    q = nodo.get("Quantity") or 0
    if not ch and q > CAP_QUERY:
        # truncado de facets: reintentar (una vez ya fue la llamada del padre)
        return None
    return nodo


def coleccionar(c, nodo: dict, out: list) -> None:
    """Facets trae subárbol anidado de 2 niveles; refetch solo nodos truncados
    (Children vacíos pero Quantity grande). Los nodos consultados van a `out`:
    hojas, y también el padre cuando es mixto (hijos con productos directos)."""
    nodo = dict(nodo)  # copia: el subárbol refetcheado no pisa el original
    q = nodo.get("Quantity") or 0
    ch = nodo.get("Children") or []
    if not ch and q > CAP_QUERY:
        sub = arbol(c, nid(nodo))
        if sub:
            ch = sub.get("Children") or []
            if q < (sub.get("Quantity") or 0):
                # el refetch vino con el subárbol entero (facets del padre quedó corto)
                q = sub.get("Quantity") or q
    if ch and q > CAP_QUERY:
        for hijo in ch:
            coleccionar(c, hijo, out)
        # padre mixto: sus productos directos no están en los hijos (dedupe filma solape)
        out.append(nodo)
    else:
        out.append(nodo)


def crawl(c, rate: float, base_url: str = "https://www.carrefour.com.ar") -> dict[str, str]:
    r = c.get("/api/catalog_system/pub/category/tree/9")
    if r is None or r.status_code != 200:
        raise SystemExit(f"tree/9 falló: {r.status_code if r else 'None'}")
    tops = r.json()
    log.info("departamentos: %d", len(tops))
    nodos: list[dict] = []
    for t in tops:
        sub = arbol(c, t["id"])
        if sub is None:
            sub = t
            log.info("depto %s sin facets (q=%d, hoja)", t.get("Name"), t.get("Quantity"))
        coleccionar(c, sub, nodos)
    log.info("nodos a consultar: %d (suma Quantity: %d)", len(nodos), sum(n.get("Quantity") or 0 for n in nodos))

    got: dict[str, str] = {}
    vistos = set()
    for i, n in enumerate(nodos):
        cid = nid(n)
        desde = 0
        windows_fallados = 0
        while True:
            r = c.get("/api/catalog_system/pub/products/search", params={"fq": f"C:{cid}/", "_from": desde, "_to": desde + 49})
            if r is None:
                windows_fallados += 1
                if windows_fallados >= 2:
                    break
                desde += 50
                if desde > CAP_QUERY - 50:
                    break
                continue
            windows_fallados = 0
            r.raise_for_status()
            items = r.json() or []
            if not items:
                break
            for p in items:
                for it in p.get("items") or []:
                    ean = str(it.get("ean") or "").strip()
                    if not ean or ean in vistos:
                        continue
                    vistos.add(ean)
                    ims = it.get("images") or []
                    url = ""
                    if ims:
                        url = ims[0].get("imageUrl") or ims[0].get("url") or ""
                    url = RX_IDS.sub(lambda m: f"{m.group(1)}{m.group(2)}-500-500{m.group(3)}", url)
                    if url and not url.startswith("http"):
                        url = f"{base_url}{url}"
                    if url:
                        got[ean] = url
            desde += 50
            if desde > CAP_QUERY:
                break
        if (i + 1) % 50 == 0:
            log.info("… %d/%d nodos, %d eans únicos", i + 1, len(nodos), len(got))
    log.info("FIN crawl: %d eans con imagen (de %d skus)", len(got), len(vistos))
    return got


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawl", action="store_true", help="fase crawl (sin crawlear si matches existe)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--rate", type=float, default=0.35)
    args = ap.parse_args()

    if args.crawl or not MATCHES_PATH.exists():
        with RateLimitedClient("https://www.carrefour.com.ar", rate=args.rate, verify=False) as c:
            got = crawl(c, args.rate)
        MATCHES_PATH.write_text(json.dumps(got, ensure_ascii=False, indent=0), encoding="utf-8")
        log.info("carrefour_matches.json: %d entradas", len(got))
        if args.crawl:
            return 0
    matches = json.loads(MATCHES_PATH.read_text(encoding="utf-8"))

    cli = img_upload.cliente_s3()
    subidas = img_upload.subir(matches, "carrefour", log, cli=cli, limit=args.limit)
    log.info("FIN: %d imágenes nuevas fuente carrefour", subidas)
    return 0


if __name__ == "__main__":
    sys.exit(main())
