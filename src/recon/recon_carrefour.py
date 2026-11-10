"""_recon_carrefour.py 5 — profundidad del anidado en facets fq."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scrapers"))
from base import RateLimitedClient, LOG  # noqa: E402
import logging

logging.basicConfig(level=logging.CRITICAL)
LOG.setLevel(logging.CRITICAL)

prof_max = 0

def prof(nodo, nivel):
    global prof_max
    ch = nodo.get("Children") or []
    if ch:
        prof_max = max(prof_max, nivel)
    for c in ch:
        prof(c, nivel + 1)

with RateLimitedClient("https://www.carrefour.com.ar", rate=0.35, verify=False) as c:
    r = c.get("/api/catalog_system/pub/facets/search", params={"fq": "C:161/"})
    t = r.json()["CategoriesTrees"][0]
    prof(t, 0)
    print("nodo 161: Quantity", t.get("Quantity"), "profundidad anidada:", prof_max)
    ch = t.get("Children") or []
    print("hijos:", len(ch), "suma quantities hijos:", sum(h.get("Quantity") or 0 for h in ch))
    # nietos con hijos?
    nietos_con_hijos = [(g.get("Name"), len(g.get("Children") or [])) for h in ch for g in (h.get("Children") or []) if g.get("Children")]
    print("nietos con bis-nietos:", nietos_con_hijos[:6], "… total", len(nietos_con_hijos))
