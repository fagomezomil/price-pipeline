"""img_cencosud.py — paso 2 vía 2b: imágenes de los banners VTEX de Cencosud (Jumbo/Disco/Vea).

Reutiliza el motor de img_carrefour (facets fq anidado + window-skip + inyección -500-500).
Los 3 banners corren bajo la misma cuenta VTEX (jumboargentina): Jumbo 322k SKUs,
Vea/Disco ~375k cada uno (verificado 09/11) — catálogo enorme pero comparte maestro
central, por eso el default es JUMBO ONLY y los otros dos son opt-in (--banner).
Escribe data/img/cencosud_{banner}_matches.json; sube con fuente=<banner>.

Uso: python src/img_cencosud.py [--banner jumbo] [--crawl] [--limit N]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scrapers"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import img_upload  # noqa: E402
from base import LOG, RateLimitedClient  # noqa: E402
from img_carrefour import arbol, coleccionar, crawl  # noqa: E402

BANNERS = {
    "jumbo": "https://www.jumbo.com.ar",
    "disco": "https://www.disco.com.ar",
    "vea": "https://www.vea.com.ar",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--banner", default="jumbo", choices=list(BANNERS))
    ap.add_argument("--crawl", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--rate", type=float, default=0.35)
    args = ap.parse_args()

    banner = args.banner
    base = BANNERS[banner]
    matches_path = img_upload.DATA_IMG / f"cencosud_{banner}_matches.json"

    if args.crawl or not matches_path.exists():
        with RateLimitedClient(base, rate=args.rate, verify=False) as c:
            got = crawl(c, args.rate, base_url=base)
        matches_path.write_text(json.dumps(got, ensure_ascii=False, indent=0), encoding="utf-8")
        LOG.info("[%s] matches: %d eans", banner, len(got))
        if args.crawl:
            return 0

    matches = json.loads(matches_path.read_text(encoding="utf-8"))
    cli = img_upload.cliente_s3()
    subidas = img_upload.subir(matches, banner, LOG, cli=cli, limit=args.limit)
    LOG.info("FIN [%s]: %d imágenes nuevas", banner, subidas)
    return 0


if __name__ == "__main__":
    sys.exit(main())
