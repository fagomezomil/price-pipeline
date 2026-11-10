"""img_dia.py — paso 2 vía 2e: imágenes de DIA Argentina (VTEX, catálogo visible chico).

Motor de carrefour reutilizado. Escribe data/img/dia_matches.json, filtra SEPA
(cache) y sube con fuente='dia'.
Uso: python src/img_dia.py [--crawl] [--limit N]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scrapers"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import img_upload  # noqa: E402
from base import LOG, RateLimitedClient  # noqa: E402
from img_carrefour import crawl  # noqa: E402

BASE = "https://diaonline.supermercadosdia.com.ar"
MATCHES = img_upload.DATA_IMG / "dia_matches.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawl", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--rate", type=float, default=0.35)
    args = ap.parse_args()

    if args.crawl or not MATCHES.exists():
        with RateLimitedClient(BASE, rate=args.rate, verify=False) as c:
            got = crawl(c, args.rate, base_url=BASE)
        MATCHES.write_text(json.dumps(got, ensure_ascii=False, indent=0), encoding="utf-8")
        LOG.info("[dia] matches: %d eans", len(got))
        if args.crawl:
            return 0

    matches = json.loads(MATCHES.read_text(encoding="utf-8"))
    hist = set(json.loads((img_upload.DATA_IMG / "sepa_eans.json").read_text(encoding="utf-8")))
    filtrado = {e: u for e, u in matches.items() if e in hist}
    LOG.info("[dia] %d matches → %d en catálogo SEPA", len(matches), len(filtrado))
    cli = img_upload.cliente_s3()
    n = img_upload.subir(filtrado, "dia", LOG, cli=cli, limit=args.limit)
    LOG.info("FIN [dia]: %d nuevas", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
