"""_recon_leyendas.py — confirma nombres de columnas de leyenda en productos.csv SEPA."""
import io
import logging
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
log = logging.getLogger("recon")

import httpx

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

from sepa_daily import UA, descargar_zip, resolver_url_zip  # noqa: E402

tmp = ROOT / "data" / "sepa" / "_recon.zip"
if not tmp.exists():
    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True) as client:
        fecha, url = resolver_url_zip(date.today(), client)
        log.info("baja %s", url)
        descargar_zip(url, tmp, client)

with zipfile.ZipFile(tmp) as z:
    names = [n for n in z.namelist() if n.endswith(".zip")]
    log.info("zips internos: %d", len(names))
    with zipfile.ZipFile(z.open(names[0])) as iz:
        first = iz.open("productos.csv").readline()
        cols = first.decode("utf-8", "replace").strip().split("|")
        log.info("columnas productos.csv:")
        for c in cols:
            log.info("  %s", c)
tmp.unlink(missing_ok=True)
