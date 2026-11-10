"""sonda_ckan.py — mide cuándo SEPA publica y repisa el ZIP del día.

Modo --once (usado por schtask cada 30 min): un solo round de HEADs y sale.
El estado (último ETag por recurso) se reconstruye del propio CSV, así que
no hay estado en memoria y arranca limpio tras reboot/logon.

Modo normal (sin --once): loop continuo cada 30 min (para correr en sesión).

Log en logs/sonda_ckan.csv. Con 2-3 días de CSV se ve la primera
disponibilidad del ZIP nuevo diario y la hora del republish completo.
Uso: python src/sonda_ckan.py [--once] [--intervalo 1800]
"""
import argparse
import csv
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

import httpx

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "logs" / "sonda_ckan.csv"
CKAN = "https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios"
UA = {"User-Agent": "precios-tuc/0.1 (sonda periodica de disponibilidad; python httpx)"}

log = logging.getLogger("sonda")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

FIELDS = ["ts", "recurso", "status", "bytes", "http_last_modified", "etag", "cambio"]


def cargar_prev() -> dict[str, str]:
    """Último etag visto por recurso, de las filas históricas del CSV."""
    prev: dict[str, str] = {}
    if not CSV.exists():
        return prev
    with open(CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("etag"):
                prev[row["recurso"]] = row["etag"]
    return prev


def recursos(cli: httpx.Client) -> dict[str, str]:
    """{nombre_dia: url} de los 7 recursos weekday."""
    pkg = cli.get(CKAN, timeout=60).json()["result"]
    out = {}
    for x in pkg["resources"]:
        name = (x.get("name") or "").strip()
        url = x.get("url") or ""
        if url.endswith(".zip") and name and len(name) <= 12:
            out[name] = url
    return out


def sondea(cli: httpx.Client, urls: dict[str, str], writer, prev: dict[str, str]) -> None:
    """Un round de HEADs -> CSV. Actualiza prev in place."""
    for name, url in sorted(urls.items()):
        try:
            r = cli.head(url, timeout=30, follow_redirects=True)
        except Exception as e:
            log.warning("HEAD %s falló: %s", name, e)
            continue
        et = r.headers.get("etag", "")
        cambio = name in prev and et != prev[name]
        writer.writerow({
            "ts": datetime.now().isoformat(timespec="seconds"),
            "recurso": name,
            "status": r.status_code,
            "bytes": r.headers.get("content-length", ""),
            "http_last_modified": r.headers.get("last-modified", ""),
            "etag": et[:60],
            "cambio": "SI" if cambio else "",
        })
        if cambio:
            log.info("CAMBIO %s (etag nuevo)", name)
        prev[name] = et


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="un round y salir (para schtask)")
    ap.add_argument("--intervalo", type=int, default=1800)
    a = ap.parse_args()

    CSV.parent.mkdir(exist_ok=True)
    nuevo = not CSV.exists()
    f = open(CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    if nuevo:
        writer.writeheader()

    prev = cargar_prev()
    with httpx.Client(headers=UA, follow_redirects=True) as cli:
        urls = recursos(cli)
        log.info("recursos: %s", ", ".join(urls))
        if a.once:
            sondea(cli, urls, writer, prev)
            f.flush()
            f.close()
            log.info("round único OK -> %s", CSV)
            return 0
        log.info("sondeando cada %ds -> %s", a.intervalo, CSV)
        while True:
            sondea(cli, urls, writer, prev)
            f.flush()
            time.sleep(a.intervalo)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
