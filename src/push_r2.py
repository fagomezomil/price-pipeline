"""push_r2.py — sync post-ciclo a Cloudflare R2 (F5).

Sube al bucket precios-sepa:
  1. data/duckdb/precios.duckdb               -> s3://precios-sepa/duckdb/precios.duckdb
  2. parquets del dt del día (SEPA + scrape)  -> s3://precios-sepa/sepa/dt=X/..., scrape/<cadena>/dt=X/
  3. manifest.json (al final, cuando todo ya está) -> el pull lado web nunca ve estado a medio terminar

Idempotente: si manifest.json ya registra este dt completo, sale sin subir.
Uso: python src/push_r2.py [--date 2026-11-09]
Credenciales: R2_ACCOUNT_ID / R2_ACCESS_KEY / R2_SECRET / R2_BUCKET leídas de
  precios-web/.env.local (fuente única de secrets en esta máquina) o del .env local.
"""
import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

# proxy AVG MITM de esta máquina: sin esto, ssl no ve su CA (core: SkillSpector --system-certs, curl -k)
try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

ROOT = Path(__file__).resolve().parent.parent
ENV_WEB = Path("D:/Proyectos/precios-web/.env.local")

log = logging.getLogger("push_r2")

# ~100-400MB por objeto: multipart obligatorio para que una connection
# inestable no tire el upload entero
TRANSFER = TransferConfig(
    multipart_threshold=32 * 1024 * 1024,
    multipart_chunksize=16 * 1024 * 1024,
    max_concurrency=4,
)


def cargar_creds() -> dict[str, str]:
    """R2_ACCOUNT_ID/ACCESS_KEY/SECRET/BUCKET desde .env.local del repo web (o .env de gestación)."""
    queridas = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY", "R2_SECRET", "R2_BUCKET")
    vals: dict[str, str] = {}
    for path in (ENV_WEB, ROOT / ".env"):
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if "=" not in line or line.startswith("#"):
                continue
            k, v = line.split("=", 1)
            for q in queridas:
                if k.strip() == q and q not in vals:
                    vals[q] = v.strip()
        if len([k for k in queridas if k in vals]) == 4:
            break
    faltan = [k for k in queridas if k not in vals or not vals[k]]
    if faltan:
        raise SystemExit(f"faltan credenciales R2 en {ENV_WEB} o {ROOT/'.env'}: {faltan}")
    return vals


def s3(creds: dict[str, str]):
    return boto3.client(
        "s3",
        endpoint_url=f"https://{creds['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=creds["R2_ACCESS_KEY"],
        aws_secret_access_key=creds["R2_SECRET"],
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=30,
            read_timeout=600,
        ),
    )


def archivos_del_dia(dt: str) -> list[Path]:
    """Parquets del dt (SEPA + cada scraper que corrió). Lo que no existió, no se sube."""
    sal: list[Path] = []
    d_sepa = ROOT / "data" / "sepa" / f"dt={dt}"
    if d_sepa.exists():
        sal.extend(sorted(p for p in d_sepa.iterdir() if p.suffix == ".parquet"))
    d_scrape = ROOT / "data" / "scrape"
    if d_scrape.exists():
        for cadena in sorted(d_scrape.iterdir()):
            if not cadena.is_dir():
                continue
            f = cadena / f"dt={dt}" / "precios.parquet"
            if f.exists():
                sal.append(f)
    return sal


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--force", action="store_true", help="re-subir aunque manifest diga completo")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)

    creds = cargar_creds()
    bucket = creds["R2_BUCKET"]
    cli = s3(creds)

    # idempotencia por manifest
    previo = {}
    try:
        r = cli.get_object(Bucket=bucket, Key="manifest.json")
        previo = json.loads(r["Body"].read().decode("utf-8"))
    except ClientError:
        pass
    if not a.force and previo.get("dt") == a.date and previo.get("completo"):
        log.info("dt %s ya sincronizado según manifest — nada que hacer", a.date)
        return 0

    db_src = ROOT / "data" / "duckdb" / "precios.duckdb"
    if not db_src.exists():
        log.error("no existe %s — correr curado primero", db_src)
        return 1

    # 1. DuckDB consolidada (la web descarga esto)
    log.info("subiendo %s (%.1f MB) -> duckdb/precios.duckdb", db_src.name, db_src.stat().st_size / 1e6)
    cli.upload_file(str(db_src), bucket, "duckdb/precios.duckdb", Config=TRANSFER)

    # 2. parquets del día (backup del delta)
    archivos = archivos_del_dia(a.date)
    if not archivos:
        log.warning("sin parquets del dt %s — se sube igual la duckdb", a.date)
    subidos = []
    for p in archivos:
        base = p.relative_to(ROOT / "data").as_posix()  # sepa/dt=.../x.parquet | scrape/cad/dt=.../precios.parquet
        log.info("subiendo %s (%.1f MB)", base, p.stat().st_size / 1e6)
        cli.upload_file(str(p), bucket, base, Config=TRANSFER)
        subidos.append(base)

    # 3. manifest al final
    cli.put_object(
        Bucket=bucket,
        Key="manifest.json",
        Body=json.dumps({"dt": a.date, "completo": True, "subidos": subidos}, ensure_ascii=False).encode("utf-8"),
    )
    log.info("manifest dt=%s — push completo (%d parquets)", a.date, len(subidos))
    return 0


if __name__ == "__main__":
    sys.exit(main())
