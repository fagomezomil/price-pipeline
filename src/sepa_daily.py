"""Pipeline diario SEPA (minoristas): descarga -> extraction -> Parquet -> vistas DuckDB.

Uso: python src/sepa_daily.py [--date YYYY-MM-DD]
Idempotente: si ya existe data/sepa/dt=YYYY-MM-DD/precios_*.parquet, corte temprano.
Gotchas conocidos: los CSV de SEPA traen líneas basura al final (se filtra id_comercio numérico),
id_producto es el EAN real y productos_ean suele ser '1'.
"""
import io
import logging
import re
import sys
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx

# proxy AVG MITM de esta máquina: necesario para correr en sesión interactiva
try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "sepa"
DB = ROOT / "data" / "duckdb" / "precios.duckdb"
CKAN = "https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

DIAS = {0: "lunes", 1: "martes", 2: "miercoles", 3: "jueves", 4: "viernes", 5: "sabado", 6: "domingo"}

log = logging.getLogger("sepa")


def _sin_acentos(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").casefold()


def resolver_url_zip(d: date, client: httpx.Client) -> tuple[str, str]:
    """Resuelve la URL del ZIP del día vía CKAN. Devuelve (dt_iso, url).

    Los recursos son estables por día de semana ('Lunes'...'Domingo', con acentos); su contenido se
    reemplaza semanalmente. dt se toma del filename del resource (sepa_martes.zip no trae fecha), así
    que si el contenido es otra fecha el parquet igual queda etiquetado con el dt de corrida.
    """
    r = client.get(CKAN, timeout=60)
    r.raise_for_status()
    pkg = r.json()["result"]
    dia = _sin_acentos(DIAS[d.weekday()])
    exact = [
        x for x in pkg["resources"]
        if _sin_acentos(x.get("name") or "") == dia and (x.get("url") or "").endswith(".zip")
    ]
    if not exact:
        exact = [x for x in pkg["resources"] if dia in _sin_acentos(x.get("name") or "") and (x.get("url") or "").endswith(".zip")]
    if not exact:
        raise RuntimeError(f"no hay recurso ZIP para '{dia}'")
    return d.isoformat(), exact[0]["url"]


def descargar_zip(url: str, dest: Path, client: httpx.Client) -> None:
    tmp = dest.with_suffix(".part")
    with client.stream("GET", url, timeout=httpx.Timeout(120, read=600), follow_redirects=True) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
    if tmp.stat().st_size < 10_000_000:
        raise RuntimeError(f"zip sospechosamente chico ({tmp.stat().st_size} bytes)")
    tmp.replace(dest)


def _num(x) -> float | None:
    s = str(x or "").strip().replace(",", ".")
    if not s or not re.fullmatch(r"\d+(\.\d+)?", s):
        return None
    return float(s)


def procesar_cadena(iz: Path, cid: str, dt: str, out_dir: Path, schema) -> tuple[int, object]:
    """Un ZIP interno -> precios_{cid}.parquet. Devuelve (n_precios, df_sucursales)."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    dest = out_dir / f"precios_{cid}.parquet"
    if dest.exists() and dest.stat().st_size > 0:
        return 0, None

    import pandas as pd  # noqa: duplicate-safe
    COLS = ["dt", "fuente", "comercio_id", "bandera", "sucursal_id", "sucursal_nombre",
            "localidad", "provincia", "ean", "descripcion", "marca", "cantidad", "unidad",
            "precio_lista", "promo1", "promo1_leyenda", "promo2", "promo2_leyenda"]
    from io import StringIO

    with zipfile.ZipFile(iz) as z:
        def leer(name):
            raw = z.read(name)
            for enc in ("utf-8", "latin-1"):
                try:
                    return pd.read_csv(StringIO(raw.decode(enc)), sep="|", dtype=str)
                except UnicodeDecodeError:
                    continue
            return pd.read_csv(StringIO(raw.decode("utf-8", "replace")), sep="|", dtype=str)

        comercio = leer("comercio.csv")
        comercio = comercio[comercio["id_comercio"].astype(str).str.fullmatch(r"\d+", na=False)]
        bandera = dict(zip(comercio["id_bandera"].astype(str), comercio["comercio_bandera_nombre"].fillna("")))
        suc = leer("sucursales.csv")
        suc = suc[suc["id_comercio"].astype(str).str.fullmatch(r"\d+", na=False)]
        suc_meta = suc[["id_bandera", "id_sucursal", "sucursales_nombre", "sucursales_localidad", "sucursales_provincia"]].copy()

        writer = None
        n = 0
        with z.open("productos.csv") as f:
            raw_text = None
            buf = f.read()
            for enc in ("utf-8", "latin-1"):
                try:
                    raw_text = buf.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            if raw_text is None:
                raw_text = buf.decode("utf-8", "replace")
            del buf
        chunks = pd.read_csv(io.StringIO(raw_text), sep="|", dtype=str, chunksize=1_000_000)
        for chunk in chunks:
            chunk = chunk[chunk["id_comercio"].astype(str).str.fullmatch(r"\d+", na=False)]
            if chunk.empty:
                continue
            df = chunk.copy()
            # EAN: id_producto es el EAN real; fallback productos_ean
            ean = df["id_producto"].astype(str)
            fallback = df.get("productos_ean", pd.Series("", index=df.index)).astype(str)
            ean = ean.where(ean.str.fullmatch(r"\d+", na=False), fallback.where(fallback.str.fullmatch(r"\d+", na=False), ""))
            df_out = pd.DataFrame({
                "dt": dt,
                "fuente": "sepa",
                "comercio_id": df["id_comercio"].astype(str),
                "bandera": df["id_bandera"].astype(str).map(bandera).fillna(""),
                "sucursal_id": df["id_sucursal"].astype(str),
                "ean": ean,
                "descripcion": df["productos_descripcion"].fillna(""),
                "marca": df["productos_marca"].fillna(""),
                "cantidad": df["productos_cantidad_presentacion"].fillna(""),
                "unidad": df["productos_unidad_medida_presentacion"].fillna(""),
                "precio_lista": [_num(x) for x in df["productos_precio_lista"]],
                "promo1": [_num(x) for x in df.get("productos_precio_unitario_promo1", pd.Series(dtype=str))],
                "promo1_leyenda": df.get("productos_leyenda_promo1", pd.Series("", index=df.index)).fillna("").astype(str),
                "promo2": [_num(x) for x in df.get("productos_precio_unitario_promo2", pd.Series(dtype=str))],
                "promo2_leyenda": df.get("productos_leyenda_promo2", pd.Series("", index=df.index)).fillna("").astype(str),
            })
            df_out = df_out[df_out["precio_lista"].notna()]
            # merge por IDs crudos: la col "bandera" ya es el NOMBRE mapeado, no sirve de key
            df_out["_idb"] = df["id_bandera"].astype(str).loc[df_out.index]
            df_out = df_out.merge(
                suc_meta[["id_bandera", "id_sucursal", "sucursales_nombre", "sucursales_localidad", "sucursales_provincia"]],
                left_on=["_idb", "sucursal_id"],
                right_on=["id_bandera", "id_sucursal"],
                how="left",
            )
            df_out = df_out.drop(columns=["_idb"])
            df_out["sucursal_nombre"] = df_out["sucursales_nombre"].fillna("")
            df_out["localidad"] = df_out["sucursales_localidad"].fillna("")
            df_out["provincia"] = df_out["sucursales_provincia"].fillna("")
            tabla = pa.Table.from_pandas(df_out[COLS], schema=schema, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(dest, schema, compression="zstd")
            writer.write_table(tabla)
            n += len(df_out)
        if writer:
            writer.close()
        log.info("comercio %s: %d precios", cid, n)
    return n, suc_meta.assign(dt=dt, comercio_id=cid)


def procesar_todo(outer_zip: Path, out_dir: Path, dt: str) -> int:
    """Itera ZIPs internos una sola vez; escribe precios_* y sucursales.parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    schema = pa.schema([
        ("dt", pa.string()), ("fuente", pa.string()), ("comercio_id", pa.string()),
        ("bandera", pa.string()), ("sucursal_id", pa.string()), ("sucursal_nombre", pa.string()),
        ("localidad", pa.string()), ("provincia", pa.string()), ("ean", pa.string()),
        ("descripcion", pa.string()), ("marca", pa.string()), ("cantidad", pa.string()),
        ("unidad", pa.string()), ("precio_lista", pa.float64()), ("promo1", pa.float64()),
        ("promo1_leyenda", pa.string()), ("promo2", pa.float64()),
        ("promo2_leyenda", pa.string()),
    ])
    total = 0
    suc_frames = []
    with tempfile.TemporaryDirectory(dir=str(ROOT / "data")) as td:
        with zipfile.ZipFile(outer_zip) as z:
            z.extractall(td)
        tz = Path(td) / dt
        tz = tz if tz.exists() else Path(td)
        inner_zips = sorted(tz.rglob("sepa_*.zip"))
        log.info("ZIPs internos: %d", len(inner_zips))
        for iz in inner_zips:
            m = re.search(r"sepa-(\d+)_", iz.name)
            cid = m.group(1) if m else iz.stem.split("_")[1]
            dest = out_dir / f"precios_{cid}.parquet"
            if dest.exists() and dest.stat().st_size > 0:
                log.info("comercio %s: skip precios (ya procesado)", cid)
                try:
                    import pandas as pd
                    from io import StringIO
                    with zipfile.ZipFile(iz) as z:
                        raw = z.read("sucursales.csv")
                    df = None
                    for enc in ("utf-8", "latin-1"):
                        try:
                            df = pd.read_csv(StringIO(raw.decode(enc)), sep="|", dtype=str)
                            break
                        except UnicodeDecodeError:
                            continue
                    df = df[df["id_comercio"].astype(str).str.fullmatch(r"\d+", na=False)]
                    suc_frames.append(df.assign(dt=dt, comercio_id=cid))
                except Exception as e:
                    log.warning("sucursales comercio %s falló: %s", cid, e)
                continue
            n, sdf = None, None
            try:
                n, sdf = procesar_cadena(iz, cid, dt, out_dir, schema)
            except zipfile.BadZipFile:
                log.warning("ZIP interno corrupto (comercio %s): %s — salteando", cid, iz.name)
                continue
            except Exception as e:
                log.error("comercio %s falló: %s — salteando", cid, e)
                continue
            if sdf is not None:
                suc_frames.append(sdf)
    if suc_frames:
        tabla = pa.Table.from_pandas(pd.concat(suc_frames, ignore_index=True), preserve_index=False)
        pq.write_table(tabla, out_dir / "sucursales.parquet", compression="zstd")
        log.info("sucursales consolidado: %d filas", tabla.num_rows)
    # el conteo real se hace sobre los parquets (incluye corridas parciales anteriores)
    return None


def crear_vistas_duckdb() -> None:
    import duckdb
    DB.parent.mkdir(parents=True, exist_ok=True)
    base = ROOT.as_posix()
    con = duckdb.connect(str(DB))
    # sin union_by_name: esquemas idénticos (normalizados). Con él, DuckDB 1.5.6 truena
    # (INTERNAL Error: Unsupported type for NumericValueUnionToValue) al agregar multi-día.
    con.execute(
        f"CREATE OR REPLACE VIEW v_precio_sepa AS "
        f"SELECT * FROM read_parquet('{base}/data/sepa/dt=*/precios_*.parquet')"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW v_sucursal_sepa AS "
        f"SELECT * FROM read_parquet('{base}/data/sepa/dt=*/sucursales.parquet')"
    )
    con.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    dt = sys.argv[sys.argv.index("--date") + 1] if "--date" in sys.argv else date.today().isoformat()
    d = datetime.strptime(dt, "%Y-%m-%d").date()
    out_dir = DATA / f"dt={dt}"
    done_flag = out_dir / ".done"
    if done_flag.exists():
        log.info("SEPA %s ya procesado (idempotente), saliendo", dt)
        return 0
    import time
    t0 = time.time()
    zip_dest = out_dir / "sepa.zip"
    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True) as client:
        fecha_iso, url = resolver_url_zip(d, client)
        log.info("ZIP: %s -> %s", fecha_iso, url)
        out_dir.mkdir(parents=True, exist_ok=True)
        if not zip_dest.exists():
            descargar_zip(url, zip_dest, client)
            log.info("descarga: %.1f MB en %.0fs", zip_dest.stat().st_size / 1e6, time.time() - t0)
    n_real = None
    procesar_todo(zip_dest, out_dir, fecha_iso)
    import duckdb
    con = duckdb.connect()  # in-memory para el conteo
    n_real = con.sql(
        f"SELECT count(*) FROM read_parquet('{out_dir.as_posix().replace(chr(92), '/')}/precios_*.parquet')"
    ).fetchone()[0]
    if n_real < 5_000_000:
        log.error("insumo parcial: %d precios (<5M), fallando por seguridad", n_real)
        return 1
    zip_dest.unlink(missing_ok=True)
    crear_vistas_duckdb()
    done_flag.touch()
    log.info("SEPA %s OK: %d precios en %.0fs", dt, n_real, time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())