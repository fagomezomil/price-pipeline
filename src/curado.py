"""Capa de calidad SEPA: dim_sucursal acumulativa + vista curada + mediana por EAN-cadena.

Uso: python src/curado.py [--date YYYY-MM-DD]
Idempotente: dim/vistas son CREATE OR REPLACE; precio_mediana borra+reinserta por dt.
Los 4 puntos de la auditoría (ver memory/project_sepa_calidad.md):
  1) filtros <$10 / '%insumo%' en v_precio_curado (data cruda intacta en v_precio_sepa)
  2) dim_sucursal acumulativa multi-día para backfill de localidad/provincia
  3) flag es_pesable (EAN GS1 prefijo 02-29 = peso variable)
  4) flag promo_sospechosa + tabla precio_mediana por (dt, comercio_id, ean)
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger("curado")

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "duckdb" / "precios.duckdb"

LOGICA_CURADO = "precio_lista >= 10 AND descripcion NOT ILIKE '%insumo%'"


def crear_dim(con) -> None:
    """dim_sucursal acumulativa, única por (comercio, sucursal) para join 1:1 con precios.

    Dedupe por (comercio_id, id_sucursal) y no por (comercio, bandera, sucursal): los precios
    no llevan id_bandera crudo y hay banderas del mismo comercio que comparten id_sucursal
    (Maxi Comodín 1-2). El id_bandera se conserva como dato documental con arg_max.
    """
    base = ROOT.as_posix()
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_sucursal AS
        SELECT comercio_id, id_bandera, id_sucursal,
               arg_max(sucursales_nombre, dt)               AS sucursal_nombre,
               arg_max(NULLIF(sucursales_localidad, ''), dt) AS localidad,
               arg_max(NULLIF(sucursales_provincia, ''), dt) AS provincia,
               max(dt)                                       AS ultima_vez,
               min(dt)                                       AS primera_vez
        FROM read_parquet('{base}/data/sepa/dt=*/sucursales.parquet')
        GROUP BY 1, 2, 3
    """)
    # colapsar colisiones intra-comercio (join 1:1; se elige la fila con provincia poblada)
    con.execute("""
        CREATE OR REPLACE TABLE dim_sucursal AS
        SELECT * FROM (
            SELECT *, row_number() OVER (
                PARTITION BY comercio_id, id_sucursal
                ORDER BY (provincia IS NOT NULL AND provincia <> '') DESC, ultima_vez DESC
            ) AS rn FROM dim_sucursal
        ) WHERE rn = 1
    """)
    con.execute("ALTER TABLE dim_sucursal DROP COLUMN rn")
    n, n_prov = con.execute(
        "SELECT count(*), count(*) FILTER (provincia IS NOT NULL AND provincia <> '') FROM dim_sucursal"
    ).fetchone()
    log.info("dim_sucursal: %d sucursales (%d con provincia)", n, n_prov)


def crear_vista_curada(con) -> None:
    con.execute(f"""
        CREATE OR REPLACE VIEW v_precio_curado AS
        SELECT p.dt, p.comercio_id, p.bandera, p.sucursal_id,
               COALESCE(NULLIF(d.sucursal_nombre, ''), p.sucursal_nombre) AS sucursal_nombre,
               COALESCE(NULLIF(d.localidad, ''),       p.localidad)       AS localidad,
               COALESCE(NULLIF(d.provincia, ''),       p.provincia)       AS provincia,
               p.ean, p.descripcion, p.marca, p.cantidad, p.unidad,
               p.precio_lista, p.promo1, p.promo1_leyenda, p.promo2, p.promo2_leyenda,
               substr(p.ean, 1, 2) BETWEEN '02' AND '29'                AS es_pesable,
               p.promo1 IS NOT NULL AND p.promo1 > 0
                   AND p.promo1 < 0.5 * p.precio_lista                  AS promo_sospechosa
        FROM v_precio_sepa p
        LEFT JOIN dim_sucursal d ON p.comercio_id = d.comercio_id AND p.sucursal_id = d.id_sucursal
        WHERE {LOGICA_CURADO}
    """)


def construir_mediana(con, dt: str) -> int:
    """Tabla por (dt, comercio_id, ean) con mediana del precio efectivo. Append idempotente."""
    con.execute("CREATE TABLE IF NOT EXISTS precio_mediana ("
                "dt VARCHAR, comercio_id VARCHAR, ean VARCHAR, n_sucursales BIGINT, "
                "mediana DOUBLE, pmin DOUBLE, pmax DOUBLE, spread DOUBLE)")
    con.execute("DELETE FROM precio_mediana WHERE dt = ?", [dt])
    con.execute(f"""
        INSERT INTO precio_mediana
        SELECT dt, comercio_id, ean, count(*) AS n_sucursales,
               median(pe) AS mediana, min(pe) AS pmin, max(pe) AS pmax,
               max(pe) / NULLIF(median(pe), 0) AS spread
        FROM (
            SELECT CAST(dt AS VARCHAR) AS dt, comercio_id, ean,
                   COALESCE(CASE WHEN promo1 > 0 AND promo1 < precio_lista THEN promo1 END,
                            precio_lista) AS pe
            FROM read_parquet('{ROOT.as_posix()}/data/sepa/dt={dt}/precios_*.parquet')
            WHERE {LOGICA_CURADO}
        )
        GROUP BY 1, 2, 3
    """)
    (n,) = con.execute("SELECT count(*) FROM precio_mediana WHERE dt = ?", [dt]).fetchone()
    return n


def crear_vista_scrape(con) -> None:
    base = ROOT.as_posix()
    con.execute(f"""
        CREATE OR REPLACE VIEW v_scrape AS
        SELECT * FROM read_parquet('{base}/data/scrape/*/dt=*/precios.parquet', union_by_name=true)
    """)


def chequear_drift(con, dt: str) -> None:
    prev = (date.fromisoformat(dt) - timedelta(days=1)).isoformat()
    actual = con.execute("SELECT count(*) FROM precio_mediana WHERE dt = ?", [dt]).fetchone()[0]
    previo = con.execute("SELECT count(*) FROM precio_mediana WHERE dt = ?", [prev]).fetchone()[0]
    if actual and previo and abs(actual - previo) / previo > 0.3:
        log.warning("DRIFT >30%% en mediana: %s=%d -> %s=%d", prev, previo, dt, actual)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    dt = sys.argv[sys.argv.index("--date") + 1] if "--date" in sys.argv else date.today().isoformat()

    import duckdb
    con = duckdb.connect(str(DB))
    try:
        crear_dim(con)
        crear_vista_curada(con)
        n = construir_mediana(con, dt)
        log.info("precio_mediana %s: %d combos", dt, n)
        chequear_drift(con, dt)
        crear_vista_scrape(con)
    finally:
        con.close()
    log.info("curado OK (%s)", dt)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    a = ap.parse_args()
    sys.exit(main())