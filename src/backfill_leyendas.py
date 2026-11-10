"""backfill_leyendas.py — agrega promo1_leyenda/promo2_leyenda (NULL) a los parquets
históricos SEPA para que todos los dts queden con esquema homogéneo 18 columnas.
Uso: python src/backfill_leyendas.py            (todos los dt, salta ya backfillados)
     python src/backfill_leyendas.py --dry      (solo lista lo que haría)
"""
import argparse
import os
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
SEPA = ROOT / "data" / "sepa"

# orden explícito = el del schema nuevo (promo1_leyenda entre promo1 y promo2)
SEL = ("dt, fuente, comercio_id, bandera, sucursal_id, sucursal_nombre, localidad, provincia, "
       "ean, descripcion, marca, cantidad, unidad, precio_lista, promo1, "
       "CAST(NULL AS VARCHAR) AS promo1_leyenda, promo2, CAST(NULL AS VARCHAR) AS promo2_leyenda")


def rewrite(f: Path, con) -> bool:
    """Devuelve True si reescribió, False si ya tiene las columnas."""
    cols = [c[0] for c in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(f)]).description]
    if "promo1_leyenda" in cols:
        return False
    if "promo2" not in cols:
        raise RuntimeError(f"{f}: sin promo2, esquema inesperado")
    tmp = f.with_suffix(".new.parquet")
    src, dst = f.as_posix(), tmp.as_posix()
    con.execute(f"""
        COPY ({'SELECT ' + SEL} FROM read_parquet('{src}'))
        TO '{dst}' (FORMAT PARQUET, COMPRESSION zstd)
    """)
    # swap con retry (el server web puede tener el file abierto un instante)
    os.replace(tmp, f)
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    días = sorted(p for p in SEPA.glob("dt=*") if p.is_dir())
    print(f"días: {len(días)}")
    for d in días:
        files = sorted(d.glob("precios_*.parquet"))
        filas = 0
        t0 = time.time()
        for f in files:
            con = duckdb.connect()  # conexión efímera por file (gotcha del poisoning)
            try:
                hecho = rewrite(f, con)
                if a.dry:
                    print(f"  {d.name}/{f.name}: {'rewrite' if hecho else 'ok'}")
                    break
                filas += 1 if hecho else 0
            finally:
                con.close()
        if not a.dry:
            print(f"  {d.name}: {len(files)} precios_*.parquet ({filas} reescritos) en {time.time()-t0:.0f}s")
    print("FIN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
