"""_probe_schemas.py — esquemas de precios_*.parquet por dt (detectar variantes)."""
import duckdb
from pathlib import Path
import collections

ROOT = Path(__file__).resolve().parent.parent
SEPA = ROOT / "data" / "sepa"
con = duckdb.connect()

variants = collections.defaultdict(list)
for d in sorted(p for p in SEPA.glob("dt=*") if p.is_dir()):
    # muestras 1 archivo por día
    fs = sorted(d.glob("precios_*.parquet"))
    sig = None
    if fs:
        cols = tuple(c[0] for c in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(fs[0])]).description)
        # chequear todos los files del día iguales
        dif = []
        for f in fs[1:]:
            c2 = tuple(x[0] for x in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(f)]).description)
            if c2 != cols:
                dif.append(f.name)
        sig = cols
        print(f"{d.name}: {len(fs)} files, cols={cols}")
        if dif:
            print(f"   !! files con esquema distinto: {dif}")
    variants[sig].append(d.name)

print("\nvariantes:", {tuple(k): v for k, v in variants.items()})
