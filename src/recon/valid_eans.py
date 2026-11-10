"""_valid_eans.py — ¿cuántos de los EANs faltantes son GTIN GS1 reales (check digit)?"""

def es_valido13(s: str) -> bool:
    s = str(s).rjust(13, "0")
    if not s.isdigit() or len(s) != 13:
        return False
    # check digit EAN-13
    total = sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(s[:12]))
    return (10 - total % 10) % 10 == int(s[12])

import duckdb

con = duckdb.connect(r"D:\Proyectos\precios\data\duckdb\precios.duckdb", read_only=True)
falt = [str(r[0]) for r in con.execute("""
    SELECT DISTINCT p.ean
    FROM v_precio_curado p LEFT JOIN dim_imagen d ON p.ean = d.ean
    WHERE p.dt = '2026-11-09' AND d.ean IS NULL
""").fetchall()]
con.close()

reales = [e for e in falt if es_valido13(e)]
print(f"faltantes del día: {len(falt):,}")
print(f"  GTIN válidos (check digit ok): {len(reales):,} ({len(reales)*100/max(len(falt),1):.1f}%)")
print(f"  códigos internos/código no-estándar: {len(falt)-len(reales):,} ({(len(falt)-len(reales))*100/max(len(falt),1):.1f}%)")
print("\nmuestras de GTIN válidos:")
for e in reales[:10]:
    print("  ", e)
