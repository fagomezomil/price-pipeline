"""Verificación del wire de imágenes: EANs en dim_imagen ∩ v_precio_curado,
para abrir PDP en dev (localhost:3000) y/o la URL pública del webp.
Solo lectura de duckdb. NO usa red.
Uso: python src/_check_pdp_img.py
"""
import duckdb

DB = "data/duckdb/precios.duckdb"
IMG_BASE = "https://pub-c478db7ccd01488da4952346e75618c5.r2.dev"

con = duckdb.connect(DB, read_only=True)
try:
    dt = con.execute("SELECT max(dt) FROM v_precio_sepa").fetchone()[0]
    print(f"dt servido: {dt}")

    try:
        n = con.execute("SELECT count(*) FROM dim_imagen").fetchone()[0]
        print(f"dim_imagen: {n} filas")
        overlap = con.execute(
            """
            SELECT d.ean, d.key
            FROM dim_imagen d
            JOIN (SELECT DISTINCT ean FROM v_precio_curado WHERE dt = ?) c USING (ean)
            ORDER BY d.ean
            LIMIT 10
            """,
            [dt],
        ).fetchall()
        print(f"overlap dim_imagen ∩ curado(dt={dt}): {len(overlap)} mostrando hasta 10")
        for ean, key in overlap:
            n_suc = con.execute(
                "SELECT count(DISTINCT sucursal_id) FROM v_precio_curado WHERE dt = ? AND ean = ?",
                [dt, ean],
            ).fetchone()[0]
            ar_t = con.execute(
                """
                SELECT count(DISTINCT sucursal_id) FROM v_precio_curado
                WHERE dt = ? AND ean = ? AND provincia = 'AR-T'
                """,
                [dt, ean],
            ).fetchone()[0]
            desc = con.execute(
                f'SELECT descripcion FROM v_precio_curado WHERE dt = ? AND ean = ? LIMIT 1',
                [dt, ean],
            ).fetchone()[0]
            print(f"  EAN {ean} | {desc[:48]} | suc nac {n_suc} | AR-T {ar_t}")
            print(f"    PDP   : http://localhost:3000/producto/{ean}")
            print(f"    imagen: {IMG_BASE}/{key}")
    finally:
        pass
finally:
    con.close()
print("FIN")
