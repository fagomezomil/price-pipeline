import duckdb

con = duckdb.connect("D:/Proyectos/precios/data/duckdb/precios.duckdb", read_only=True)

def q(s):
    return con.sql(s).fetchall()

print("=== NIVEL 1: precios del día ===")
dt = q("SELECT CAST(max(dt) AS VARCHAR) FROM precio_mediana")[0][0]
n_dia = q(f"SELECT count(*) FROM v_precio_curado WHERE CAST(dt AS VARCHAR) = '{dt}'")[0][0]
eans_dia = q(f"SELECT count(DISTINCT ean) FROM v_precio_curado WHERE CAST(dt AS VARCHAR) = '{dt}'")[0][0]
combos_dia = q(
    f"SELECT count(*) FROM (SELECT DISTINCT ean, sucursal_id FROM v_precio_curado WHERE CAST(dt AS VARCHAR) = '{dt}')"
)[0][0]
print(f"dt={dt}: filas={n_dia:,} · EANs únicos={eans_dia:,} · combos ean×sucursal={combos_dia:,}")

print("\n=== NIVEL 2: histórico completo (todos los dt) ===")
hist_ean = q("SELECT count(DISTINCT ean) FROM v_precio_curado")[0][0]
hist_filas = q("SELECT count(*) FROM v_precio_curado")[0][0]
print(f"filas acumuladas={hist_filas:,} · EANs únicos históricos={hist_ean:,} · dts={q('SELECT count(DISTINCT CAST(dt AS VARCHAR)) FROM precio_mediana')[0][0]}")

print("\n=== IMÁGENES ===")
sc = q(
    "SELECT count(DISTINCT ean), count(DISTINCT CASE WHEN imagen_url IS NOT NULL AND imagen_url != '' THEN ean END) FROM v_scrape"
)[0]
print(f"v_scrape: {sc[0]:,} EANs únicos · con imagen_url: {sc[1]:,} ({100*sc[1]/max(sc[0],1):.1f}% del scrape)")
print(f"cobertura imagen vs únicos históricos: {100*sc[1]/hist_ean:.2f}%")

con.close()
