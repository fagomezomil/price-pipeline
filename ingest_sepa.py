# Ingesta SEPA -> DuckDB. Uso: python ingest_sepa.py <carpeta_full> <db_salida>
# Los CSV de SEPA traen líneas basura ('Ultima actualizacion: ...' colado): se filtra id_comercio numérico.
import duckdb

src = "sepa/martes/full"
db = "sepa/sepa_martes.db"

con = duckdb.connect(db)
for t in ("comercio", "sucursales", "productos"):
    con.execute("DROP TABLE IF EXISTS " + t)

con.execute(f"""CREATE TABLE comercio AS
    SELECT id_comercio, id_bandera, comercio_cuit, comercio_razon_social, comercio_bandera_nombre, comercio_ultima_actualizacion
    FROM read_csv_auto('{src}/*/comercio.csv', sep='|', header=true, strict_mode=false, ignore_errors=true, null_padding=true, all_varchar=true)
    WHERE regexp_matches(id_comercio, '^[0-9]+$')""")

con.execute(f"""CREATE TABLE sucursales AS
    SELECT id_comercio, id_bandera, id_sucursal, sucursales_nombre, sucursales_tipo, sucursales_calle,
           sucursales_numero, sucursales_latitud, sucursales_longitud, sucursales_localidad, sucursales_provincia
    FROM read_csv_auto('{src}/*/sucursales.csv', sep='|', header=true, strict_mode=false, ignore_errors=true, null_padding=true, all_varchar=true)
    WHERE regexp_matches(id_comercio, '^[0-9]+$')""")

con.execute(f"""CREATE TABLE productos AS
    SELECT id_comercio, id_bandera, id_sucursal, id_producto, productos_ean, productos_descripcion,
           productos_cantidad_presentacion, productos_unidad_medida_presentacion, productos_marca,
           productos_precio_lista, productos_precio_unitario_promo1, productos_precio_unitario_promo2
    FROM read_csv_auto('{src}/*/productos.csv', sep='|', header=true, strict_mode=false, ignore_errors=true, null_padding=true, all_varchar=true)
    WHERE regexp_matches(id_comercio, '^[0-9]+$')""")

con.execute("CREATE INDEX IF NOT EXISTS ix_p_ean ON productos(productos_ean)")

print("comercio:", con.sql("SELECT count(*) FROM comercio").fetchone()[0])
print("sucursales:", con.sql("SELECT count(*) FROM sucursales").fetchone()[0])
print("productos:", con.sql("SELECT count(*) FROM productos").fetchone()[0])
print("cadenas:")
for r in con.sql("SELECT id_comercio, comercio_bandera_nombre, count(*) FROM comercio GROUP BY 1,2 ORDER BY 1").fetchall():
    print(" ", r)