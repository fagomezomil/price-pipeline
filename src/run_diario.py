"""Orquestador diario: SEPA + todos los scrapers, tolerante a fallos, con log y resumen por drift.

Uso: python src/run_diario.py [--date YYYY-MM-DD]
Log: logs/YYYY-MM-DD.log. Salida: 0 si SEPA OK (los scrapers fallidos solo avisar).
"""
import argparse
import logging
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
SRC = ROOT / "src"

# Pausados (2026-10-14, decisión Fede): cordiez, toledo, maxiconsumo y supermami
# salen del ciclo diario — sus crawls consumen ~65, ~20, ~54 y ~20 min.
# Los parquets ya ingeridos siguen en v_scrape.
# Para reactivar: agregar acá, ej ("california", ["src/scrapers/california.py"]).
SCRAPERS = [
    ("california", ["src/scrapers/california.py"]),
    ("supertodo", ["src/scrapers/supertodo.py"]),
    ("lagallega", ["src/scrapers/lagallega.py"]),
]

log = logging.getLogger("diario")


def correr(cmd: list[str], env_extra: dict | None = None) -> tuple[int, str]:
    import os
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if env_extra:
        env.update(env_extra)
    r = subprocess.run([sys.executable] + cmd, capture_output=True, text=True, env=env,
                       cwd=str(ROOT), timeout=7200)
    return r.returncode, (r.stdout or "") + (r.stderr or "")


def contar(fuente: str, cadena: str, dt: str) -> int:
    import duckdb
    base = ROOT.as_posix()
    con = duckdb.connect()  # conexión nueva: un path inexistente aborta la transacción y no contamina
    try:
        if fuente == "sepa":
            return con.sql(
                f"SELECT count(*) FROM read_parquet('{base}/data/sepa/dt={dt}/precios_*.parquet')").fetchone()[0]
        return con.sql(
            f"SELECT count(*) FROM read_parquet('{base}/data/scrape/{cadena}/dt={dt}/precios.parquet')").fetchone()[0]
    except Exception:
        return 0
    finally:
        con.close()


def resumen(dt: str, ayer: str) -> None:
    log.info("=== RESUMEN %s (vs %s) ===", dt, ayer)
    filas = [("sepa", "", dt), ("sepa", "", ayer), ("scrape", "california", dt),
             ("scrape", "supertodo", dt), ("scrape", "lagallega", dt)]
    for fuente, cadena, d in filas:
        n = contar(fuente, cadena, d)
        log.info("  %-11s %-11s %s = %10d", fuente, cadena if cadena else "-", d, n)


def drift(fuente: str, cadena: str, dt: str, prev: str) -> None:
    actual, previo = contar(fuente, cadena, dt), contar(fuente, cadena, prev)
    if actual and previo and abs(actual - previo) / max(previo, 1) > 0.3:
        log.warning("DRIFT >30%% en %s: %d -> %d", cadena or fuente, previo, actual)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    a = ap.parse_args()
    LOGS.mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(LOGS / f"{a.date}.log", encoding="utf-8"),
                                  logging.StreamHandler(sys.stdout)])
    dt, prev = a.date, None
    from datetime import timedelta
    prev = (date.fromisoformat(a.date) - timedelta(days=1)).isoformat()

    rc, out = correr(["src/sepa_daily.py", "--date", dt])
    if rc != 0:
        log.error("SEPA falló (rc=%d): %s", rc, out[-2000:])
        return 1

    fallos = []
    for nombre, cmd in SCRAPERS:
        path = ROOT / cmd[0]
        if not path.exists():
            log.info("[%s] no implementado aún — salteando", nombre)
            continue
        rc, out = correr([*cmd, "--date", dt])
        if rc != 0:
            fallos.append(nombre)
            log.error("[%s] falló (rc=%d): %s", nombre, rc, out[-2000:])
        else:
            for l in out.splitlines()[-3:]:
                log.info("[%s] %s", nombre, l)

    for nombre in ("california", "supertodo", "lagallega"):
        drift("scrape", nombre, dt, prev)
    resumen(dt, prev)
    if fallos:
        log.warning("scrapers fallidos: %s", ", ".join(fallos))

    # capa de calidad (dim_sucursal + v_precio_curado + precio_mediana): si se rompe, la web no tiene data
    rc, out = correr(["src/curado.py", "--date", dt])
    if rc != 0:
        log.error("curado falló (rc=%d): %s", rc, out[-2000:])
        return 1

    # sync a R2 (F5): la web/backup consumen de ahí. NO-fatal: si falla,
    # la web queda con la data de ayer y el push reintenta mañana.
    rc, out = correr(["src/push_r2.py", "--date", dt])
    if rc != 0:
        log.warning("push_r2 falló (rc=%d): %s — la web sigue con la data anterior", rc, out[-1500:])
    else:
        for l in out.splitlines()[-3:]:
            log.info("[push_r2] %s", l)

    return 0


if __name__ == "__main__":
    sys.exit(main())