"""M8: copy analytics_marts (DuckDB warehouse) -> PostgreSQL serving.marts for Metabase.

PostgreSQL connection comes from the libpq env vars: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE.
"""
import argparse
import os
import sys

import duckdb

TABLES = ["fct_orders", "fct_order_items", "dim_customers", "dim_products", "dim_sellers"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True, help="path to olist.duckdb")
    args = parser.parse_args()

    # In-memory DuckDB used only as a bridge. In Airflow the extension is preinstalled in the image
    # (DUCKDB_EXTENSION_DIR); a manual run uses the default ~/.duckdb
    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIR")
    con = duckdb.connect(config={"extension_directory": ext_dir} if ext_dir else {})
    con.execute("INSTALL postgres")             # no-op if already installed
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{args.duckdb}' AS wh (READ_ONLY)")
    con.execute("ATTACH '' AS pg (TYPE postgres)")   # empty string = read everything from PG* env vars

    # One transaction: on failure PostgreSQL keeps the old tables, so the dashboard never sees empty ones
    con.execute("BEGIN")
    for t in TABLES:
        con.execute(f"DROP TABLE IF EXISTS pg.marts.{t}")
        con.execute(f"CREATE TABLE pg.marts.{t} AS SELECT * FROM wh.analytics_marts.{t}")
    con.execute("COMMIT")

    mismatched = []
    for t in TABLES:
        src = con.execute(f"SELECT count(*) FROM wh.analytics_marts.{t}").fetchone()[0]
        dst = con.execute(f"SELECT count(*) FROM pg.marts.{t}").fetchone()[0]
        status = "OK" if src == dst else "MISMATCH"
        print(f"{t:<18} duckdb={src:>7} postgres={dst:>7} {status}")
        if status != "OK":
            mismatched.append(t)
    if mismatched:
        sys.exit(f"Row count mismatch: {mismatched}")


if __name__ == "__main__":
    main()
