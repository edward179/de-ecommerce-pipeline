"""M3: compare row counts, PostgreSQL source vs DuckDB raw_olist, after a load."""
import subprocess

import duckdb

TABLES = [
    "customers", "sellers", "products", "orders",
    "order_items", "order_payments", "order_reviews",
    "product_category_name_translation",
]


def pg_count(table: str) -> int:
    """Count rows in the M1 PostgreSQL via psql inside the de-postgres container."""
    out = subprocess.run(
        ["docker", "exec", "de-postgres", "psql", "-U", "olist", "-d", "olist",
         "-Atc", f"SELECT count(*) FROM olist.{table}"],
        capture_output=True, text=True, check=True,
    ).stdout
    return int(out)


def main() -> None:
    con = duckdb.connect("warehouse/olist.duckdb", read_only=True)
    print(f"{'table':<35}{'postgres':>10}{'duckdb':>10}  status")
    for t in TABLES:
        pg = pg_count(t)
        dk = con.execute(f"SELECT count(*) FROM raw_olist.{t}").fetchone()[0]
        print(f"{t:<35}{pg:>10}{dk:>10}  {'OK' if pg == dk else 'MISMATCH'}")
    last = con.execute(
        "SELECT load_id, inserted_at FROM raw_olist._dlt_loads ORDER BY inserted_at DESC LIMIT 1"
    ).fetchone()
    print(f"\nlast dlt load: {last[0]} at {last[1]:%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    main()
