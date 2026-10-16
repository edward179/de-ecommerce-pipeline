"""M3: dlt loads Parquet from the lake (SeaweedFS) into DuckDB (schema raw_olist)."""
import argparse

import dlt
from dlt.sources.filesystem import filesystem, read_parquet

TABLES = [
    "customers", "sellers", "products", "orders",
    "order_items", "order_payments", "order_reviews",
    "product_category_name_translation",
]

# Merge keys = M1 primary keys. order_reviews has no PK, so it uses a pair checked to be unique.
PRIMARY_KEYS = {
    "customers": "customer_id",
    "sellers": "seller_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "product_category_name_translation": "product_category_name",
}


def lake_table(table: str, dt: str):
    """One resource = the Parquet files of one table in partition dt, merged on its key."""
    files = filesystem(file_glob=f"olist/{table}/dt={dt}/*.parquet")
    resource = (files | read_parquet(use_pyarrow=True, chunksize=50_000)).with_name(table)
    resource.apply_hints(primary_key=PRIMARY_KEYS[table], write_disposition="merge")
    return resource


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="lake partition, YYYY-MM-DD")
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name="olist_lake_to_duckdb",
        destination=dlt.destinations.duckdb("warehouse/olist.duckdb"),
        dataset_name="raw_olist",
    )
    info = pipeline.run([lake_table(t, args.dt) for t in TABLES])
    print(info)


if __name__ == "__main__":
    main()
