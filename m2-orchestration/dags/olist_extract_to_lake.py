"""M2–M4 + M8: 8 olist tables (PostgreSQL) -> Parquet on SeaweedFS (S3) -> dlt -> DuckDB -> dbt -> PostgreSQL serving."""
import io

import pendulum
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

PG_CONN = "olist_pg"
S3_CONN = "lake_s3"
BUCKET = "olist-lake"
TABLES = [
    "customers", "sellers", "products", "orders",
    "order_items", "order_payments", "order_reviews",
    "product_category_name_translation",
]


def _run_date() -> str:
    """Run date (UTC), used as the partition dt=YYYY-MM-DD."""
    return get_current_context()["dag_run"].run_after.strftime("%Y-%m-%d")


@dag(
    dag_id="olist_extract_to_lake",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["m2", "m3", "m4", "m8", "extract", "load", "transform", "serve"],
)
def olist_extract_to_lake():

    @task(retries=2)
    def extract_table(table: str) -> dict:
        # 1) Read the whole table from PostgreSQL
        df = PostgresHook(postgres_conn_id=PG_CONN).get_df(
            f"SELECT * FROM olist.{table}", df_type="pandas"
        )
        # 2) Write Parquet in memory (no local file)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        # 3) Upload to the lake, partitioned by run date
        key = f"olist/{table}/dt={_run_date()}/{table}.parquet"
        S3Hook(aws_conn_id=S3_CONN).load_bytes(
            buf.getvalue(), key=key, bucket_name=BUCKET, replace=True
        )
        return {"table": table, "key": key, "rows": len(df)}

    @task
    def validate(results: list[dict]) -> None:
        pg = PostgresHook(postgres_conn_id=PG_CONN)
        s3 = S3Hook(aws_conn_id=S3_CONN)
        mismatched = []
        for r in results:
            pg_rows = pg.get_first(f"SELECT COUNT(*) FROM olist.{r['table']}")[0]
            body = s3.get_key(r["key"], bucket_name=BUCKET).get()["Body"].read()
            lake_rows = pq.read_metadata(io.BytesIO(body)).num_rows
            status = "OK" if pg_rows == lake_rows else "MISMATCH"
            print(f"{r['table']:<35} postgres={pg_rows:>7} lake={lake_rows:>7} {status}")
            if status != "OK":
                mismatched.append(r["table"])
        if mismatched:
            raise ValueError(f"Row count mismatch: {mismatched}")

    @task.bash(retries=1)
    def load_to_duckdb() -> str:
        """M3: run the dlt pipeline (own venv in the image) for this run's partition."""
        return f"cd /opt/m3 && /opt/dlt-venv/bin/python lake_to_duckdb.py --dt {_run_date()}"

    @task.bash(retries=0)
    def dbt_build() -> str:
        """M4: dbt build (run + test + snapshot) on the freshly loaded warehouse, own venv in the image."""
        return "cd /opt/m4 && /opt/dbt-venv/bin/dbt build --target airflow --profiles-dir ."

    @task.bash(retries=1)
    def publish_marts() -> str:
        """M8: copy analytics_marts -> PostgreSQL serving.marts (DuckDB postgres extension)."""
        return (
            "cd /opt/m8 && PGHOST=de-postgres PGPORT=5432 PGUSER=serving_writer PGDATABASE=serving "
            'PGPASSWORD="$SERVING_WRITER_PASSWORD" '
            "/opt/dbt-venv/bin/python publish_marts.py --duckdb /opt/m3/warehouse/olist.duckdb"
        )

    validate(extract_table.expand(table=TABLES)) >> load_to_duckdb() >> dbt_build() >> publish_marts()


olist_extract_to_lake()
