"""M6: customer RFM segmentation with PySpark (local mode), next to the DuckDB warehouse."""
import os
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
WAREHOUSE = HERE.parent / "m3-ingest-warehouse" / "warehouse" / "olist.duckdb"
IN_FILE = HERE / "data" / "in" / "fct_orders.parquet"
OUT_DIR = HERE / "data" / "out" / "customer_rfm"
EXCLUDED_STATUSES = ["canceled", "unavailable"]   # same revenue rule as the dashboard
QUINTILES = [0.2, 0.4, 0.6, 0.8]


def export_orders() -> None:
    """Copy the order fact to Parquet. Read-only, so the warehouse is held only for this step."""
    IN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        con.execute(f"""
            copy (
                select order_id, customer_unique_id, order_status, purchase_date, order_amount
                from analytics_marts.fct_orders
            ) to '{IN_FILE}' (format parquet)
        """)


def bucket(col, cutoffs):
    """Score 1-5 from 4 cutoffs. Equal values always get the same score (unlike ntile)."""
    c1, c2, c3, c4 = cutoffs
    return (F.when(col <= c1, 1).when(col <= c2, 2).when(col <= c3, 3)
            .when(col <= c4, 4).otherwise(5))


def main() -> None:
    export_orders()

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")   # Spark UI and ports on localhost only
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("olist-rfm")
             .config("spark.driver.memory", "1g")
             .config("spark.ui.port", "14040")
             .config("spark.sql.shuffle.partitions", "4")   # default 200 is sized for clusters
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    orders = (spark.read.parquet(str(IN_FILE))
              .where(~F.col("order_status").isin(EXCLUDED_STATUSES) & (F.col("order_amount") > 0)))

    # Reference date = day after the last order (the dataset ends in 2018, so not "today")
    ref_date = orders.agg(F.date_add(F.max("purchase_date"), 1)).first()[0]

    customers = (orders.groupBy("customer_unique_id")
                 .agg(F.datediff(F.lit(ref_date), F.max("purchase_date")).alias("recency_days"),
                      F.count("*").alias("frequency"),
                      F.sum("order_amount").alias("monetary"))
                 .cache())   # used 3 times below: 2 quantiles + the final write

    r_cut = customers.approxQuantile("recency_days", QUINTILES, 0.001)
    m_cut = customers.approxQuantile("monetary", QUINTILES, 0.001)

    rfm = (customers
           .withColumn("r_score", F.lit(6) - bucket(F.col("recency_days"), r_cut))   # fewer days = better
           .withColumn("f_score", F.least(F.col("frequency"), F.lit(3)))            # 97% buy once: 1 / 2 / 3+
           .withColumn("m_score", bucket(F.col("monetary"), m_cut))
           .withColumn("segment",
                       F.when((F.col("f_score") >= 2) & (F.col("r_score") >= 4), "Champions")
                        .when(F.col("f_score") >= 2, "Loyal")
                        .when((F.col("r_score") >= 4) & (F.col("m_score") >= 4), "Promising")
                        .when(F.col("r_score") >= 4, "New")
                        .when(F.col("r_score") >= 2, "At risk")
                        .otherwise("Lost"))
           .withColumn("reference_date", F.lit(ref_date)))

    # One file is enough for ~95k rows and easier to read from dbt
    rfm.coalesce(1).write.mode("overwrite").parquet(str(OUT_DIR))

    print("Reference date:", ref_date)
    print("Recency cutoffs (days):", r_cut)
    print("Monetary cutoffs:", m_cut)
    result = spark.read.parquet(str(OUT_DIR))
    print("Rows written:", result.count())
    (result.groupBy("segment")
     .agg(F.count("*").alias("customers"), F.round(F.avg("monetary"), 2).alias("avg_monetary"))
     .orderBy(F.desc("customers")).show())
    spark.stop()


if __name__ == "__main__":
    main()
