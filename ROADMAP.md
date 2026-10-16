# ROADMAP: M1 → M8

Order: the end-to-end batch MVP first (M1 → M4 + M8), then M6, M7 and M5.
A module is "done" when it runs and has 1–2 screenshots in `<module>/images/`.

| # | Folder | Scope | Tools | Status |
|---|---|---|---|---|
| M1 | `m1-infra/` | Docker Engine (WSL2), PostgreSQL as the OLTP source, load the dataset | Docker, PostgreSQL, SQL | ✅ |
| M2 | `m2-orchestration/` | DAG: extract PostgreSQL → data lake (Parquet) | Airflow 3.3, SeaweedFS | ✅ |
| M3 | `m3-ingest-warehouse/` | dlt: load lake → warehouse, incremental / merge | dlt, DuckDB | ✅ |
| M4 | `m4-dbt/` | staging → intermediate → marts, tests, docs, incremental, snapshot | dbt Core 1.12 + dbt-duckdb | ✅ |
| M8 | `m8-dashboard/` | Publish marts to a PostgreSQL `serving` DB + revenue / orders dashboard | DuckDB `postgres` extension, PostgreSQL, Metabase OSS 0.64 | ✅ |
| M6 | `m6-spark/` | One PySpark batch job (e.g. customer RFM) | PySpark 4.1 local | ⬜ |
| M7 | `m7-streaming/` | Faker events → Kafka → consumer → warehouse | Kafka 4.3, Kafbat UI | ⬜ |
| M5 | `m5-bruin/` | Optional: rebuild 1–2 models with Bruin and compare with dbt | Bruin CLI | ⬜ |
