FROM apache/airflow:3.3.2-python3.12

# Separate venv for dlt (M3), so Airflow's own packages stay untouched
USER root
COPY --from=m3 requirements.txt /tmp/m3-requirements.txt
RUN python -m venv /opt/dlt-venv \
 && /opt/dlt-venv/bin/pip install --no-cache-dir -r /tmp/m3-requirements.txt \
 && rm /tmp/m3-requirements.txt

# Separate venv for dbt (M4)
COPY --from=m4 requirements.txt /tmp/m4-requirements.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/m4-requirements.txt \
 && rm /tmp/m4-requirements.txt

# DuckDB postgres extension for M8 publish_marts: installed at build time, so tasks need no network
RUN /opt/dbt-venv/bin/python -c "import duckdb; duckdb.connect(config={'extension_directory': '/opt/duckdb-ext'}).execute('INSTALL postgres')" \
 && chmod -R a+rX /opt/duckdb-ext
USER airflow
