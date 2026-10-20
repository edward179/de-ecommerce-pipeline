-- Built by hand after the Spark job; the Airflow container has no Spark output, so skip it there
{{ config(enabled=target.name != 'airflow') }}

select
    customer_unique_id,
    recency_days,
    frequency,
    cast(monetary as decimal(18, 2)) as monetary,
    r_score,
    f_score,
    m_score,
    segment,
    reference_date
from {{ source('spark', 'customer_rfm') }}
