-- Spark output must match the warehouse: same customers and same revenue as fct_orders
{{ config(enabled=target.name != 'airflow') }}

with expected as (
    select count(distinct customer_unique_id) as customers, sum(order_amount) as revenue
    from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable') and order_amount > 0
),

actual as (
    select count(*) as customers, sum(monetary) as revenue
    from {{ ref('customer_rfm') }}
)

select *
from expected, actual
where expected.customers != actual.customers
   or expected.revenue   != actual.revenue
