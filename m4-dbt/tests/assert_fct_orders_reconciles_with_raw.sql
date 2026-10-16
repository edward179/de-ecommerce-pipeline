-- Reconcile the mart with raw: any returned row = test failure
with raw as (
    select
        (select count(*) from {{ source('olist', 'orders') }})                     as order_count,
        (select sum(price + freight_value) from {{ source('olist', 'order_items') }}) as order_amount
),

mart as (
    select count(*) as order_count, sum(order_amount) as order_amount
    from {{ ref('fct_orders') }}
)

select
    raw.order_count   as raw_order_count,
    mart.order_count  as mart_order_count,
    raw.order_amount  as raw_order_amount,
    mart.order_amount as mart_order_amount
from raw, mart
where raw.order_count != mart.order_count
   or abs(raw.order_amount - mart.order_amount) > 0.01
