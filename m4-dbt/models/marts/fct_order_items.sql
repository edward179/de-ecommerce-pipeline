{{
  config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}

with items as (
    select * from {{ ref('stg_olist__order_items') }}
    {% if is_incremental() %}
    -- only rows from dlt loads newer than the last processed one
    where cast(_dlt_load_id as double) > (select max(cast(_dlt_load_id as double)) from {{ this }})
    {% endif %}
),

orders   as (select order_id, order_status, purchased_at from {{ ref('stg_olist__orders') }}),
products as (select product_id, category_name from {{ ref('dim_products') }})

select
    md5(i.order_id || '|' || cast(i.order_item_id as varchar)) as order_item_key,
    i.order_id,
    i.order_item_id,
    i.product_id,
    p.category_name,
    i.seller_id,
    o.order_status,
    o.purchased_at,
    cast(o.purchased_at as date)  as purchase_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value     as item_amount,
    i._dlt_load_id
from items i
left join orders   o on o.order_id   = i.order_id
left join products p on p.product_id = i.product_id
