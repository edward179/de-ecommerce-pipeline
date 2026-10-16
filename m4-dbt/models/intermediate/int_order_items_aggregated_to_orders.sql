select
    order_id,
    count(*)                   as item_count,
    count(distinct product_id) as product_count,
    count(distinct seller_id)  as seller_count,
    sum(price)                 as items_amount,
    sum(freight_value)         as freight_amount
from {{ ref('stg_olist__order_items') }}
group by order_id
