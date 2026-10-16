with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select customer_id, customer_unique_id from {{ ref('stg_olist__customers') }}
),

items    as (select * from {{ ref('int_order_items_aggregated_to_orders') }}),
payments as (select * from {{ ref('int_order_payments_aggregated_to_orders') }}),
reviews  as (select * from {{ ref('int_order_reviews_aggregated_to_orders') }})

select
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    o.purchased_at,
    cast(o.purchased_at as date)                                  as purchase_date,
    o.approved_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,
    date_diff('day', o.purchased_at, o.delivered_to_customer_at)  as delivery_days,
    o.delivered_to_customer_at > o.estimated_delivery_at          as is_late,  -- NULL if not delivered yet
    coalesce(i.item_count, 0)                                     as item_count,
    coalesce(i.seller_count, 0)                                   as seller_count,
    coalesce(i.items_amount, 0)                                   as items_amount,
    coalesce(i.freight_amount, 0)                                 as freight_amount,
    coalesce(i.items_amount, 0) + coalesce(i.freight_amount, 0)   as order_amount,
    coalesce(p.payment_amount, 0)                                 as payment_amount,
    p.payment_count,
    p.main_payment_type,
    p.max_installments,
    r.review_count,
    r.review_score_avg
from orders o
left join customers c on c.customer_id = o.customer_id
left join items     i on i.order_id    = o.order_id
left join payments  p on p.order_id    = o.order_id
left join reviews   r on r.order_id    = o.order_id
