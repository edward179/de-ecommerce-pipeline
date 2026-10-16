with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

orders as (
    select customer_id, purchased_at from {{ ref('stg_olist__orders') }}
),

latest_address as (   -- address from the most recent order
    select
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state
    from customers c
    join orders o on o.customer_id = c.customer_id
    qualify row_number() over (partition by c.customer_unique_id order by o.purchased_at desc) = 1
),

order_stats as (
    select
        customer_unique_id,
        count(*)          as order_count,
        min(purchased_at) as first_order_at,
        max(purchased_at) as last_order_at,
        sum(order_amount) as lifetime_order_amount
    from {{ ref('fct_orders') }}
    group by customer_unique_id
)

select
    a.*,
    s.order_count,
    s.first_order_at,
    s.last_order_at,
    s.lifetime_order_amount
from latest_address a
left join order_stats s on s.customer_unique_id = a.customer_unique_id
