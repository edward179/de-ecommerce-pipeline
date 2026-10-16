with source as (
    select * from {{ source('olist', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date at time zone 'UTC' as shipping_limit_at,
        cast(price as decimal(10, 2)) as price,
        cast(freight_value as decimal(10, 2)) as freight_value,
        _dlt_load_id
    from source
)

select * from renamed