with source as (
    select * from {{ source('olist', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp      at time zone 'UTC' as purchased_at,
        order_approved_at             at time zone 'UTC' as approved_at,
        order_delivered_carrier_date  at time zone 'UTC' as delivered_to_carrier_at,
        order_delivered_customer_date at time zone 'UTC' as delivered_to_customer_at,
        order_estimated_delivery_date at time zone 'UTC' as estimated_delivery_at,
        _dlt_load_id
    from source
)

select * from renamed
