with source as (
    select * from {{ source('olist', 'order_payments') }}
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        cast(payment_value as decimal(10, 2)) as payment_value,
        _dlt_load_id
    from source
)

select * from renamed