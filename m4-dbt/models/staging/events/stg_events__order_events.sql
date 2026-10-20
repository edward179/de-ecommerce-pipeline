with source as (
    select * from {{ source('events', 'order_events') }}
),

renamed as (
    select
        event_id,
        event_type,
        event_ts at time zone 'UTC' as event_at,
        order_id,
        customer_unique_id,
        product_id,
        cast(amount as decimal(10, 2)) as amount,
        payment_type,
        schema_version,
        _kafka_partition,
        _kafka_offset,
        _dlt_load_id
    from source
)

select * from renamed
