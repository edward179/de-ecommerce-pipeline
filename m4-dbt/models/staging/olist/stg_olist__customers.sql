with source as (
    select * from {{ source('olist', 'customers') }}
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _dlt_load_id
    from source
)

select * from renamed