with source as (
    select * from {{ source('olist', 'products') }}
),

renamed as (
    select
        product_id,
        product_category_name,
        cast(product_name_lenght as integer)        as product_name_length,         -- source column is misspelled "lenght"
        cast(product_description_lenght as integer) as product_description_length,
        cast(product_photos_qty as integer)          as product_photos_qty,
        cast(product_weight_g as double)     as product_weight_g,
        cast(product_length_cm as double)   as product_length_cm,
        cast(product_height_cm as double)  as product_height_cm,
        cast(product_width_cm as double)   as product_width_cm,
        _dlt_load_id
    from source
)

select * from renamed
