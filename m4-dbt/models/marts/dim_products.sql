select
    p.product_id,
    p.product_category_name,
    coalesce(t.category_name_english, p.product_category_name, 'unknown') as category_name,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from {{ ref('stg_olist__products') }} p
left join {{ ref('stg_olist__category_translation') }} t
    on t.product_category_name = p.product_category_name
