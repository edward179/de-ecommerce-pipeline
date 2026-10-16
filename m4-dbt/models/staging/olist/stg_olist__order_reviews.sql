with source as (
    select * from {{ source('olist', 'order_reviews') }}
),

renamed as (
    select
        md5(review_id || '|' || order_id) as review_order_key,  -- surrogate key for (review_id, order_id)
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date    at time zone 'UTC' as review_created_at,
        review_answer_timestamp at time zone 'UTC' as review_answered_at,
        _dlt_load_id
    from source
)

select * from renamed
