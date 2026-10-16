select
    order_id,
    count(*)                     as review_count,
    round(avg(review_score), 2)  as review_score_avg
from {{ ref('stg_olist__order_reviews') }}
group by order_id
