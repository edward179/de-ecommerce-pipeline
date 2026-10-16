select
    order_id,
    count(*)                              as payment_count,
    sum(payment_value)                    as payment_amount,
    max(payment_installments)             as max_installments,
    arg_max(payment_type, payment_value)  as main_payment_type   -- payment type with the largest amount
from {{ ref('stg_olist__order_payments') }}
group by order_id
