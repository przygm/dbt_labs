select
    orders.order_id
from {{ ref('stg_jaffle_shop__orders') }} as orders
where orders.status = 'completed'
    and not exists (
        select 1
        from {{ ref('stg_stripe__payments') }} payments
        where payments.order_id = orders.order_id
            and payments.status = 'success'
    )