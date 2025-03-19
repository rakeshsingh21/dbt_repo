select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from {{ source('customer_orders', 'orders') }}
/* from raw.jaffle_shop.orders */