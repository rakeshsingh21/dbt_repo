select
    id as customer_id,
    first_name,
    last_name
from {{ source('customer_orders', 'customers') }}
/*from raw.jaffle_shop.customers */