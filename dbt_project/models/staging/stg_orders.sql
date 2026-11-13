-- Rename/cast only.
select
    brand_id,
    order_id,
    customer_id,
    order_timestamp,
    order_status,
    payment_method,
    currency,
    gross_amount,
    discount_amount,
    net_amount,
    sales_channel,
    {{ adapter.quote('_updated_at') }} as silver_updated_at
from {{ source('silver', 'orders') }}
