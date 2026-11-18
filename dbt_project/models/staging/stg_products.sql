-- Rename/cast only. cost_price is nullable.
select
    brand_id,
    product_id,
    product_name,
    category,
    retail_price,
    cost_price,
    cast(is_active as boolean) as is_active,
    {{ adapter.quote('_updated_at') }} as silver_updated_at
from {{ source('silver', 'products') }}
