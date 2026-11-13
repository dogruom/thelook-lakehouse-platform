-- One row per (brand_id, product_id). margin_amount is null when cost_price is unknown.
select
    {{ brand_surrogate_key(['brand_id', 'product_id']) }} as product_sk,
    brand_id,
    product_id,
    product_name,
    category,
    retail_price,
    cost_price,
    retail_price - cost_price as margin_amount,
    is_active,
    silver_updated_at
from {{ ref('stg_products') }}
