-- Rename/cast only.
select
    brand_id,
    order_id,
    item_id,
    product_id,
    cast(quantity as {{ dbt.type_int() }}) as quantity,
    unit_price,
    line_total,
    {{ adapter.quote('_updated_at') }} as silver_updated_at
from {{ source('silver', 'order_items') }}
