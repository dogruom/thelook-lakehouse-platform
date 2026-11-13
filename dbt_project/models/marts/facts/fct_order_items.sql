-- Grain: (brand_id, order_id, item_id). Surrogate keys are derived from the natural keys (brand_id
-- included), so no dimension join is needed; referential integrity is asserted by tests.
select
    {{ brand_surrogate_key(['i.brand_id', 'i.order_id', 'i.item_id']) }} as order_item_sk,
    {{ brand_surrogate_key(['i.brand_id', 'i.order_id']) }} as order_sk,
    {{ brand_surrogate_key(['i.brand_id', 'i.product_id']) }} as product_sk,
    case
        when o.customer_id is null then cast(null as {{ dbt.type_string() }})
        else {{ brand_surrogate_key(['i.brand_id', 'o.customer_id']) }}
    end as customer_sk,
    i.brand_id,
    i.order_id,
    i.item_id,
    i.product_id,
    o.customer_id,
    {{ date_to_key('o.order_timestamp') }} as order_date_key,
    i.quantity,
    i.unit_price,
    i.line_total
from {{ ref('stg_order_items') }} as i
left join {{ ref('stg_orders') }} as o
    on i.brand_id = o.brand_id
   and i.order_id = o.order_id
