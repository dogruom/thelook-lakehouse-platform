-- Grain: (brand_id, order_id). Items are aggregated per order and joined on brand_id AND order_id.
with items as (
    select
        brand_id,
        order_id,
        count(*) as item_count,
        sum(quantity) as item_quantity,
        sum(line_total) as items_total
    from {{ ref('stg_order_items') }}
    group by brand_id, order_id
)

select
    {{ brand_surrogate_key(['o.brand_id', 'o.order_id']) }} as order_sk,
    {{ brand_surrogate_key(['o.brand_id', 'o.customer_id']) }} as customer_sk,
    o.brand_id,
    o.order_id,
    o.customer_id,
    {{ date_to_key('o.order_timestamp') }} as order_date_key,
    o.order_timestamp,
    o.order_status,
    o.payment_method,
    o.currency,
    o.sales_channel,
    o.gross_amount,
    o.discount_amount,
    o.net_amount,
    cast(coalesce(i.item_count, 0) as {{ dbt.type_int() }}) as item_count,
    cast(coalesce(i.item_quantity, 0) as {{ dbt.type_bigint() }}) as item_quantity,
    cast(coalesce(i.items_total, 0) as decimal(18, 2)) as items_total
from {{ ref('stg_orders') }} as o
left join items as i
    on o.brand_id = i.brand_id
   and o.order_id = i.order_id
