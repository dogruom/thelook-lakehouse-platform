-- Grain: (brand_id, customer_id), every customer of dim_customers (tombstones kept, flagged).
-- One Big Table for Customer 360: current customer attributes + order and session metrics up to
-- the end of var('as_of_date') (inclusive day, UTC). Same as_of_date + same inputs -> same output
-- (no current_date / now()). Valid orders = completed + pending; cancelled and returned are counted
-- separately and excluded from revenue (D-097). Every join is on brand_id AND customer_id.
-- Limitation: customer attributes are the current Silver state, not the state at as_of_date.
-- Column names follow DATA_CONTRACTS §5.2; RFM scores live in agg_rfm_segments (built FROM this
-- table, so they cannot be columns here without a cycle; join on brand_id + customer_id).
{%- set as_of_date = var('as_of_date') %}
{%- set as_of_end = dbt.dateadd('day', 1, "cast('" ~ as_of_date ~ "' as timestamp)") %}

with orders as (
    select
        brand_id,
        customer_id,
        count(*) as order_count,
        sum(case when order_status in ('completed', 'pending') then 1 else 0 end) as valid_order_count,
        sum(case when order_status = 'cancelled' then 1 else 0 end) as cancelled_order_count,
        sum(case when order_status = 'returned' then 1 else 0 end) as returned_order_count,
        sum(case when order_status in ('completed', 'pending') then net_amount else 0 end) as net_revenue,
        min(order_timestamp) as first_order_at,
        max(case when order_status in ('completed', 'pending') then order_timestamp end) as last_valid_order_at
    from {{ ref('fct_orders') }}
    where order_timestamp < {{ as_of_end }}
    group by brand_id, customer_id
),

sessions as (
    select
        brand_id,
        customer_id,
        count(*) as session_count,
        sum(case when converted then 1 else 0 end) as converted_session_count,
        sum(page_views) as page_views,
        sum(add_to_carts) as cart_additions,
        max(session_start) as last_session_at
    from {{ ref('fct_sessions') }}
    where customer_id is not null
      and session_start < {{ as_of_end }}
    group by brand_id, customer_id
)

select
    c.customer_sk,
    c.brand_id,
    c.customer_id,
    c.email_token,
    c.city,
    c.country,
    c.loyalty_tier,
    c.signup_date,
    c.is_deleted,
    cast(
        {{ dbt.datediff('c.signup_date', "cast('" ~ as_of_date ~ "' as date)", 'day') }}
        as {{ dbt.type_int() }}
    ) as tenure_days,
    cast(coalesce(o.order_count, 0) as {{ dbt.type_int() }}) as order_count,
    cast(coalesce(o.valid_order_count, 0) as {{ dbt.type_int() }}) as frequency_orders,
    cast(coalesce(o.cancelled_order_count, 0) as {{ dbt.type_int() }}) as cancelled_order_count,
    cast(coalesce(o.returned_order_count, 0) as {{ dbt.type_int() }}) as returned_order_count,
    cast(coalesce(o.net_revenue, 0) as decimal(12, 2)) as monetary_total_spend,
    cast(
        case when o.valid_order_count > 0 then o.net_revenue / o.valid_order_count end
        as decimal(10, 2)
    ) as avg_order_value,
    o.first_order_at as first_order_ts,
    o.last_valid_order_at as last_order_ts,
    cast(
        case
            when o.last_valid_order_at is not null
                then {{ dbt.datediff("cast(o.last_valid_order_at as date)", "cast('" ~ as_of_date ~ "' as date)", 'day') }}
        end as {{ dbt.type_int() }}
    ) as recency_days,
    cast(
        case
            when o.last_valid_order_at is not null
                then {{ whole_hours_between('o.last_valid_order_at', as_of_end) }}
        end as {{ dbt.type_bigint() }}
    ) as recency_hours,
    cast(coalesce(s.session_count, 0) as {{ dbt.type_int() }}) as session_count,
    cast(coalesce(s.converted_session_count, 0) as {{ dbt.type_int() }}) as converted_session_count,
    cast(coalesce(s.page_views, 0) as {{ dbt.type_int() }}) as total_page_views,
    cast(coalesce(s.cart_additions, 0) as {{ dbt.type_int() }}) as total_cart_additions,
    s.last_session_at,
    cast('{{ as_of_date }}' as date) as as_of_date
from {{ ref('dim_customers') }} as c
left join orders as o
    on c.brand_id = o.brand_id
   and c.customer_id = o.customer_id
left join sessions as s
    on c.brand_id = s.brand_id
   and c.customer_id = s.customer_id
