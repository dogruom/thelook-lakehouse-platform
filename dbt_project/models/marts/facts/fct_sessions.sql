-- Grain: (brand_id, session_id). customer_id is the non-null customer of the session, if any
-- (a singular test asserts a session never carries more than one); null = anonymous session.
with sessions as (
    select
        brand_id,
        session_id,
        max(customer_id) as customer_id,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        count(*) as event_count,
        sum(case when event_type = 'page_view' then 1 else 0 end) as page_views,
        sum(case when event_type = 'search' then 1 else 0 end) as searches,
        sum(case when event_type = 'add_to_cart' then 1 else 0 end) as add_to_carts,
        sum(case when event_type = 'checkout' then 1 else 0 end) as checkouts,
        sum(case when event_type = 'purchase' then 1 else 0 end) as purchases
    from {{ ref('stg_clickstream') }}
    group by brand_id, session_id
)

select
    {{ brand_surrogate_key(['brand_id', 'session_id']) }} as session_sk,
    case
        when customer_id is null then cast(null as {{ dbt.type_string() }})
        else {{ brand_surrogate_key(['brand_id', 'customer_id']) }}
    end as customer_sk,
    brand_id,
    session_id,
    customer_id,
    {{ date_to_key('session_start') }} as session_date_key,
    session_start,
    session_end,
    cast({{ dbt.datediff('session_start', 'session_end', 'second') }} as {{ dbt.type_bigint() }}) as duration_seconds,
    cast(event_count as {{ dbt.type_int() }}) as event_count,
    cast(page_views as {{ dbt.type_int() }}) as page_views,
    cast(searches as {{ dbt.type_int() }}) as searches,
    cast(add_to_carts as {{ dbt.type_int() }}) as add_to_carts,
    cast(checkouts as {{ dbt.type_int() }}) as checkouts,
    cast(purchases as {{ dbt.type_int() }}) as purchases,
    purchases > 0 as converted
from sessions
