-- Proves: per brand, Gold facts lose and duplicate nothing versus staging: row counts of
-- fct_orders / fct_order_items / fct_sessions equal their staging grain, and sum(net_amount) of
-- fct_orders equals stg_orders (null sums count as 0 on both sides). Returns one row per
-- (brand, check) that differs, plus one row per check for which staging has no rows at all
-- (an empty staging must never pass as "both sides empty").
-- Limits: counts and one amount only; it does not compare column values. A full outer join keeps
-- brands that exist on only one side, so an empty mart cannot pass against non-empty staging.
with checks as (
    select 'orders_rows' as check_name, brand_id, cast(count(*) as decimal(28, 2)) as staging_value
    from {{ ref('stg_orders') }} group by brand_id
    union all
    select 'order_items_rows', brand_id, cast(count(*) as decimal(28, 2))
    from {{ ref('stg_order_items') }} group by brand_id
    union all
    select 'sessions_rows', brand_id, cast(count(distinct session_id) as decimal(28, 2))
    from {{ ref('stg_clickstream') }} group by brand_id
    union all
    select 'net_amount_sum', brand_id, cast(coalesce(sum(net_amount), 0) as decimal(28, 2))
    from {{ ref('stg_orders') }} group by brand_id
),

marts as (
    select 'orders_rows' as check_name, brand_id, cast(count(*) as decimal(28, 2)) as mart_value
    from {{ ref('fct_orders') }} group by brand_id
    union all
    select 'order_items_rows', brand_id, cast(count(*) as decimal(28, 2))
    from {{ ref('fct_order_items') }} group by brand_id
    union all
    select 'sessions_rows', brand_id, cast(count(*) as decimal(28, 2))
    from {{ ref('fct_sessions') }} group by brand_id
    union all
    select 'net_amount_sum', brand_id, cast(coalesce(sum(net_amount), 0) as decimal(28, 2))
    from {{ ref('fct_orders') }} group by brand_id
),

expected_checks as (
    select 'orders_rows' as check_name
    union all select 'order_items_rows'
    union all select 'sessions_rows'
    union all select 'net_amount_sum'
),

empty_staging as (
    select
        'staging_empty:' || e.check_name as check_name,
        cast(null as {{ dbt.type_string() }}) as brand_id,
        cast(null as decimal(28, 2)) as staging_value,
        cast(null as decimal(28, 2)) as mart_value
    from expected_checks as e
    where not exists (select 1 from checks as c where c.check_name = e.check_name)
),

diffs as (
    select
        coalesce(c.check_name, m.check_name) as check_name,
        coalesce(c.brand_id, m.brand_id) as brand_id,
        c.staging_value,
        m.mart_value
    from checks as c
    full outer join marts as m
        on c.check_name = m.check_name
       and c.brand_id = m.brand_id
    where c.staging_value is null
       or m.mart_value is null
       or c.staging_value <> m.mart_value
)

select * from diffs
union all
select * from empty_staging
