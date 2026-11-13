-- Proves: dim_date is a correct, gap-free calendar: known ISO weekdays, is_weekend consistent with
-- day_of_week, row count and min/max equal to the configured range, date_key = yyyymmdd for every
-- row. Returns one row per violation. Limits: spot-checks four dates, not every weekday.
with d as (
    select * from {{ ref('dim_date') }}
),

known as (
    select '2026-11-13' as date_str, 5 as expected_dow
    union all select '2026-11-15', 7
    union all select '2026-11-16', 1
    union all select '2026-01-01', 4
),

bounds as (
    select
        count(*) as n_rows,
        -- min/max over a string cast: a bare min/max(date) is answered from Delta file stats by the
        -- metadata-only optimiser, which fails on the Thrift Server (LocalDate -> java.sql.Date).
        cast(min(cast(date_day as {{ dbt.type_string() }})) as date) as min_day,
        cast(max(cast(date_day as {{ dbt.type_string() }})) as date) as max_day
    from d
)

select 'known_weekday' as violation, k.date_str as detail
from known as k
left join d on cast(d.date_day as {{ dbt.type_string() }}) = k.date_str
where d.day_of_week is null or d.day_of_week <> k.expected_dow

union all
select 'is_weekend_mismatch', cast(date_day as {{ dbt.type_string() }})
from d
where is_weekend is null or is_weekend <> (day_of_week >= 6)

union all
select 'row_count', cast(n_rows as {{ dbt.type_string() }})
from bounds
where n_rows <> {{ dbt.datediff('min_day', 'max_day', 'day') }} + 1

union all
select 'min_date', cast(min_day as {{ dbt.type_string() }})
from bounds
where min_day is null or cast(min_day as {{ dbt.type_string() }}) <> '{{ var("dim_date_start") }}'

union all
select 'max_date', cast(max_day as {{ dbt.type_string() }})
from bounds
where max_day is null or cast(max_day as {{ dbt.type_string() }}) <> '{{ var("dim_date_end") }}'

union all
select 'date_key', cast(date_day as {{ dbt.type_string() }})
from d
where date_key is null
   or date_key <> extract(year from date_day) * 10000
                  + extract(month from date_day) * 100
                  + extract(day from date_day)
