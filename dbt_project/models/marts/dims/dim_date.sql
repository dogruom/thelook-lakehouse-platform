-- Conformed, non-tenant calendar (no brand_id by design). Day grain, inclusive range from vars
-- dim_date_start / dim_date_end. date_spine's end date is exclusive, hence the +1 day.
with spine as (
    {{ dbt.date_spine(
        datepart='day',
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date=dbt.dateadd('day', 1, "cast('" ~ var('dim_date_end') ~ "' as date)")
    ) }}
),

days as (
    select cast(date_day as date) as date_day from spine
)

select
    {{ date_to_key('date_day') }} as date_key,
    date_day,
    cast(extract(year from date_day) as {{ dbt.type_int() }}) as year,
    cast(extract(quarter from date_day) as {{ dbt.type_int() }}) as quarter,
    cast(extract(month from date_day) as {{ dbt.type_int() }}) as month,
    cast(extract(day from date_day) as {{ dbt.type_int() }}) as day_of_month,
    {{ iso_day_of_week('date_day') }} as day_of_week,
    {{ iso_day_of_week('date_day') }} >= 6 as is_weekend
from days
