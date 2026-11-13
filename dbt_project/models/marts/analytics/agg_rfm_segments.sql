-- Grain: (brand_id, customer_id) for every non-deleted customer with at least one valid order up
-- to var('as_of_date'). RFM scores are NTILE(5) quintiles computed PER BRAND (partition by
-- brand_id): a customer is only compared with customers of the same tenant. Ties are broken by
-- customer_id so the output is deterministic (NTILE alone splits ties arbitrarily). Recency is in
-- hours because the synthetic order history spans two days (D-097). 5 = best on every axis.
with base as (
    select
        customer_sk,
        brand_id,
        customer_id,
        recency_hours,
        frequency_orders as frequency,
        monetary_total_spend as monetary,
        as_of_date
    from {{ ref('obt_customer_360') }}
    where frequency_orders > 0
      and not is_deleted
),

scored as (
    select
        *,
        ntile(5) over (partition by brand_id order by recency_hours desc, customer_id) as r_score,
        ntile(5) over (partition by brand_id order by frequency, customer_id) as f_score,
        ntile(5) over (partition by brand_id order by monetary, customer_id) as m_score
    from base
)

select
    customer_sk,
    brand_id,
    customer_id,
    recency_hours,
    frequency,
    monetary,
    cast(r_score as {{ dbt.type_int() }}) as r_score,
    cast(f_score as {{ dbt.type_int() }}) as f_score,
    cast(m_score as {{ dbt.type_int() }}) as m_score,
    {{ dbt.concat(["cast(r_score as " ~ dbt.type_string() ~ ")", "'-'",
                   "cast(f_score as " ~ dbt.type_string() ~ ")", "'-'",
                   "cast(m_score as " ~ dbt.type_string() ~ ")"]) }} as rfm_score,
    {{ rfm_segment('r_score', 'f_score', 'm_score') }} as rfm_segment,
    as_of_date
from scored
