-- Proves: per brand, every RFM segment holds more than 0 % and less than 100 % of the scored
-- customers (W4.D21.2 "no segment at 0 %/100 %"). Returns (brand_id, rfm_segment, customers,
-- brand_customers) for every missing or all-covering segment.
with segments as (
    select 'champions' as rfm_segment
    union all select 'loyal'
    union all select 'promising'
    union all select 'at_risk'
    union all select 'hibernating'
),

brands as (
    select brand_id, count(*) as brand_customers
    from {{ ref('agg_rfm_segments') }}
    group by brand_id
),

counts as (
    select brand_id, rfm_segment, count(*) as customers
    from {{ ref('agg_rfm_segments') }}
    group by brand_id, rfm_segment
)

select
    b.brand_id,
    s.rfm_segment,
    coalesce(c.customers, 0) as customers,
    b.brand_customers
from brands as b
cross join segments as s
left join counts as c
    on c.brand_id = b.brand_id
   and c.rfm_segment = s.rfm_segment
where coalesce(c.customers, 0) = 0
   or coalesce(c.customers, 0) = b.brand_customers
