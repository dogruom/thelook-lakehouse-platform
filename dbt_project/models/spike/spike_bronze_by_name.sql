-- Probe 1 (W2.D11): dbt reads a Spark-written Bronze table by catalog name, not by path.
select
    brand_id,
    count(*) as events
from {{ source('bronze', 'raw_customers') }}
group by brand_id
