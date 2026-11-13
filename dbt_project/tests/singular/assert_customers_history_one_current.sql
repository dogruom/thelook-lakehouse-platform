-- SCD2 integrity: every customer has exactly one current version, and is_current agrees with
-- valid_to (current <=> valid_to is null). Returns violations with a reason.
select brand_id, customer_id, 'current_count_not_one' as reason, count_if(is_current) as n
from {{ ref('stg_customers_history') }}
group by brand_id, customer_id
having count_if(is_current) <> 1

union all

select brand_id, customer_id, 'valid_to_disagrees_with_is_current' as reason, count(*) as n
from {{ ref('stg_customers_history') }}
where (valid_to is null) <> is_current
group by brand_id, customer_id
