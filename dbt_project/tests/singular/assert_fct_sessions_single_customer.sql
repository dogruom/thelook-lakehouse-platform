-- Proves: no session carries more than one distinct non-null customer_id, so fct_sessions can
-- safely take a single customer per session. Returns offending (brand_id, session_id).
-- Limits: it cannot say whether the one customer is the right one; anonymous events inside an
-- identified session are allowed.
select brand_id, session_id, count(distinct customer_id) as distinct_customers
from {{ ref('stg_clickstream') }}
where customer_id is not null
group by brand_id, session_id
having count(distinct customer_id) > 1
