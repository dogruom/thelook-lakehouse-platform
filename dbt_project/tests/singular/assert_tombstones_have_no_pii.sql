-- Tombstones (is_deleted = true) must not carry PII. Silver NULLs first_name, last_name,
-- email_token, phone_masked and city on a D event (silver/customers.py, customers_history.py);
-- staging never selects the raw names, so this asserts the columns that reach dbt:
-- email_token, phone_masked, city. country, loyalty_tier and signup_date are kept by design.
select brand_id, customer_id, 'stg_customers' as model_name
from {{ ref('stg_customers') }}
where is_deleted
  and (email_token is not null or phone_masked is not null or city is not null)

union all

select brand_id, customer_id, 'stg_customers_history' as model_name
from {{ ref('stg_customers_history') }}
where is_deleted
  and (email_token is not null or phone_masked is not null or city is not null)
