-- One row per (brand_id, customer_id): current state, tombstones kept with is_deleted = true.
-- Privacy columns come from Silver as-is; no PII is hashed or revealed in dbt.
select
    {{ brand_surrogate_key(['brand_id', 'customer_id']) }} as customer_sk,
    brand_id,
    customer_id,
    email_token,
    phone_masked,
    city,
    country,
    loyalty_tier,
    signup_date,
    is_deleted,
    cdc_action,
    cdc_sequence,
    silver_updated_at
from {{ ref('stg_customers') }}
