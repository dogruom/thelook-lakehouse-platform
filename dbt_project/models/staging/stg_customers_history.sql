-- Rename/cast only. Raw PII (first_name, last_name) is intentionally not selected.
select
    brand_id,
    customer_id,
    version,
    valid_from,
    valid_to,
    cast(is_current as boolean) as is_current,
    cast(is_deleted as boolean) as is_deleted,
    email_token,
    phone_masked,
    city,
    country,
    loyalty_tier,
    cast(signup_date as date) as signup_date,
    {{ adapter.quote('_cdc_action') }} as cdc_action,
    cast({{ adapter.quote('_cdc_sequence') }} as bigint) as cdc_sequence,
    {{ adapter.quote('_cdc_ts') }} as cdc_ts,
    {{ adapter.quote('_updated_at') }} as silver_updated_at
from {{ source('silver', 'customers_history') }}
