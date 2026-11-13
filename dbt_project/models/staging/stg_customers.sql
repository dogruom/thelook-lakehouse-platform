-- Rename/cast only. Raw PII (first_name, last_name) is intentionally not selected.
select
    brand_id,
    customer_id,
    email_token,
    phone_masked,
    city,
    country,
    loyalty_tier,
    cast(signup_date as date) as signup_date,
    cast(is_deleted as boolean) as is_deleted,
    _cdc_action as cdc_action,
    cast(_cdc_sequence as bigint) as cdc_sequence,
    _cdc_ts as cdc_ts,
    _updated_at as silver_updated_at
from {{ source('silver', 'customers') }}
