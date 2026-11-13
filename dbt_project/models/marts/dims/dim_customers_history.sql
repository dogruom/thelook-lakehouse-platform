-- SCD2 history exposed from Silver (Spark-owned). One row per (brand_id, customer_id, valid_from).
select
    {{ brand_surrogate_key(['brand_id', 'customer_id', 'valid_from']) }} as customer_history_sk,
    {{ brand_surrogate_key(['brand_id', 'customer_id']) }} as customer_sk,
    brand_id,
    customer_id,
    version,
    valid_from,
    valid_to,
    is_current,
    is_deleted,
    email_token,
    phone_masked,
    city,
    country,
    loyalty_tier,
    signup_date,
    cdc_action,
    cdc_sequence
from {{ ref('stg_customers_history') }}
