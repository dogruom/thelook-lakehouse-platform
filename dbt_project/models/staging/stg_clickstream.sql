-- Rename/cast only. customer_id is nullable and not FK-checked in Silver.
select
    brand_id,
    event_id,
    session_id,
    customer_id,
    event_type,
    page_url,
    device_category,
    event_timestamp,
    event_date,
    produced_at,
    {{ adapter.quote('_updated_at') }} as silver_updated_at
from {{ source('silver', 'clickstream') }}
