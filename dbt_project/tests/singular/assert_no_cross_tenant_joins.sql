-- Proves: no staged foreign key points at a parent that exists only in a different brand, i.e. a
-- join that forgot brand_id would not change results on the current data.
-- Limits: the orders and order_items legs are subsets of relationships_by_brand failures (kept as
-- a readable tenant assertion); the clickstream leg is the only extra coverage. On 2026-11-13
-- local data 3 customer_ids exist in more than one brand but 0 clickstream rows reference them,
-- so that leg is vacuous on current data; the fixture pytest proves the macro bites. It does not
-- prove that every downstream query joins on brand_id (code review and relationships_by_brand).
{{ cross_tenant_key_matches(ref('stg_orders'), 'customer_id', ref('stg_customers'), 'customer_id', label='orders.customer_id -> customers') }}
union all
{{ cross_tenant_key_matches(ref('stg_order_items'), 'order_id', ref('stg_orders'), 'order_id', label='order_items.order_id -> orders') }}
union all
{{ cross_tenant_key_matches(ref('stg_clickstream'), 'customer_id', ref('stg_customers'), 'customer_id', label='clickstream.customer_id -> customers') }}
