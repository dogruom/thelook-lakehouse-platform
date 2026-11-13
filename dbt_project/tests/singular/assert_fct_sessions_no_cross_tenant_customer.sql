-- Proves: no session resolves its customer_id to ANOTHER brand's customer: customer_id absent in
-- the session's own brand but present in dim_customers of a different brand must not happen.
-- Returns offending (relationship, brand_id, key_value). customer_id values unknown in every
-- brand are caught by the error-level relationships_by_brand test on fct_sessions (D-096).
{{ cross_tenant_key_matches(
    ref('fct_sessions'), 'customer_id',
    ref('dim_customers'), 'customer_id',
    label='fct_sessions.customer_id -> dim_customers.customer_id'
) }}
