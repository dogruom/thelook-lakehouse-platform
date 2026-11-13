{{ config(enabled=var('tenant_fixtures', false), tags=['tenant_fixture']) }}

{{ cross_tenant_key_matches(
    ref('fx_tenant_child'), 'customer_id', ref('fx_tenant_parent'), 'customer_id',
    label='fx_tenant_child.customer_id -> fx_tenant_parent'
) }}
