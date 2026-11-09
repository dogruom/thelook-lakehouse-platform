-- Probes 2 + 3 (W2.D11): incremental `merge` on Delta with a composite unique_key.
-- Round 1 builds three rows. Round 2 (incremental) sends an update for (brand_a, c1) and a new
-- (brand_b, c3). brand_b also has a c1: if the merge matched on customer_id alone it would
-- overwrite the other tenant's row - the singular test catches exactly that.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['brand_id', 'customer_id'],
) }}

{% if is_incremental() %}
select 'brand_a' as brand_id, 'c1' as customer_id, 'v2' as version, 2 as round
union all
select 'brand_b', 'c3', 'v1', 2
{% else %}
select 'brand_a' as brand_id, 'c1' as customer_id, 'v1' as version, 1 as round
union all
select 'brand_b', 'c1', 'v1', 1
union all
select 'brand_a', 'c2', 'v1', 1
{% endif %}
