{{ config(enabled=(target.type == 'spark')) }}
-- Expected state of spike_merge_composite after each round (var spike_round). Returns the rows
-- that differ from the expectation in either direction; zero rows = pass.
-- EXCEPT is DISTINCT, so exact duplicate rows are invisible here: this test runs together with
-- unique_combination_of_columns(brand_id, customer_id) on the same model, which catches them.
with expected as (
    select 'brand_a' as brand_id, 'c1' as customer_id,
        {{ "'v2'" if var('spike_round') | int == 2 else "'v1'" }} as version
    union all select 'brand_b', 'c1', 'v1'
    union all select 'brand_a', 'c2', 'v1'
    {% if var('spike_round') | int == 2 %}
    union all select 'brand_b', 'c3', 'v1'
    {% endif %}
),
actual as (
    select brand_id, customer_id, version from {{ ref('spike_merge_composite') }}
)
(select 'missing' as problem, * from (select * from expected except select * from actual))
union all
(select 'unexpected' as problem, * from (select * from actual except select * from expected))
