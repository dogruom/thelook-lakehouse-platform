-- Probe 4 (W2.D11): enforced model contract (column names and types, not_null constraint).
{{ config(materialized='table') }}

select
    cast('brand_a' as string) as brand_id,
    cast('c1' as string) as customer_id,
    cast(1 as bigint) as order_count
