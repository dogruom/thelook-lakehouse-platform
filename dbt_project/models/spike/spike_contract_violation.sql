-- Negative probe (W2.D11): declared bigint, produced string -> the enforced contract must fail
-- before anything is written. Disabled unless --vars "{spike_negative: true}".
{{ config(materialized='table', enabled=var('spike_negative', false)) }}

select cast('brand_a' as string) as brand_id, cast('many' as string) as order_count
