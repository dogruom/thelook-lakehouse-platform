# ADR-0005: Multi-tenant isolation model and partitioning

- **Status:** Accepted (W3.D18, 2026-11-11)
- **Date:** 2026-11-11
- **Related:** Charter §3 (tenant invariant: `brand_id` in every key and join; partitioning is not a
  security boundary); ADR-0002 (Kafka key and `brand_id` header); D-064, D-071, D-074, D-078,
  D-079, D-085 (tenant isolation checks and views)

## Context
Several brands (tenants) share every pipeline, topic and table. Business identifiers are only unique
within a brand: two brands can hold the same `customer_id` or `order_id` (the generator injects
such collisions on purpose). A join or MERGE that forgets `brand_id` silently mixes tenants — the
most damaging bug a multi-tenant data platform can have, because nothing fails. The platform must
make that bug structurally unlikely and detectable, and choose a physical layout without
pretending the layout is an access control.

## Decision
1. **Shared tables, composite keys.** One table per entity for all brands. Every business key is
   composite and starts with `brand_id`: `(brand_id, customer_id)`, `(brand_id, customer_id,
   _cdc_sequence)`, `(brand_id, order_id)`, `(brand_id, order_id, item_id)`, `(brand_id,
   product_id)`, `(brand_id, event_id)`. Every MERGE condition, window partition and join in Spark
   and every dbt relationship test includes `brand_id`.
2. **Where the tenant comes from.** Kafka: the payload `brand_id`, cross-checked against the key
   prefix and the `brand_id` header (D-064); a mismatch goes to the DLQ. Landing files: the path
   segment `landing/{brand_id}/...` is the tenant; the payload claim is kept as `payload_brand_id`
   and a mismatch goes to the DLQ (D-071, D-077). Headers are never used for partitioning.
3. **Detection, not trust.** `lakehouse.quality.isolation.assert_no_cross_tenant_rows` fails a joined
   frame whose two sides carry different brands; a deliberately leaky fixture (orders ⋈ customers
   on `customer_id` only, over colliding ids) must raise, and the composite join must pass. The
   tenant-isolation integration suite compares per-brand counts with the generator manifest.
4. **Tenant-scoped views.** `silver.v_<table>__<brand>` = `SELECT * FROM silver.<table> WHERE brand_id
   = '<brand>'`, generated from validated identifiers only and registered for named (not
   namespaced) runs. They give consumers a single-brand surface; they are a convenience, **not** a
   security boundary — anyone who can read the base table reads all brands. Real access control
   (row filters, grants) is a catalog feature: Glue/Lake Formation or Unity Catalog in M3.
5. **Partitioning, as built.** Chosen for pruning and file sizes at this scale, never for isolation:

   | Table | Partitioned by | Why |
   |---|---|---|
   | `bronze.raw_customers` | — | small; read as a stream and by key lookups |
   | `bronze.raw_clickstream` | `event_date` | volume and time-bounded reads |
   | `bronze.raw_orders` | `brand_id` | per-brand landing batches; few brands → few partitions |
   | `bronze.raw_products` | — | tiny catalog seed |
   | `silver.customers`, `silver.customers_history` | — | small, MERGE by key |
   | `silver.orders`, `silver.order_items` | — | small; insert-only MERGE by key |
   | `silver.clickstream` | `event_date` | dedup MERGE bounded to the event's date (D-079) |
   | `dead_letter.*`, `ops.pipeline_metrics` | — | small |

   `brand_id` is not a general partition column: with many small brands it creates many small
   files and skewed partitions; with few large brands it adds nothing that a `brand_id` filter on a
   Delta table with data skipping does not already give. Revisit with measured file counts in M3.

## Consequences
- A missing `brand_id` in a join is caught by tests, not by luck; the negative fixture proves the
  check can fail.
- Shared tables keep one pipeline and one schema for all brands; per-brand cost and access control
  need catalog features later (M3), and this ADR says so instead of implying isolation by layout.
- Skew: a dominant brand makes its partitions and its MERGE keys hot; at the current synthetic scale
  this is not measurable, so no salting is added (DELIVERY_PLAN removed the salting benchmark).
- Not provided: row-level security, per-tenant encryption keys, per-tenant retention.
