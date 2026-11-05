# ADR-0012: Bounded Databricks track on Databricks Free Edition

- **Status:** Accepted (owner, 2026-11-05) — reverses the "Databricks out of v1" cut in DELIVERY_PLAN v8.0
- **Date:** 2026-11-05

## Context
Databricks is the lakehouse platform of several Berlin employers (Zalando among them, which is
under-represented in the ATS-based job sample), and the owner wants hands-on Unity Catalog
experience, including console/UI work that builds an architect's view. The earlier removal was
based on time and trial cost. **Databricks Free Edition** removes the cost: it is free,
serverless-only, includes Unity Catalog (one workspace, one metastore), jobs, one SQL warehouse
and dashboards, with daily compute quotas; custom workspace storage (own S3) is not supported
([limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)).
Our code is Spark 3.5 + Delta and portable; the dbt models are adapter-portable.

## Decision
Two days in Milestone 3 (W6.D32–D33), after the AWS core is green:
1. **Unity Catalog by hand, then as code**: create catalog/schemas/volume, grants for a
   tenant-scoped group, explore lineage in Catalog Explorer; then express the same grants as SQL in
   the bundle so the setup is reproducible.
2. **Same job code as a serverless job**: upload a generator sample into a UC volume; deploy the
   `lakehouse` wheel with a Databricks Asset Bundle; run Bronze → Silver with `availableNow`.
3. **Gold with dbt-databricks** on the SQL warehouse (same models, second adapter) and one
   AI/BI dashboard on Customer 360 / RFM.
4. **Liquid Clustering vs Z-Order** note on a Silver table (qualitative + file statistics; Free
   Edition timings are not benchmark-grade).
Everything is recorded in `docs/LOCAL_VS_CLOUD.md` (third column). Capability gaps found on Free
Edition (e.g. a bundle feature unavailable) are documented, not worked around with paid tiers.

## Consequences
- $0 cost; covers part of the warehouse/lakehouse-platform gap (Databricks SQL, Unity Catalog).
- Replaces the Snowflake stretch and shrinks the generic performance study (W6.D32 in v8.0).
- Claims stay bounded: "ran the same jobs and dbt models on Databricks Free Edition" — not
  "production Databricks experience".
- Free Edition quotas or inactivity deletion can interrupt; the track is time-boxed and its
  evidence (bundle, SQL, screenshots of lineage/grants) is kept in the repository.
