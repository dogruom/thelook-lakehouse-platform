# Portfolio Notes — Verified Capabilities, Talking Points and README Drafts

> **Purpose**: grows at every checkpoint (`checkpoint` skill) so that README, CV bullets and interview answers come from verified work, not from memory at the end.
> **Rule**: the *Verified* section contains only items with evidence (commit + verification output, linked to [PROGRESS.md](PROGRESS.md)). Everything else stays in *Drafts*. No invented metrics, impact, scale or production claims.
> **Finalised**: W6.D35.1 (interview narrative) and W6.D34.2 (README rewrite).

---

## 1. Verified capabilities

| Date | Capability | Evidence | Competency |
|---|---|---|---|
| — | _none yet — first entries expected at the W1 checkpoint_ | — | — |

Competency tags: **DE** = Senior Data Engineer · **ARCH** = Data Architect.

## 2. Interview talking points (decision → trade-off → evidence)

| Topic | Decision | Trade-off I can explain | Evidence |
|---|---|---|---|
| Event keys | `{brand_id}:{entity_id}` key + `brand_id` header | per-entity ordering vs hot tenant partitions; headers are not partition keys | ADR-0002 (W1.D6) |
| Runtime parity | Python 3.11 / Spark 3.5 / Delta 3.3 locally, on Glue 5.0 and Databricks | one codebase across three runtimes vs newest Spark 4 features | ADR-0007, ADR-0011 |
| Service boundaries | data contracts between independently deployable components, enforced by import-linter | microservice benefits without synchronous coupling | ADR-0009 |
| Cloud cost | serverless, ephemeral, budget-tracked | always-on convenience vs cost; measured spend per run | ADR-0008, ADR-0011 |

## 3. Drafts (not yet verified — never copy to README before evidence exists)

### README "What this project demonstrates" (draft)
- Turning at-least-once Kafka delivery into idempotent Delta tables, with restart and checkpoint-loss tests.
- Sequence-ordered CDC and order-independent SCD2 history, proven by scenario tests.
- Tenant isolation proven by tests, including a negative test that detects a leaky join.
- The same Spark/Delta jobs on local Spark, AWS Glue 5.0 and Databricks serverless; dbt Gold on two engines.

### Implemented-architecture diagram
Maintained at each milestone so that it shows **only components that exist and run** (target architecture stays in the Charter).
