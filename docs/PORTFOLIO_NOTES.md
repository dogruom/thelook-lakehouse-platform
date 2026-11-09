# Portfolio Notes — Verified Capabilities, Talking Points and README Drafts

> **Purpose**: grows at every checkpoint (`checkpoint` skill) so that README, CV bullets and interview answers come from verified work, not from memory at the end.
> **Rule**: the *Verified* section contains only items with evidence (commit + verification output, linked to [PROGRESS.md](PROGRESS.md)). Everything else stays in *Drafts*. No invented metrics, impact, scale or production claims.
> **Finalised**: W6.D35.1 (interview narrative) and W6.D34.2 (README rewrite).

---

## 1. Verified capabilities

| Date | Capability | Evidence | Competency |
|---|---|---|---|
| 2026-11-06 | Enforced component boundaries: import-linter contracts in CI, proven by a negative probe that breaks the contract | commit `00df04e`; PROGRESS W1.D1 Evidence | ARCH |
| 2026-11-06 | Supply-chain-aware CI: SHA-pinned actions, read-only token, checksum-verified gitleaks full-history scan with reviewed exceptions | commit `00df04e` (CI run link pending) | DE |
| 2026-11-06 | Versioned event contracts (Pydantic v2 + committed JSON Schema snapshots) with a breaking-change gate proven by a negative test; PII-safe validation errors | commit `6889317`; PROGRESS W1.D3 Evidence | DE |
| 2026-11-06 | Deterministic multi-tenant CDC generator with an exact-count ground-truth manifest (expected outcome per emission, batch-aware `classify_outcomes`, reconciliation equation) validated against an independent reference MERGE | commit `848d9c8`; PROGRESS W1.D4 Evidence | DE |
| 2026-11-06 | Keyed idempotent Kafka producer (`acks=all`, idempotence, delivery accounting per tenant) with a live round-trip test proving per-key partition affinity and no extra records; clickstream generator with exact defect manifest | commit `03d02b4`; PROGRESS W1.D5 Evidence | DE |
| 2026-11-09 | PySpark job framework: settings-driven session profiles, run ID on every log line, wheel-based packaging submitted to a standalone cluster, with a negative run proving driver and executor load the shipped code rather than a source mount; Spark tests run in the image, no PySpark on the host | commits `e82f91b`, `5836709`, `80a6e63`; PROGRESS W2.D7 Evidence | DE |

Competency tags: **DE** = Senior Data Engineer · **ARCH** = Data Architect.

## 2. Interview talking points (decision → trade-off → evidence)

| Topic | Decision | Trade-off I can explain | Evidence |
|---|---|---|---|
| Event keys | `{brand_id}:{entity_id}` key + `brand_id` header | per-entity ordering vs hot tenant partitions; headers are not partition keys | [ADR-0002](adr/0002-event-interface-kafka-envelope.md); round-trip test W1.D5 |
| Delivery semantics | idempotent producer + `acks=all`, no Kafka transactions; dedup in Silver | at-least-once and simple vs transactional EOS that only covers Kafka→Kafka | D-036; W1.D5 |
| Runtime parity | Python 3.11 / Spark 3.5 / Delta 3.3 locally, on Glue 5.0 and Databricks | one codebase across three runtimes vs newest Spark 4 features | ADR-0007, ADR-0011 |
| Service boundaries | data contracts between independently deployable components, enforced by import-linter | microservice benefits without synchronous coupling | ADR-0009 |
| Schema evolution | strict producer (`extra="forbid"`), tolerant consumer; any constraint change = breaking | safety vs friction of version bumps on harmless tightening | DECISION_LOG D-006, D-015; W1.D3 |
| CDC correctness | outcome precedence DLQ → DUPLICATE → NOOP → ACCEPT, gate before dedup, sequence not wall clock | batch-local dedup drops intermediate versions (SCD2 needs them) vs simpler MERGE | DECISION_LOG D-022…D-024, D-029; W1.D4 |
| Parsing vs typing | money and timestamps stay strings in the Spark schema; typed casts happen in Silver after a validity check | one more step vs a cast that silently rounds `12.505`, accepts `1e2` and turns overflow into null (measured) | DECISION_LOG D-045; W2.D7 |
| Cloud cost | serverless, ephemeral, budget-tracked | always-on convenience vs cost; measured spend per run | ADR-0008, ADR-0011 |

## 3. Drafts (not yet verified — never copy to README before evidence exists)

### README "What this project demonstrates" (draft)
- Turning at-least-once Kafka delivery into idempotent Delta tables, with restart and checkpoint-loss tests.
- Sequence-ordered CDC and order-independent SCD2 history, proven by scenario tests.
- Tenant isolation proven by tests, including a negative test that detects a leaky join.
- The same Spark/Delta jobs on local Spark, AWS Glue 5.0 and Databricks serverless; dbt Gold on two engines.

### Implemented-architecture diagram
Maintained at each milestone so that it shows **only components that exist and run** (target architecture stays in the Charter).
