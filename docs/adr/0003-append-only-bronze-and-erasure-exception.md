# ADR-0003: Append-only Bronze and the erasure exception

- **Status:** Accepted for the append-only rules (W3.D18, 2026-11-11). The erasure procedure is
  **Proposed**: designed here, built and tested in W6.D31, when this ADR is updated.
- **Date:** 2026-11-11
- **Related:** Charter §3 invariant 1 and the privacy controls; ADR-0006 (restart, replay,
  checkpoint loss); D-047 (stable parse codes), D-057 (re-reads), D-062 (DLQ envelope),
  D-071 (file Bronze), D-072 (reconciliation terms), D-075 (PII key)

## Context
Bronze is the only place where every message or landed line is kept as it arrived. Every later
layer (DLQ, Silver, SCD2 history, Gold, reconciliation) and every audit depend on being able to go
back to it: the DLQ references Bronze rows instead of copying them (`_bronze_ref`), the SCD2 history
is rebuilt from Bronze (D-076), the replay job joins DLQ rows back to Bronze (D-080), and
reconciliation counts Bronze rows (D-072). A Bronze that is updated in place would make all of
these unverifiable. At the same time the platform holds personal data (synthetic, but treated as
real), so a subject's erasure request must be possible in Bronze too.

## Decision
1. **Writers only append.** `bronze_stream` (Kafka) and `bronze_batch` (landing files) are the only
   writers; each micro-batch is one Delta append with `txnAppId` / `txnVersion` (ADR-0006). No job
   runs `UPDATE`, `DELETE`, `MERGE` or `OVERWRITE` against a Bronze table. Unparseable input is
   stored, not dropped (`_raw_value` + `_parse_error`, D-047).
2. **Re-reads are tolerated, not repaired.** After a checkpoint loss the same source position can be
   appended again. Bronze keeps both rows; downstream jobs recognise the re-read by `_source_ref`
   and count it as `reread_rows` beside the reconciliation equation (D-057, D-072).
3. **Schema changes are additive only.** Kafka writes have no `mergeSchema` (an unplanned change
   fails loudly); landing-file writes use `mergeSchema` so a new contract version can add a column
   to the existing table (D-071). Columns are never dropped or retyped in place.
4. **Downstream references, not copies.** DLQ rows carry `_bronze_ref`; Silver rows carry
   `_bronze_ref`; replay re-reads Bronze. Bronze rows therefore must stay addressable by
   `_source_ref` for as long as they are retained.
5. **The one exception: audited erasure (Proposed, W6.D31).** A subject's records may be rewritten
   in Bronze (and Silver, SCD2 history, DLQ, Gold) by a dedicated `ops.erasure` job only. Each run
   writes one row per table to `ops.erasure_log` (request id, subject key `(brand_id, customer_id)`,
   table, rows rewritten, Delta version before/after, operator, timestamp). Rewriting means nulling
   or removing the subject's payload fields while keeping lineage columns, so counts and references
   still reconcile.
6. **What erasure does not claim.** `VACUUM` removes old file versions only after the retention
   window; until then time travel still returns the data. Kafka topic retention, streaming
   checkpoints/state and any downstream copy are handled explicitly in the procedure. The project
   never claims "VACUUM = erasure" or legal compliance; it demonstrates privacy controls.

## Consequences
- Every Silver/DLQ/history result can be recomputed or audited from Bronze; this is what makes the
  SCD2 rebuild and DLQ replay possible.
- Bronze grows without bound until retention is applied: retention and `VACUUM` policy per layer is
  an operations task (M3, lifecycle rules on AWS S3, ADR-0008).
- Re-reads cost storage and must be counted everywhere a Bronze row count is used.
- Erasure is the only code path allowed to modify Bronze; it must be isolated in one job, tested
  (subject absent from current versions; time-travel caveat shown), and logged. Until W6.D31 the
  procedure exists only as this design.
