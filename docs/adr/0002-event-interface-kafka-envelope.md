# ADR-0002: Event interface — Kafka, common envelope, keys vs headers

- **Status:** Accepted
- **Date:** 2026-11-06

## Context
The generator and the ingestion jobs communicate only through durable topics (ADR-0009). The
platform needs one streaming transport for the local-first build, one message envelope shared by
`customer_cdc` and `clickstream_events`, and clear rules for ordering, schema evolution and replay.
The AWS target (ADR-0008, ADR-0011) does not deploy a streaming service in v1, so the transport
choice must keep a Kinesis/MSK adapter possible without redesigning the contract.

## Decision

### a) Transport: Kafka (KRaft, single node locally); Kinesis compared only
Kafka 3.7 in KRaft mode runs in Docker Compose at no cost, offline, with a fully inspectable
broker (console consumer, offsets, partitions). Kinesis Data Streams would need an AWS account,
credentials and billed shard-hours for every development loop, which conflicts with the local-first
rule and the USD 50 ceiling (ADR-0008). On AWS the equivalent would be Amazon MSK (same client API,
only bootstrap servers and auth change) or Kinesis (a producer/consumer adapter behind the same
envelope; partition key = the Kafka key, shard = partition). Neither is deployed in v1; any such
run would be ephemeral and counted against the ADR-0008 budget.

### b) Common envelope (payload)
Every CDC and clickstream message value is UTF-8 JSON carrying: `event_id` (UUIDv4), `brand_id`,
`schema_version` (integer, **authoritative copy**) and `produced_at` (UTC, producer emission time),
plus the entity fields. Details: [DATA_CONTRACTS.md §2.0](../DATA_CONTRACTS.md).

### c) Key vs headers
- **Key** = `{brand_id}:{customer_id}` (CDC) or `{brand_id}:{session_id}` (clickstream). The key
  alone determines the partition, which gives per-customer / per-session ordering.
- **Partitioner** = `murmur2_random` (D-039), the Java-client hash. librdkafka's default (CRC32)
  would send the same key to a different partition than Spark, Kafka Connect or a Java producer,
  so per-key ordering would silently break once a second client writes the topic.
- **Headers** `brand_id`, `schema_version`, `event_type` are routing copies for filtering and
  debugging. They never take part in partitioning. Bronze does not persist headers, so downstream
  logic must use the payload fields.
- A null `session_id` produces key `{brand_id}:`; the event is kept in Bronze and rejected to the
  DLQ downstream (never dropped by the producer).
- Consequence: all events of one session or customer share a partition, but a hot tenant can still
  skew load across partitions. Accepted for v1; the key spreads tenants by entity, not by brand
  alone. Ordering holds per key only, hence CDC consumers order by `_cdc_sequence`, never by wall
  clock.

### d) Serialisation and schema evolution
JSON with committed JSON Schema snapshots generated from the Pydantic models
(`src/lakehouse/contracts`, guarded by a schema snapshot test). No schema registry in v1.
Producers are strict (unknown fields rejected); consumers are tolerant (unknown fields ignored
within one `schema_version`). Additive changes keep the version; breaking changes bump
`schema_version` and add a contract test (ADR-0009). Revisit Confluent Schema Registry with Avro or
Protobuf when a second independent producer team appears, when cross-repository compatibility
checks in CI are needed, or when payload size or throughput becomes a constraint.

### e) Topic retention is not the system of record
Retention is a configured operational window (`TOPIC_RETENTION_MS`, default 7 days). Bronze Delta is
the append-only, replayable history; replay and reprocessing read from Bronze, not from the topic.
Losing a Spark checkpoint may cause re-reading within the retention window; this is documented
behaviour, not a data-loss path.

### f) Producer semantics
`enable.idempotence=true` with `acks=all` removes duplicates caused by producer retries within a
session. Delivery remains **at-least-once**: a restarted producer, a replayed run or an injected
duplicate can still emit the same event again. No Kafka transactions are used. Duplicates are
handled downstream: Silver dedups on `(brand_id, event_id)`; CDC is ordered by `_cdc_sequence` and
applied with idempotent MERGE. This ADR makes no exactly-once claim.

## Consequences
- Local development is free and reproducible; AWS streaming remains a documented adapter, not a
  built component.
- Header/payload duplication costs a few bytes per message and can drift. Today the generator
  unit tests compare headers with the payload and the producer round trip compares the `brand_id`
  header with the key prefix; Bronze trusts only the payload copy.
- Ordering guarantees are per key only; hot-tenant skew is possible and observable via partition
  counts.
- Without a registry, compatibility is enforced by snapshots and tests in this repository only.

## Alternatives considered
| Option | Why not (now) |
|---|---|
| AWS Kinesis Data Streams as primary | Paid and online for every dev loop; conflicts with local-first and ADR-0008 budget |
| Amazon MSK | Always-on broker cost; same client API as local Kafka, so deferred without redesign |
| Schema Registry + Avro/Protobuf | Extra service and tooling for one producer; JSON Schema snapshots give the needed guarantees now |
| Key by `brand_id` only | Per-tenant ordering is unnecessary and creates severe hot-partition skew |
| Headers-only `brand_id` (no payload copy) | Bronze does not persist headers; tenant identity would be lost |
| Kafka transactions / exactly-once | Not needed with idempotent Silver; adds complexity and an unproven claim |
