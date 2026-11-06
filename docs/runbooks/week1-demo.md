# Runbook: Week 1 demo (about 10 minutes)

Shows: local stack up, seeded generator with ground-truth manifest, producer writing keyed and
headed messages to Kafka, and the round-trip integration test. Interface rationale:
[ADR-0002](../adr/0002-event-interface-kafka-envelope.md). Numbers below are placeholders until
filled from recorded evidence.

## Prerequisites
- Python 3.11 venv active with `pip install -e .[dev]`.
- Docker Desktop running with enough memory (see Troubleshooting).
- The local environment file created once by `python scripts/bootstrap_env.py`. Never print or commit it.
- `COMPOSE_PROJECT_NAME=crm_lakehouse` in the shell (value from `.env.example`); a stale value makes
  `docker compose exec kafka` report "service not running": `$env:COMPOSE_PROJECT_NAME='crm_lakehouse'`.

## Steps (PowerShell, repo root)

1. Start the core stack
   ```powershell
   docker compose --profile core up -d
   ```
   Expected: containers created; `crm_kafka` reaches `healthy` (allow up to about 60 s start period).

2. Smoke test
   ```powershell
   python scripts/smoke_test.py
   ```
   Expected: all checks pass, exit code 0. Recorded 2026-11-06: `RESULT: all 24 checks passed`.

3. Dry-run the CDC generator and read the manifest summary
   ```powershell
   python -m lakehouse.generator cdc --brands 3 --events 1000 --seed 42 --dry-run
   ```
   Expected: manifest summary with injected-defect counts per category; re-running with seed 42
   gives identical files (hash). Recorded: emitted 1020 = accepted 880 + dlq 70 + duplicates 20 +
   stale 50 + late_beyond_window 0 (CDC lateness is not generated yet, D-030).

4. Produce to Kafka
   ```powershell
   python -m lakehouse.generator produce --brands 3 --events 5000 --cdc-events 1000 --seed 42
   ```
   Expected: summary of messages produced to `clickstream_events` and `customer_cdc`, zero delivery
   errors, exit 0. Recorded: `attempted=6220 delivered=6220 failed=0 undelivered=0`
   (customer_cdc 1020, clickstream_events 5200; brand_alpha 3735, brand_beta 1864, brand_gamma 621).
   Re-running with the same seed re-sends the same `event_id`s — intended at-least-once behaviour;
   Silver dedup removes them.

5. Inspect keys, headers and partitions (console consumer inside the `kafka` service; internal
   listener `localhost:9092`, tools in `/opt/kafka/bin`)
   ```powershell
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clickstream_events --from-beginning --max-messages 5 --property print.key=true --property print.headers=true --property print.partition=true
   ```
   Expected: 5 lines, each with a partition number, headers `brand_id`, `schema_version`,
   `event_type`, a key `{brand_id}:{session_id}` and a JSON value whose `brand_id` and
   `schema_version` match the headers. Sample (truncated):
   `Partition:0  brand_id:brand_alpha,schema_version:1,event_type:page_view  brand_alpha:e87a214c-…  {"brand_id":"brand_alpha",…}`.
   Add `-T` when running from a non-interactive shell.

6. Round-trip integration test
   ```powershell
   pytest tests/integration/test_producer_roundtrip.py tests/integration/test_produce_cli.py -q
   ```
   Expected: all tests pass (counts, key format, `brand_id` header = key prefix, one partition per
   key, partition = Java murmur2 of the key (D-039), every record consumed, no extra records, CLI
   end to end). Recorded: `5 passed` = CDC + clickstream round trips and the CLI run against the broker,
   plus two broker-free checks (murmur2 vectors, partition-split detector). The broker-outage test
   (`tests/integration/test_producer_failure_modes.py`, ~61 s, needs no broker) is run separately.

7. Stop (never `down -v`)
   ```powershell
   docker compose --profile core stop
   ```
   Expected: containers stopped, volumes kept. Plain `docker compose stop` does **not** stop services
   that belong to a profile — check with `docker ps`.

**Recorded end-to-end run (2026-11-06, from a stopped stack, steps 1–6):** compose up 18.6 s · smoke
test 56.3 s (24/24) · CDC dry-run 1.9 s · produce 2.8 s (6220 delivered) · console consumer 6.0 s ·
round-trip tests 19.3 s (5 passed) · **total 105 s**.

## Troubleshooting
- **Port 5432 conflict** (local PostgreSQL): the host port is `POSTGRES_HOST_PORT`, which defaults
  to `15432` (see `.env.example`). Set it to a free port in your local environment file (edit the
  value only; do not print the file).
- **Broker not ready** (producer or consumer times out): check `docker compose ps` for `crm_kafka`
  health and wait for `healthy`, then retry. Host clients use `127.0.0.1:9094`; clients inside the
  network use `kafka:9092`. Topics are created by the `kafka-init` job (auto-create is off); if a
  topic is missing, check `docker compose logs kafka-init --tail 20`.
- **Docker memory**: the project's reference setup gives Docker Desktop 7.6 GB (WSL2 memory setting); this is a configuration value, not a measured requirement.
  Out-of-memory kills show as restarting containers; raise the limit or start fewer profiles.
