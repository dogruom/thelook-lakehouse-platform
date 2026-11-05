# ADR-0001: SeaweedFS as local S3-compatible storage

- **Status:** Accepted
- **Date:** 2026-10-20

## Context
The platform is *local-first, cloud-deployable*: reviewers must run it with
`docker compose up` at $0, while the cloud profile targets real AWS S3.
We need a local object store that speaks the S3 API (s3a://, boto3, aws-cli).

MinIO was the de-facto choice for years, but its community edition moved to
source-only / maintenance mode (no official community images, console features
removed). Building on it would mean pinning an unmaintained binary.

## Decision
Use **SeaweedFS** (S3 gateway) locally. All code talks to storage only through
the S3 API configured by `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`
and path-style access. No vendor-specific client (e.g. `mc`) is used;
bucket bootstrap uses the standard `aws-cli` (`scripts/init_storage.sh`).

## Consequences
- Swapping to AWS S3 (cloud profile) is a config change, not a code change.
- MinIO/`mc` Dockerfiles removed to avoid two competing storage stacks.
- SeaweedFS has fewer Delta-specific battle reports than MinIO; covered by
  the smoke test writing/reading a Delta table via s3a.
