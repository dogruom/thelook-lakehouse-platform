---
paths:
  - "docker/**"
  - "docker-compose.yml"
  - "infra/**"
  - ".github/**"
  - "scripts/*.sh"
---

# Infrastructure, CI and cloud rules

## Local stack (Docker Compose)
- Profiles: `core` (Kafka, SeaweedFS, Postgres/metastore, Spark), `sql` (Spark Thrift Server),
  `orchestration` (Airflow), `tools` (generator, spark-test). Start only what the task needs.
- Pin every image by version (no `:latest`). Publish ports on `127.0.0.1` only.
- Secrets come from `.env` via `${VAR:?message}` — no secret fallback values in compose files.
- Image rebuilds cost time and bandwidth: change Dockerfiles only with a stated reason; mount
  config files (e.g. `spark-defaults.conf`) instead of baking them where possible.
- Stop with `docker compose stop`; never delete volumes, images or containers.

## Terraform (AWS)
- `terraform fmt -check`, `validate`, `tflint` and `trivy config` must pass before `plan`.
- `apply`/`destroy` only with explicit owner approval in the same session; record actual spend.
- Cost guardrails are part of every change: no NAT Gateways, no always-on clusters, Glue jobs
  with timeouts and max concurrency 1, S3 lifecycle rules, budget alarms, CloudWatch
  log retention ≤ 14 days. State: S3 backend with native locking; never committed.
- Least privilege: prefix-scoped S3 policies, no `*` actions on data buckets, OIDC trust limited
  to this repository and branch/environment.

## CI (GitHub Actions)
- Pin actions to a major version or SHA; `permissions:` minimal per job (`contents: read`,
  `id-token: write` only where OIDC is used).
- Unit job on every push/PR; integration job (Docker `core` profile) on PRs to `main` and manual
  dispatch only — free-minute budget matters.
- Secret scanning (gitleaks) runs in pre-commit and CI.
