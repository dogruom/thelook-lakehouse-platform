# Local vs cloud differences

Observed differences between the local stack (Docker Compose) and the cloud targets
(AWS S3 / Glue 5.0, Databricks). One row per difference; add rows when a new one is observed.

| # | Area | Local (Compose) | Cloud target | Consequence | Since |
|---|---|---|---|---|---|
| 1 | Object storage auth | SeaweedFS S3 API (port 9000) uses identities rendered from env; the filer (9001) and master (9333) APIs are **unauthenticated** and can read/write the bucket | AWS S3: every request is IAM-authorised | Local-only, bound to `127.0.0.1`; not a security boundary. Never expose these ports | W1.D2 |
| 2 | Storage container user | SeaweedFS runs as root (image default); S3 keys are visible in `docker inspect` env output | Managed service, no container | Acceptable locally; do not describe the local stack as fully hardened | W1.D2 |
| 3 | S3A credentials | `EnvironmentVariableCredentialsProvider` (keys from container env) | Glue/Databricks: IAM role / instance profile provider | Jobs must not set credentials in code; provider is runtime configuration | W1.D2 |
| 4 | Postgres host port | `127.0.0.1:${POSTGRES_HOST_PORT:-15432}` (a native PostgreSQL often owns 5432 on Windows) | Glue Data Catalog replaces the Postgres-backed Hive Metastore | Host tools connect to 15432; containers still use `postgres:5432` | W1.D2 |
