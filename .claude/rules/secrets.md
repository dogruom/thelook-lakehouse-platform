# Secrets and credentials (always applies)

- Never read, print, copy or modify `.env*` (except `.env.example`), `*.tfstate*`, `*.tfvars`,
  keys, `~/.aws/**`, `~/.ssh/**`. `.claude/hooks/guard_commands.py` enforces this for shell commands.
- Never print secrets in logs, test output, docs or chat. Do not run commands that dump resolved
  configuration (`printenv`, `docker compose config`, `docker inspect` without `--format`).
- New configuration: add the key with a **placeholder** (`change-me`, empty, or a clearly local
  value) to `.env.example`; read it via pydantic-settings / env vars. No secret defaults in code,
  Dockerfiles, `docker-compose.yml`, Spark config or Terraform.
- AWS: local = short-lived owner credentials (`aws login` or MFA role assumption; never keys in files); CI = GitHub OIDC role
  (no access keys in GitHub secrets); runtime = IAM execution role. Never create long-lived
  access keys for this project.
- GitHub Actions: secrets only for values that cannot use OIDC; non-secret config in `vars`;
  never `echo` a secret; use environments with required reviewers for deploy jobs.
- Backend/frontend projects (e.g. Supabase): service-role / secret keys stay server-side or in a
  secret store; never in frontend bundles or `NEXT_PUBLIC_*` / `VITE_*` variables; the
  publishable/anon key relies on row-level security.
- Suspected exposure: stop, tell the owner which file/commit (never the value), recommend rotation
  first, history cleanup second.
