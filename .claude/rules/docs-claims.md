---
paths:
  - "docs/**"
  - "README.md"
  - "**/*.md"
---

# Documentation rules

- Write for a reviewer or hiring manager: professional English, concise, no marketing superlatives
  ("world-class", "top 1%", "irresistible").
- Separate **design intent** from **evidence**. Status markers: ✅ verified (commit hash + verification
  output or CI link), 🔄 in progress / implemented without evidence, ⏳ / 📋 planned.
- Numbers (latency, speed-up, cost, setup time) appear only with methodology and measured results
  (Charter §8). Otherwise label them as targets or remove them.
- Follow the honest-claims policy in `CLAUDE.md` (no exactly-once, GDPR-compliance, production-ready,
  Databricks-compatibility claims without evidence).
- Update an existing document before creating a new one. Bump the version/date header when the
  meaning of a document changes. Architecture decisions go into `docs/adr/NNNN-<slug>.md`
  (Context → Decision → Consequences).
- Use repository-relative links. Never paste secrets, `.env` content or personal data.
