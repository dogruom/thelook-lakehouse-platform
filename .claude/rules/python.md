---
paths:
  - "**/*.py"
  - "pyproject.toml"
  - "requirements/**"
---

# Python rules

- Python 3.11+; type hints on all public functions; code must pass `ruff check`, `ruff format --check`
  and `mypy` for the touched package before it is called done.
- Data models and contracts: Pydantic v2 `BaseModel` with `Field(..., description=...)` constraints;
  cross-field rules via `@model_validator(mode="after")`.
- Configuration via `pydantic-settings` (`GEN_` prefix for the generator). No hard-coded credentials,
  endpoints or secrets; defaults must be obviously local-dev values and mirrored in `.env.example`.
- Logging via `src/utils/logging.py` (structlog JSON, include `brand_id` context). No `print()` outside
  CLI entry points and `scripts/`.
- Determinism: every generator accepts a seed; the same seed and config produce identical output.
  Use a local `random.Random(seed)` / `Faker.seed_instance`, never global random state.
- Tests: write or update a pytest test for each behaviour change. Unit tests run offline (mock S3 with
  `moto`, no Kafka or Docker). Prefer small, targeted runs: `pytest tests/unit/test_x.py -q`.
- Dependencies: pin in `requirements/*.txt`; keep `pyproject.toml` ranges consistent. Do not add a
  dependency without stating why.
