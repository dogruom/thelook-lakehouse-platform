---
name: work-orchestrator
description: Classify a non-trivial engineering task in this repository (complexity, risk, context, execution mode), recommend the right Claude model, plan it as small verifiable steps, execute and verify. Use at the start of any multi-file, architectural, data-correctness, infrastructure or ambiguous task, or when the owner dictates a loose request that must be turned into a plan.
---

# Work orchestrator

## 1. Normalise the request
Dictated requests may be fragmented. Restate in 2–4 lines:
- **Requested** (what the owner explicitly asked) vs **Assumed** (your inferences, marked as such).
- Ask a question only if the answer changes architecture, security, data integrity, cost or delivery.

## 2. Classify (print this block)
```
Complexity: trivial | normal | complex | architectural
Risk:       low | medium | high      (high = data loss, tenant leak, PII, cloud cost, history rewrite)
Context:    file | module | repository | external research
Mode:       direct | plan first | isolated subtask | review
Model:      Haiku | Sonnet | Opus   (+ one-line reason)
```

## 3. Model routing (verify availability with `/model`; the owner switches models, you cannot)
| Use | Model | Typical tasks here |
|---|---|---|
| Haiku | trivial, low risk | typo/format fixes, small doc edits, renames, explaining a command |
| Sonnet (default) | normal coding | generator models, tests, refactors, dbt models, DAGs, docs updates |
| Opus | architectural / hard | ADRs, CDC/SCD2/idempotency design, cross-module debugging, milestone reviews |

Say explicitly when the current task does **not** justify Opus. Raise effort level before switching
up a model tier when a task is merely tricky.

## 4. Plan (skip for trivial/direct)
- Prerequisites and dependencies (PROGRESS task IDs, e.g. `W1.D2.1 depends on W1.D1.2`).
- Files likely to change; acceptance criteria copied from PROGRESS.md; verification commands.
- Break into steps of ≤ ~1 hour, each independently verifiable. Use Plan mode (`/plan`) for
  architectural or high-risk work and wait for approval.

## 5. Execute the smallest safe unit
- Read only the files the step needs; prefer `Grep` and targeted reads over whole-repo scans.
- Subagents only for genuinely independent research or an independent review (they cost usage).
- Do not change core architecture or contracts without stating reason and scope first.

## 6. Verify
- Run the targeted checks (`pytest <file> -q`, `ruff check <paths>`, `mypy <pkg>`, dbt/Spark command).
- Review `git diff` for unintended changes, secrets, generated files and unrelated formatting.
- Report exact results, including failures and skipped checks. Never mark work ✅ without evidence.

## 7. Close
Explain what changed and why (decisions affecting behaviour in plain words, one teaching note if a
new concept appeared), list unfinished work, and propose the next best action with its model.
