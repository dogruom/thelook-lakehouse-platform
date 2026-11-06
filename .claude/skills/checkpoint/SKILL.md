---
name: checkpoint
description: Run the evidence-based daily, weekly or milestone checkpoint defined in docs/DELIVERY_PLAN.md and docs/PROGRESS.md - execute the verification commands, decide statuses strictly from evidence, update portfolio notes, run the tutor learning gate, and report to the owner. Use when a planned day, week or milestone is finished, when the owner says "günü/haftayı kapat", or asks "where are we / is this done?".
---

# Checkpoint (day | week | milestone)

Argument: `day W<w>.D<d>`, `week <w>` or `milestone M<n>`. Default: the active day in `Current State`.

## Day
1. Read the day card in `docs/DELIVERY_PLAN.md` and the day's task table in `docs/PROGRESS.md`.
2. Run every task's verification command (short output: `-q`, `--tb=line`, `--tail 20`).
3. Status per task: ✅ = commit hash + passing verification output · 🔄 = implemented, evidence
   missing or failing · ⏳ = not started · ❌ = blocked (name the blocker).
4. Fill the day's **Evidence** line: `commit <hash>; <command> → <result>` per ✅ task.
5. Check the day's Done-when; unmet items move to the next day explicitly (no silent carry-over).
6. Portfolio (only if something user-visible became ✅): append to `docs/PORTFOLIO_NOTES.md` —
   verified capability (with evidence link), one interview talking point (decision + trade-off),
   competency tag (Senior DE / Data Architect). Drafts stay in the "Draft" section, never in README.
6b. Day close report: append a section to `docs/DAY_CLOSE_REPORTS.md` (newest first) —
   executive summary · delivered (task IDs + status) · evidence (commit, command → result) ·
   reviews and audit verdicts · decisions (D-IDs) · not verified / known limitations · carry-overs ·
   risks · learning queued · next day · merge status (PR, CI run, merge commit).
6c. Merge under the standing approval in `CLAUDE.md` (D-032) when all its conditions hold;
   record the PR / CI / merge commit in the report.
7. Learning gate: run the `tutor` skill for the day's concepts. In **batched mode** (see tutor
   §0.1; active for W1) the tutor only *collects* (concept notes + queued questions) — no quiz.

## Week (in addition to all days of the week)
1. Run the week's regression set from DELIVERY_PLAN (unit, Spark, integration suites so far).
2. Run the weekly demo steps that are runnable locally; record duration and outcome.
3. Route `evidence-auditor` (week scope); apply its proposed status changes only where evidence supports them.
4. Update the macro status in PROGRESS from the count of ✅ tasks (not from effort spent).
5. `tutor` week gate: re-test `open`/`debt` topics + one spaced-review question per earlier week.

## Milestone
1. Week checkpoint for the last week, plus every milestone acceptance criterion in DELIVERY_PLAN.
2. Route `reviewer` (code mode, diff since the previous milestone tag; add `data` and `security`
   focus) and `evidence-auditor`. Suggest the owner runs the built-in `/security-review` once.
3. Demo runbook end-to-end from a stopped stack; record result and timing.
4. README: move verified items from `PORTFOLIO_NOTES.md` drafts into README with evidence links;
   update the "Implemented architecture" diagram so it shows **only** components that exist and run.
5. Gate verdict: PASS only if all acceptance criteria have evidence. On PASS, propose the release
   tag (`v0.1.0`, `v0.2.0`, `v1.0.0`) — tagging and pushing need the owner's go.
6. Write the "can claim / cannot claim" note for the milestone in PROGRESS; `tutor` milestone gate.

## Report to the owner (Turkish, compact)
Delivered · how it was verified · what you should understand (2–3 bullets) · gaps carried over ·
risks · next task. Never round up: partial is partial.
