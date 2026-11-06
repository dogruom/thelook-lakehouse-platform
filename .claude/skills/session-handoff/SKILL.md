---
name: session-handoff
description: Close a working session on this repository with a compact, durable handoff. Updates only the "Current State" block at the top of docs/PROGRESS.md and task statuses with verified evidence, then reports the handoff in chat. Use when the owner says the session is ending, asks for a summary/handoff, or before /clear or /compact on a long session.
---

# Session handoff

## 1. Gather facts (do not rely on memory of the chat)
```powershell
git status --short --branch
git log --oneline origin/main..HEAD     # unpushed work
git diff --stat
```
Re-run or quote the checks that were actually executed this session. Unknown = "not run".

## 2. Update docs/PROGRESS.md
- Replace the `## Current State` block (keep it ≤ 25 lines). Fields:
  `Updated` (date) · `Objective` · `Completed` · `Files changed` · `Tests & results` ·
  `Decisions` (link ADRs) · `Risks / blockers` · `Next task` (PROGRESS task ID) ·
  `Next mode` (direct / direct+review / parallel; Sonnet + effort level — see `work-orchestrator` §7).
- The block is the cheapest resume point for the next session: facts only, no narrative, no
  duplicated plan text (link the DELIVERY_PLAN day card instead).
- Update task statuses: ✅ only with commit hash + verification output; otherwise 🔄.
- Do **not** append session diaries or chat transcripts to PROGRESS (the factual per-session trace lives in SESSION_LOG, §2b). Durable architectural decisions go into an
  ADR; durable conventions go into `CLAUDE.md` or `.claude/rules/` (only if they will recur).

## 2b. Append to docs/SESSION_LOG.md (newest entry first)
Build the entry from `git log --name-status <first session commit>^..HEAD` — not from chat memory:
tasks (IDs + status) · commits / PR links · decision IDs added (DECISION_LOG) · files as clickable
repo-relative links with one line each · verification results · learning items queued · portfolio
items. Every decision taken in the session must already have a DECISION_LOG row; add it if missing.

## 3. Report in chat (Turkish, compact)
Objective · completed work · files changed · tests and exact results · decisions · risks/blockers ·
next recommended task · recommended mode/effort and why · whether `/clear` (topic change) or
`/compact` (same topic, long context) is advisable before continuing.

## 4. Commit (only if the owner authorised commits for this session)
Commit the PROGRESS/doc updates with `docs(progress): session handoff YYYY-MM-DD` via the
`git-workflow` skill. Otherwise leave them staged-ready and say so.
