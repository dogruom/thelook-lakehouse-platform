# AI-Assisted Development Playbook (owner's rules of engagement)

> **For**: the project owner. **What**: how to work with Claude Code on this repository day to day — what you do, what Claude does, what you must check. Tool commands are in [CLAUDE_CODE_CHEATSHEET.md](CLAUDE_CODE_CHEATSHEET.md); the plan is in [DELIVERY_PLAN.md](DELIVERY_PLAN.md).
> **Principle**: Claude delivers; you own the decisions, the understanding and the final acceptance.

---

## 1. Roles

| Who | Model | Does |
|---|---|---|
| **You** | — | Decide scope, architecture, cost, security; approve images/cloud/pushes; learn and defend the design |
| **Coordinator** (main session) | Opus, medium effort | Checks every prompt, plans, splits, sequences, chooses parallel work, delegates, verifies, summarises in Turkish |
| `implementer` | Sonnet | Production code from a task card, only in assigned files |
| `test-author` | Sonnet | Independent tests from acceptance criteria |
| `reviewer` | Sonnet | Design review before / code review after risky work (`data`, `security` focus) |
| `Explore`, `evidence-auditor` | Haiku | Cheap search; verification commands vs. claimed status |
| `tutor` (skill) | coordinator | Learning gate at every checkpoint |

## 2. Every prompt — your checklist

1. **Say the outcome, not the steps**: "Implement W1.D2" or "Why does the restart test fail?" — natural Turkish, dictation is fine.
2. **Name the task ID** when you can (`W1.D2`); otherwise say "Start today's task".
3. **Mention constraints only if new** (budget, deadline, "no image build today").
4. **Do not paste secrets**, `.env` content or tokens — ever. Backticked names from logs are fine.
5. **Expect pushback**: if Claude flags scope creep, over-engineering or a wrong assumption, decide in one line ("agreed" / "do it anyway because …").
6. **Answer approval prompts deliberately**: image builds, `git push`, `terraform apply`, any `aws` command and cloud spend always ask you. Read what will run before approving.

## 3. While work is running

- You may study in parallel; Claude works through the task card autonomously and stops only for real decisions.
- Glance at the **plan** it prints (classification, which agents, which files) — interrupt if it touches something you did not intend.
- If it starts editing without a plan on a non-trivial task: type `/work-orchestrator <task>`.
- Topic change → `/clear`. Very long session on the same topic → `/compact <focus>`. (Only you can run slash commands.)
- To save allowance on long routine stretches, you can switch the session to Sonnet (`/model sonnet`); the routing stays the same.

## 4. Day close (say "günü kapat")

Claude runs the `checkpoint` skill. You check:
1. **Evidence, not effort**: each ✅ in PROGRESS has a commit hash and a real command output. Partial = 🔄.
2. **Carry-overs** are named for tomorrow, not hidden.
3. **Portfolio notes**: new verified capability and one talking point added to [PORTFOLIO_NOTES.md](PORTFOLIO_NOTES.md).
4. **Tutor gate**: answer the questions honestly. If you fail, study the given resources or choose "proceed with learning debt" — never skip silently.
5. **Git**: work is committed; pushing is your approval.

## 5. Week and milestone checkpoints

- **Week (day 6)**: regression suite + demo + `evidence-auditor` + spaced-review questions. Read the gate verdict; if FAIL, the cut list in DELIVERY_PLAN §8 applies — scope shrinks, quality does not.
- **Milestone (days 12, 24, 36)**: full demo from a stopped stack, independent review, README updated **only** with verified items, "can claim / cannot claim" note, release tag (your go). Run `/security-review` once.
- Use these moments to practise the interview story: explain the milestone's main trade-off aloud in 2 minutes.

## 6. What you must still own

- Architecture and scope decisions (ADRs), cost approvals, anything outward-facing.
- Understanding: you should be able to explain every merged design decision without Claude.
- Honesty of claims: no exactly-once, GDPR compliance, production-readiness, invented metrics.

## 7. Session start and end

- **Start**: open VS Code in the repo, check the model picker (Opus · Medium), say **"Start today's task"**. Claude restores context from `Current State`, gives a 5-line brief, and starts.
- **End**: say **"oturumu kapat"** → `session-handoff` updates `Current State` (done, real test results, open issues, next task). Review the summary, approve the push.
- After a break of days: just say "devam edelim" — Claude recaps where things stand.

## 8. Daily routine and reply format

1. Morning: **"Sıradaki görevi yap"** (or "Start today's task"). Nothing else is needed.
2. Every substantive reply follows the standard format in [CLAUDE.md](../CLAUDE.md) (🎯 İstek → 🏷 Sınıf →
   ⚠ Not → ✅ Sonuç → 👀 Kontrol et → 🧠 Anla → ⏭ Sıradaki). Read 🎯 first: it is your prompt, normalised.
   Read 👀 and 🧠 last: they are your part of the work.
3. Your only fixed duties: approvals, the 👀 checks, and the checkpoint/tutor gate at day close.
4. Interview preparation is built in: each 🧠 line and each PORTFOLIO_NOTES talking point is
   something you should be able to say aloud without notes.

## 9. Anti-patterns

| Avoid | Because |
|---|---|
| Asking for "a full audit" again | Burns allowance; the plan and evidence already exist |
| Accepting ✅ without looking at the evidence line | Portfolio credibility depends on it |
| Letting the coordinator (Opus) write large code itself | Expensive; delegate to Sonnet agents |
| Running every agent on every task | Context and cost; routing decides |
| Adding tools/features not in the milestone acceptance | Scope creep; park them in the cut list |
