# Claude Code + VS Code — Project Cheat Sheet

**Project:** `thelook-lakehouse-platform`  
**Environment:** Windows 11, VS Code, Claude Code native CLI + VS Code extension, Claude Pro  
**Last reviewed:** 2026-11-05

This is the working guide for using Claude Code as the primary engineering agent for this repository. Keep project-specific architecture and engineering rules in `CLAUDE.md` and `.claude/rules/`; this file is a practical operating reference, not a second source of project truth.

---

## 1. Account, model and usage

### Confirm the account before substantial work

In the Claude Code panel, run:

```text
/status
/usage
```

- `/status` — confirms the active account/authentication, Claude Code version, model and session details.
- `/usage` — opens Account & usage. With a `claude.ai` Pro sign-in, it shows the plan's current-session and weekly usage bars. It can also show usage attribution and recommendations.
- The CLI login and VS Code extension sign-in are distinct interfaces. Sign in to the extension with the same Claude Pro account when prompted; do not assume the CLI login alone proves that the panel is using it.
- The plan's usage is shared across Claude surfaces, including Claude Code and Claude chat. A long conversation, large context, attachments, model choice and effort level affect consumption.
- Do not enable usage credits or switch to Console/API billing unless deliberately choosing separate pay-as-you-go charges.

Also available in the account: `https://claude.ai/settings/usage` (Settings → Usage).

### Model selection

Use the model picker at the bottom of the Claude Code panel, or type `/model`.

Suggested starting policy:

| Work | Starting choice |
|---|---|
| Architecture decisions, difficult debugging, cross-system review | Opus 5.5 · Medium; use High only when justified |
| Normal feature implementation, tests, refactoring, documentation | Sonnet 5.5 · Default/Medium |
| Small, straightforward tasks | Haiku 5.5, when available |

`Medium`, `High`, etc. are **effort levels**, not separate models. Start with Medium. Raise effort only for a difficult task; more reasoning and longer context can consume the plan allowance faster. Model availability is determined by the account and the model picker.

---

## 2. VS Code shortcuts worth remembering (Windows)

| Shortcut | Action |
|---|---|
| `Ctrl+Shift+P` | Command Palette — find any VS Code or Claude Code command |
| `Ctrl+Shift+X` | Extensions |
| `Ctrl+P` | Quick-open a file by name |
| `Ctrl+Shift+F` | Search across the repository |
| `Ctrl+`` | Toggle integrated terminal |
| `Ctrl+K`, then `Ctrl+S` | Open Keyboard Shortcuts |
| `Alt+K` | Insert an `@` reference to the selected code/file context in the Claude prompt |
| `Ctrl+Alt+F` | Toggle Claude Code Focus view (when supported by the installed extension) |

Useful Command Palette entries:

- `Claude Code: Open in New Tab`
- `Claude Code: Open Walkthrough`
- `Developer: Reload Window` (only for extension/UI troubleshooting)

The Claude panel can be dragged to the right secondary sidebar. Keep Explorer on the left, editor in the centre and Claude on the right.

### Voice dictation

Use the microphone button in the Claude Code prompt box when it is available. Speak naturally; do not spend time pre-formatting the request. State the goal and context first, then let Claude turn the spoken explanation into a concise task plan. Review the plan before execution. If dictation is unavailable, use Windows voice typing with `Win+H` as a fallback.

---

## 3. Claude Code commands

Type `/` in the prompt box to see the commands available in the installed version. The list below is a practical core set; not every command is available in every release or sign-in mode.

| Command | Purpose |
|---|---|
| `/help` | Help and command discovery |
| `/status` | Account, active model and session status |
| `/usage` | Pro plan usage and session usage details |
| `/model` | Select model and, where supported, effort level |
| `/plan` | Enter Plan mode; `/plan <task>` starts planning a task |
| `/context` | Inspect context-window usage |
| `/compact` | Summarize/compact the conversation when context is getting large |
| `/clear` | Start a fresh conversation in the current workspace |
| `/resume` | Resume a previous conversation, when available |
| `/rewind` | Review available conversation/code checkpoints |
| `/diff` | Review uncommitted changes and Claude's changes |
| `/memory` | Inspect/manage loaded project instructions and memory |
| `/skills` | Browse available skills |
| `/permissions` | Inspect permission rules |
| `/export` | Export the conversation |
| `/copy` | Copy the latest response |
| `/btw <question>` | Ask a side question without adding it to the main conversation |
| `/logout` | Sign out of the current Claude account |

Use `/usage` and `/status` regularly. Do not use `/cost` as a substitute for the Pro usage dashboard; session cost is primarily useful for API/pay-as-you-go sessions.

### Bundled skills worth knowing

Claude Code also ships with built-in skills. Type `/` or `/skills` to see what is available in the installed version.

| Skill | Use |
|---|---|
| `/debug` | Investigate a bug systematically |
| `/code-review` | Review code for defects and risks |
| `/verify` | Build/run/verify a change (inspect the proposed commands first) |
| `/run` | Launch and exercise an app when the project has a known run recipe |
| `/batch` | Break a suitable larger task into parallel work |
| `/doctor` | Diagnose setup, instructions, skills and usage recommendations |
| `/claude-api` | Help with projects using the Anthropic API; not needed for this Pro-only workflow |

Do not install a third-party skill pack just because it exists. Start with these bundled skills and add project-specific skills only for repeatable workflows that actually recur. For this data platform, any skill that runs Docker, downloads data, migrates schemas or changes infrastructure must state its preconditions and ask before destructive/costly actions.

---

### Project skills (this repository)

Defined in `.claude/skills/`; Claude loads them automatically when relevant, or invoke them directly:

| Skill | Invoke | Use |
|---|---|---|
| `work-orchestrator` | `/work-orchestrator <task>` | Classify a task (complexity, risk, context, mode), pick the model, plan, execute, verify |
| `git-workflow` | `/git-workflow` | Safe status/diff review, staging, commits, branch, pull, push with explanations |
| `session-handoff` | `/session-handoff` | End a session: update the `Current State` block in `docs/PROGRESS.md` and report |

Path-scoped rules in `.claude/rules/` load only when matching files are touched (`python.md`, `data-platform.md`, `docs-claims.md`). Safety deny/ask rules live in `.claude/settings.json`; personal overrides go in `.claude/settings.local.json` (gitignored).

### Session routine for this project

- **Start**: open VS Code in the repository root, check `/status` (account, model), choose the model with `/model` (Sonnet by default), then say e.g. *"Resume: read Current State in PROGRESS and continue with the next task."*
- **Topic change**: `/clear` (the project memory is reloaded from `CLAUDE.md`). **Same topic, long context**: `/compact`.
- **End**: *"Session handoff"* (or `/session-handoff`), then review the diff and commit.

## 4. The operating loop: speak → plan → approve → execute → verify

### A. Start of each session

1. State the session goal in one or two sentences.
2. Ask Claude to read the relevant project documents and current code before proposing changes.
3. Ask it to check `git status` and review existing diffs before touching files.
4. Use Plan mode for multi-file changes, architectural decisions, infrastructure changes or uncertain tasks.
5. For a genuinely small, clear task, use Manual mode and proceed directly with reviewable edits.

Useful opening prompt:

> First inspect the current repository state, relevant docs and existing diffs. Do not overwrite or discard existing work. Summarize what you understand, identify assumptions and propose the smallest safe next step. Do not modify files until I approve the plan.

### B. Speak naturally; make Claude structure the work

A good spoken request does not need to sound like a formal prompt. Include what you are trying to achieve, why it matters and any constraints you remember. Then ask Claude to:

- restate the request as a precise technical objective;
- separate facts, assumptions and open questions;
- detect contradictions or missing requirements;
- split a broad task into ordered, testable subtasks;
- recommend the simplest robust approach;
- define acceptance criteria and tests;
- ask only questions that block a safe decision.

Do not ask it to produce a giant speculative plan for every small task. Match the planning depth to the risk and size of the change.

### C. During implementation

- Prefer small, reviewable changes over one huge rewrite.
- Keep the task and acceptance criteria visible in the conversation.
- Ask Claude to explain important trade-offs, not every line of routine code.
- Keep Manual permission mode as the default while the repository's current state is being understood.
- Use Plan mode before schema, data-contract, tenant-isolation, security, infrastructure or destructive changes.
- Do not approve commands you do not understand, especially deletion, cleanup, migration, force-push or infrastructure-destroy commands.

### D. End of each session

Ask Claude to:

1. Run relevant tests/checks and report exact results (including failures and skipped tests).
2. Review the final diff for unintended changes, secrets, generated files and unrelated formatting.
3. Update the appropriate project documentation if the implementation changed architecture, contracts, decisions or progress.
4. Update `docs/PROGRESS.md` with completed work, evidence, blockers and the next concrete task.
5. Provide a concise handoff: what changed, what was verified, what remains, and the exact command/task to resume.

Do not let it claim a task is complete just because code was written. Completion requires the agreed checks and evidence.

---

## 5. Responsibility split: what the agent does and what you review

### Delegate to Claude Code

- Inspecting the repository and tracing code paths.
- Turning dictated ideas into requirements, plans and acceptance criteria.
- Creating and maintaining Markdown documentation, ADRs and progress notes.
- Writing code, tests, SQL, configuration and repeatable scripts.
- Running approved tests and explaining failures.
- Reviewing diffs for correctness, consistency and likely regressions.
- Preparing Git commit messages and PR descriptions.

### Keep human ownership

- Product priorities and scope.
- Architecture choices and meaningful trade-offs.
- Data semantics, tenant isolation, privacy and security decisions.
- Approval of destructive or costly operations.
- Review of important diffs and test evidence.
- Deciding whether the result meets the project's real acceptance criteria.

You do **not** need to manually write routine Markdown or export small tasks to another model. Ask Claude Code to create/update the relevant document in the repository, then review it. Use another model or an external reviewer only when an independent second opinion is genuinely valuable, not as a routine prompt-optimization step.

---

## 6. Git, data and infrastructure safety for this repository

This repository previously had staged, unstaged, renamed, deleted and untracked changes. Treat that state as valuable existing work until it has been understood and backed up.

Before any significant task:

```powershell
git status --short --branch
git diff --stat
git diff --cached --stat
```

Review actual diffs when needed:

```powershell
git diff
git diff --cached
```

Safety rules:

- Never run `git reset --hard`, `git clean -fd`, `git checkout -- .`, bulk restore, stash or branch-switch commands without explaining the impact and receiving explicit approval.
- Never print or paste `.env`, tokens, passwords or private keys into chat. Use `.env.example` for configuration documentation.
- Do not delete Docker containers, images, volumes or build artifacts as a space-saving shortcut without inspecting the effect and obtaining approval.
- Do not run `docker system prune`, `docker volume prune`, `docker rm`, `docker rmi` or infrastructure destroy commands casually.
- Do not download datasets or regenerate data unless that is the approved task and sufficient disk space has been checked.
- Do not claim exactly-once delivery, GDPR erasure or tenant isolation without implementation evidence and tests.

For this project, prioritize tests for duplicate/replayed events, CDC ordering/deletes and tenant isolation before trusting the ingestion pipeline.

---

## 7. Project memory: keep one source of truth

Use a small, deliberate structure. Do not create every possible rule, skill, hook or plugin up front.

| Location | Responsibility |
|---|---|
| `CLAUDE.md` | Short project map, commands, non-negotiable constraints and pointers to detailed docs |
| `.claude/rules/` | Focused engineering rules (Python, streaming/data contracts, tests/security) |
| `.claude/skills/` | Reusable workflows with clear triggers and steps |
| `.claude/settings.json` | Shared project settings only when deliberately agreed |
| `.claude/settings.local.json` | Personal/local project settings; do not commit local-only preferences |
| `docs/adr/` | Architecture decisions and their rationale |
| `docs/PROGRESS.md` | Current state, verified milestones, blockers and next step |

Keep `CLAUDE.md` concise. Put detailed domain knowledge in existing `docs/` files and link to it from `CLAUDE.md`. A skill should represent a repeated workflow, not merely a long prompt. Create a new rule or skill only after a real need appears.

---

## 8. Prompt patterns you can reuse

### Understand before changing

> Inspect the relevant files and current Git diff. Explain the current behavior, the intended behavior, the gap and the smallest safe fix. Do not edit anything yet.

### Turn a voice note into an actionable task

> Normalize my explanation into a precise engineering brief. Preserve my intent, resolve obvious speech-to-text errors from context, flag uncertain interpretations, split the work into logical tasks, and define acceptance criteria. Ask only blocking questions. Do not implement until I approve.

### Plan a risky change

> Enter Plan mode. Inspect the relevant implementation, contracts, tests and docs. List affected files, migration/compatibility risks, rollback considerations and verification steps. Do not modify files or run destructive commands. Wait for my approval.

### Implement an approved plan

> Implement only the approved plan. Keep the diff focused. Add or update tests, run the relevant checks, inspect the final diff and update the appropriate docs. Report evidence and any unresolved risks. Do not commit unless I explicitly ask.

### Review before merge/commit

> Review the current diff as a skeptical senior engineer. Look for correctness bugs, data loss, security/privacy issues, tenant leakage, broken contracts, missing tests and unrelated changes. Prioritize findings by severity and cite file/line locations. Do not edit files.

### Close the session

> Prepare a handoff for the next session. Update `docs/PROGRESS.md` only with verified facts. Record what changed, commands/tests and their actual results, unresolved issues, decisions made and the single best next task. Do not claim unverified work is complete.

---

## 9. GitHub: when to connect it

Local Git works without signing in to GitHub. Do not connect GitHub just to use Claude Code on the local repository.

Authenticate GitHub later when you need to push/pull a remote repository, create or review pull requests, manage issues, or use GitHub-specific automation. Keep Git version control and Claude Pro authentication conceptually separate.

---

## 10. Official references

- Claude Code in VS Code: https://code.claude.com/docs/en/vs-code
- Claude Code best practices: https://code.claude.com/docs/en/best-practices
- Claude Code memory / `CLAUDE.md`: https://code.claude.com/docs/en/memory
- Claude Code skills: https://code.claude.com/docs/en/skills
- Claude Code settings: https://code.claude.com/docs/en/settings
- Claude Pro usage: https://support.claude.com/en/articles/8325606-what-is-the-pro-plan
- Claude Code usage limits: https://support.claude.com/en/articles/14552983-models-usage-and-limits-in-claude-code

**Guiding principle:** Claude Code should do the implementation and documentation work; you should own intent, decisions, approvals and evidence-based review.
