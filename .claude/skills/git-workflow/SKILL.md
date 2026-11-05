---
name: git-workflow
description: Safely inspect, stage, commit, branch, pull, push and merge in this repository while teaching the owner what each Git step does. Use whenever the task involves git status/diff review, creating a branch, committing, syncing with GitHub (dogruom/thelook-lakehouse-platform) or preparing a pull request.
---

# Git workflow (safe by default)

Reference for the owner: [docs/GIT_CHEATSHEET.md](../../../docs/GIT_CHEATSHEET.md).

## Branch model
- `main` = integrated, pushed history. Work on short-lived branches from `main`:
  `feat/<scope>`, `fix/<scope>`, `docs/<scope>`, `chore/<scope>`. Merge back via PR (or `--ff-only`
  when working solo and the branch is linear). `develop` is legacy; do not build on it.
- Backup branches `backup/*` are recovery points; never delete them without approval.

## 1. Inspect (always first)
```powershell
git status --short --branch
git diff --stat; git diff --cached --stat
git log --oneline --graph --decorate -10
git fetch origin; git status -sb          # shows ahead/behind vs origin
```
Understand every change before staging. Older changes are not automatically obsolete.

## 2. Stage deliberately
- `git add <paths>` per logical change; `git add -p` to split a file. Never `git add .` blindly.
- Before committing, check: `git diff --cached --stat` and `git diff --cached` for secrets, `.env`,
  data files, notebooks outputs, CV documents, `__pycache__`.
- Unstage without touching files: `git restore --staged <path>`.

## 3. Commit
- Conventional Commits: `type(scope): imperative summary` (≤ 72 chars), body explains *why*.
- One logical change per commit; docs that describe the change go in the same commit.
- Append the attribution trailer required by the session (e.g. `Co-Authored-By: ...`).

## 4. Sync and publish
- `git pull --ff-only` (never create surprise merge commits on `main`). If it fails, stop and explain.
- `git push -u origin <branch>` for a new branch; `git push` afterwards. Never force-push.
- Push only after tests/checks relevant to the change ran and the diff was reviewed.

## Prohibited without explicit, specific approval
`git reset --hard`, `git clean`, `git checkout -- .`, `git restore <path>` (discards edits),
`git push --force*`, `git rebase` on pushed branches, `git branch -D`, `git stash drop/clear`,
`git filter-*`, amending pushed commits. If history or remote state threatens data loss: stop,
list safe options, let the owner decide.

## Teach while doing
For each new concept give one line each: what it does · why it matters · the command · what to
inspect to confirm it worked (e.g. `git log origin/main..main` shows unpushed commits).
