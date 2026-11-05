# Git Quick Reference — thelook-lakehouse-platform

> **Last reviewed**: 2026-11-05 · Remote: `origin` → `https://github.com/dogruom/thelook-lakehouse-platform.git`
> Commands work in PowerShell and Git Bash. Run them from the repository root.

## 0. The mental model (read once)

```
 working tree  --git add-->  index (staging)  --git commit-->  local repo  --git push-->  GitHub (origin)
   your files                  next commit                      your history              shared history
 <--git restore--         <--git restore --staged--                       <--git fetch / pull--
```

- A **commit** exists only on your machine until you **push**. GitHub does not see local commits.
- `origin/main` is your machine's *last-known copy* of GitHub's `main`; `git fetch` refreshes it.
- A **branch** is a movable label pointing to a commit. Creating one is cheap and safe.

## 1. Daily loop

| Step | Command | What you should see |
|---|---|---|
| Where am I? | `git status --short --branch` | `## feat/x...origin/feat/x [ahead 1]` plus changed files (`M` modified, `A` added, `D` deleted, `R` renamed, `??` untracked) |
| What changed (unstaged)? | `git diff` / `git diff --stat` | Line-level changes not yet staged |
| What will be committed? | `git diff --cached` | Exactly the content of the next commit |
| Stage specific files | `git add src/generator/config.py tests/unit/test_config.py` | Files move to "staged" in `git status` |
| Stage part of a file | `git add -p <file>` | Interactive `y/n/s` per hunk (run it yourself in a terminal) |
| Unstage (keeps edits) | `git restore --staged <file>` | File back to "modified", content untouched |
| Commit | `git commit -m "feat(generator): add customer CDC model"` | `[branch abc1234] feat(...)` |
| History | `git log --oneline --graph --decorate -15` | Commits and where branches point |
| Unpushed commits | `git log --oneline origin/main..HEAD` | Empty = nothing to push |
| Update from GitHub | `git fetch origin` then `git pull --ff-only` | `Fast-forward` or `Already up to date` |
| Publish | `git push` (first time: `git push -u origin <branch>`) | `abc..def  branch -> branch` |

## 2. Branches

```powershell
git switch main                      # go to main
git pull --ff-only                   # bring it up to date
git switch -c feat/generator-cdc     # create + switch to a new branch
git branch -vv                       # list branches with upstream and ahead/behind
git push -u origin feat/generator-cdc
```

Finishing a branch (solo, linear history):
```powershell
git switch main
git pull --ff-only
git merge --ff-only feat/generator-cdc   # fails safely if a real merge would be needed
git push
```
With a pull request instead: push the branch, open a PR on GitHub, review the diff and CI, then merge there
and run `git switch main; git pull --ff-only` locally.

**Naming**: `feat/<scope>`, `fix/<scope>`, `docs/<scope>`, `chore/<scope>`. `backup/*` branches are recovery points.

## 3. Commit messages (Conventional Commits)

```
feat(generator): add customer CDC model with _cdc_sequence      <- type(scope): imperative, <= 72 chars

Sequence is monotonic per (brand_id, customer_id) so Silver MERGE  <- body: why, not how
can reject stale updates regardless of arrival order.
```
Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `ci`, `build`.

## 4. Common situations

| Situation | Safe command | Notes |
|---|---|---|
| Staged the wrong file | `git restore --staged <file>` | Content is kept |
| Typo in the last commit message, **not pushed** | `git commit --amend -m "..."` | Never amend pushed commits |
| Forgot a file in the last commit, **not pushed** | `git add <file>; git commit --amend --no-edit` | Same rule |
| Want to undo a pushed commit | `git revert <hash>` | Creates a new "undo" commit; history stays intact |
| Need to switch branch but have edits | Commit them to a WIP commit on your branch | Avoid `stash` until comfortable with it |
| `git pull --ff-only` refuses | `git log --oneline --graph --all -15` and ask | Local and remote diverged: decide merge vs rebase consciously |
| See a file as it was in a commit | `git show <hash>:path/to/file` | Read-only |
| Find who/when changed a line | `git log -p -- <file>` / `git blame <file>` | Read-only |
| Line-ending warnings (`LF will be replaced by CRLF`) | Nothing — `.gitattributes` handles it | `.sh`, Dockerfiles, YAML stay LF |
| "dubious ownership" error | `git config --global --add safe.directory <repo path>` | Already configured for this repo |

## 5. Before every push — checklist

1. `git status` is clean or only contains what you intend to leave uncommitted.
2. `git log --oneline origin/main..HEAD` lists only commits you understand.
3. `git diff origin/main --stat` contains no `.env`, `data/`, `.docx`, credentials or large files.
4. Relevant tests/checks ran and passed (or the failure is documented).

## 6. Dangerous commands — do not run casually

| Command | Why it is dangerous | Safer alternative |
|---|---|---|
| `git reset --hard` | Deletes uncommitted work irreversibly | `git restore --staged`, WIP commit, backup branch |
| `git clean -fd` | Deletes untracked files (no undo) | Inspect with `git clean -n` (dry run) and delete manually |
| `git checkout -- .` / `git restore .` | Discards all edits | Restore single files after reviewing `git diff <file>` |
| `git push --force` | Overwrites GitHub history for everyone | `git revert`, or `--force-with-lease` only on your own unshared branch, with approval |
| `git rebase` on a pushed branch | Rewrites shared history | Merge, or rebase only local unpushed commits |
| `git branch -D` | Deletes a branch even if unmerged | `git branch -d` (refuses if unmerged) |

Claude Code is configured (`.claude/settings.json`) to deny or ask before these operations.

## 7. Recovery

- **Lost a commit?** `git reflog` lists every position HEAD has had; `git branch rescue <hash>` restores it.
- **Recovery points in this repo**: `backup/pre-consolidation-2026-11-05` (state before the 2026-11-05 consolidation).
