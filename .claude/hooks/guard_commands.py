"""PreToolUse guard for shell commands and file tools.

Shell (Bash, PowerShell): blocks commands that would print secrets, destroy data, use the
network directly or push to main - cases that prefix-based permission rules in
.claude/settings.json cannot catch reliably (option reordering, `git -c ...`, piping, aliases).

File tools (Read, Edit, Write, NotebookEdit, Glob, Grep): blocks paths outside the project,
the user's ~/.claude directory and the Claude temp/scratchpad directory.

Autonomous mode (env LAKEHOUSE_AUTORUN=1, set by scripts/autonomous/run_queue.py): additionally
protects the guardrails themselves (settings, hooks, the supervisor agent, the loop scripts)
from the unattended agent and blocks nested `claude` invocations.

This is a policy layer, not an OS sandbox: an allowed interpreter (`python -c ...`) can still
reach the network or the disk. The Windows sandbox is not available on this Claude Code build
(`claude sandbox status`), see docs/DECISION_LOG.md D-041.

Contract (Claude Code hooks): JSON on stdin, exit 0 = allow, exit 2 = block with the reason
on stderr. Run `python .claude/hooks/guard_commands.py --self-test` to verify.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sys
from pathlib import Path, PureWindowsPath


SHELL_TOOLS = {"Bash", "PowerShell"}
FILE_TOOLS = {"Read", "Edit", "Write", "NotebookEdit", "Glob", "Grep"}
WRITE_TOOLS = {"Edit", "Write", "NotebookEdit"}

SECRET_PATH = re.compile(
    r"""(?ix)
    (?<![\w.-])
    (?:[\w./\\~-]*[/\\])?
    (?:
        \.env(?:\.(?!example\b|template\b|sample\b)[\w.-]+)?   # .env, .env.local, not .env.example
      | [\w-]*\.tfstate(?:\.backup)?
      | [\w-]*\.tfvars
      | s3\.local\.json
      | id_(?:rsa|ed25519)
      | [\w-]+\.pem
      | \.aws[/\\]credentials
      | \.docker[/\\]config\.json
    )
    (?![\w-])
    (?!\.(?:example|template|sample)\b)
    """
)

# Commands that mention a secret path without printing its content.
SAFE_SECRET_MENTION = re.compile(
    r"(?i)^\s*(git\s+(check-ignore|ls-files|status)|test\s+-[ef]|Test-Path)\b"
)

# Start of a command: beginning, or after a separator / pipe / subshell / assignment.
_CMD = r"(?i)(?:^|[;&|(=]\s*)"
_NETWORK_REASON = (
    "Direct network commands are blocked. Use WebFetch (domain allowlist) for documentation."
)

BLOCK_RULES: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"(?i)(^|[;&|(]\s*|\bexec\s+\S+\s+)(printenv|env)\s*($|[|;&>)])"),
        "Dumping environment variables can print secrets. Check a single variable "
        'with `[ -n "$VAR" ] && echo set` instead.',
    ),
    (
        re.compile(r"(?i)\b(Get-ChildItem|gci|dir|ls)\s+env:"),
        "Listing env: can print secrets. Test a single variable instead.",
    ),
    (
        re.compile(
            r"(?i)\bdocker\s+compose\b(?=.*\bconfig\b)(?!.*--(quiet|services|volumes|profiles)\b)"
        ),
        "`docker compose config` prints resolved secrets. Use `docker compose config --quiet` "
        "to validate or `--services` to list services.",
    ),
    (
        re.compile(r"(?i)\bdocker\s+(container\s+)?inspect\b(?!.*(--format|-f)\b)"),
        "`docker inspect` without --format prints container env (secrets). Use --format.",
    ),
    (
        re.compile(r"(?i)\baws\s+configure\s+(get|export-credentials)\b"),
        "This prints AWS credentials. The owner runs it in their own terminal.",
    ),
    (
        re.compile(r"(?i)\bgh\s+auth\s+(token|status\s+.*--show-token)\b"),
        "This prints the GitHub token.",
    ),
    (
        re.compile(
            r"(?i)\bgit\b(\s+-c\s+\S+)*\s+(reset\s+--hard|clean\b|filter-branch|filter-repo)"
        ),
        "Destructive git operation (reset --hard / clean / history rewrite) requires the owner.",
    ),
    (
        re.compile(
            r"(?i)\bgit\b(\s+-c\s+\S+)*\s+push\b.*(\s--force(-with-lease)?\b|\s-f\b|\s\+\S)"
        ),
        "Force push is not allowed without explicit owner approval.",
    ),
    (
        re.compile(r"(?i)\bgh\s+pr\s+merge\b.*--admin\b"),
        "`gh pr merge --admin` bypasses required checks (D-032 needs green CI).",
    ),
    (
        re.compile(
            r"(?i)\bdocker\s+(compose\s+(\S+\s+)*?)?run\b.*" r"(\s(-v|--volume)[\s=]\S*:|--mount\b)"
        ),
        "Mounting host paths into a container bypasses the path guard. Use the volumes "
        "defined in docker-compose.yml.",
    ),
    (
        re.compile(r"(?i)\bdocker\s+compose\b.*\bdown\b.*(\s-v\b|--volumes|--rmi)"),
        "`docker compose down -v/--rmi` deletes volumes or images. Use `docker compose stop`.",
    ),
    (
        re.compile(
            r"(?i)\bdocker\s+((system|volume|image|container|builder|network)\s+prune"
            r"|volume\s+rm|rmi|rm|compose\s+rm)\b"
        ),
        "Deleting Docker containers, images or volumes requires explicit owner approval.",
    ),
    (
        re.compile(r"(?i)(?<!git )(?<!git)(?<![-\w])rm\s+(\S+\s+)*(-[a-z]*r[a-z]*|--recursive)\b"),
        "Recursive deletion is blocked. Delete single files, or ask the owner.",
    ),
    (
        re.compile(
            r"(?i)(?<!git )(?<![-\w])(Remove-Item|ri|rm|rmdir|rd|del|erase)\b[^;&|]*"
            r"\s-r(e(c(u(r(s(e)?)?)?)?)?)?\b"
        ),
        "Recursive deletion (Remove-Item -Recurse) is blocked. Delete single files, or ask the owner.",
    ),
    (
        re.compile(r"(?i)\b(del|erase|rd|rmdir)\b[^;&|]*\s/s\b"),
        "Recursive deletion (del /s, rd /s) is blocked.",
    ),
    (
        re.compile(
            _CMD + r"(curl(\.exe)?|wget(\.exe)?|Invoke-WebRequest|iwr|Invoke-RestMethod|irm"
            r"|Start-BitsTransfer)\b"
        ),
        _NETWORK_REASON,
    ),
    (
        re.compile(r"(?i)\bcertutil\b.*-urlcache\b|\bbitsadmin\b.*/transfer\b"),
        "Download via certutil/bitsadmin is blocked.",
    ),
    (
        re.compile(r"(?i)--dangerously-skip-permissions|--allow-dangerously-skip-permissions"),
        "Bypassing permission checks is never allowed from an agent.",
    ),
]

# Guardrail files the unattended agent must not change (autonomous mode only): permissions,
# hooks, agent/skill/rule definitions, project instructions, the loop and CI (the D-032 gate).
PROTECTED_PATHS = (
    ".claude/settings.json",
    ".claude/settings.local.json",
    ".claude/hooks/",
    ".claude/agents/",
    ".claude/skills/",
    ".claude/rules/",
    ".claude/runs/",
    "CLAUDE.md",
    "scripts/autonomous/",
    ".github/",
)
_PROTECTED_IN_COMMAND = re.compile(
    r"(?i)\.claude/(settings(\.local)?\.json|hooks/|agents/|skills/|rules/|runs/)"
    r"|scripts/autonomous/|\.github/|(^|[\s\"'/])CLAUDE\.md\b"
)
_PUSH_BLOCK_FLAGS = {
    "--force",
    "--force-with-lease",
    "--force-if-includes",
    "--mirror",
    "--all",
    "--delete",
    "--prune",
}
_GIT_GLOBAL_WITH_ARG = {"-c", "--git-dir", "--work-tree", "--namespace", "--exec-path"}
_PROTECTED_BRANCHES = {"main", "master"}
_FORCE_REASON = "Force, mirror or delete pushes are not allowed without explicit owner approval."
_MAIN_REASON = "Direct push to main is not allowed; push the feature branch and merge via a PR."


def _tokens(segment: str) -> list[str]:
    try:
        return shlex.split(segment, posix=True)
    except ValueError:
        return segment.split()


def _git_push_reason(command: str, *, autorun: bool) -> str | None:
    """Parse every `git ... push ...` segment (global options, quotes, refs/heads/ prefixes)."""
    for segment in re.split(r"\|\||&&|[;|\n]", command):
        tokens = _tokens(segment)
        start = next(
            (i for i, t in enumerate(tokens) if Path(t).name.lower() in {"git", "git.exe"}), None
        )
        if start is None:
            continue
        j = start + 1
        while j < len(tokens) and tokens[j].startswith("-"):
            j += 2 if tokens[j].lower() in _GIT_GLOBAL_WITH_ARG or tokens[j] == "-C" else 1
        if j >= len(tokens) or tokens[j].lower() != "push":
            continue
        args = tokens[j + 1 :]
        flags = [a.split("=", 1)[0] for a in args if a.startswith("-")]
        refs = [a for a in args if not a.startswith("-")]
        if any(f.lower() in _PUSH_BLOCK_FLAGS for f in flags) or any(
            re.fullmatch(r"-[a-zA-Z]*[fd][a-zA-Z]*", f) for f in flags
        ):
            return _FORCE_REASON
        for ref in refs[1:]:
            if ref.startswith("+"):
                return _FORCE_REASON
            dst = ref.split(":")[-1].strip("\"'")
            dst = dst[len("refs/heads/") :] if dst.lower().startswith("refs/heads/") else dst
            if dst.lower() in _PROTECTED_BRANCHES:
                return _MAIN_REASON
            if autorun and dst.upper() == "HEAD":
                return "In autonomous mode push an explicit branch name, not HEAD."
        if autorun and len(refs) < 2:  # - remote + branch
            return "In autonomous mode use an explicit `git push -u origin <feature-branch>`."
    return None


_AUTORUN_SHELL_RULES: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(_CMD + r"(claude(\.exe)?)(\s|$)", re.IGNORECASE),
        "Nested `claude` invocations are blocked in autonomous mode.",
    ),
]


def evaluate(command: str, *, autorun: bool = False) -> str | None:
    """Return a block reason for a shell command, or None when it is allowed."""
    stripped = command.strip()
    for pattern, reason in BLOCK_RULES:
        if pattern.search(command):
            if reason is _NETWORK_REASON and stripped.lower().startswith("docker "):
                continue  # network calls inside containers (healthchecks, S3 probes) are fine
            return reason
    push_reason = _git_push_reason(command, autorun=autorun)
    if push_reason:
        return push_reason
    segments = re.split(r"\|\||&&|[;|\n]", command)
    if any(SECRET_PATH.search(seg) and not SAFE_SECRET_MENTION.search(seg) for seg in segments):
        return (
            "The command references a secret file (.env, *.tfstate, *.tfvars, keys). "
            "Never read or modify it from the agent; document settings in .env.example."
        )
    if autorun:
        for pattern, reason in _AUTORUN_SHELL_RULES:
            if pattern.search(command):
                return reason
        normalised = re.sub(r"/(\./)+", "/", re.sub(r"[/\\]+", "/", command))
        if _PROTECTED_IN_COMMAND.search(normalised) and not re.fullmatch(
            r"(?i)\s*(python|py)\s+\.claude[/\\]hooks[/\\]guard_commands\.py\s+--self-test\s*",
            command,
        ):
            return "Guardrail files (settings, hooks, supervisor, loop scripts) are owner-only."
    return None


def _to_path(raw: str, cwd: Path, home: Path) -> Path:
    """Normalise Windows, Git-Bash (/c/...) and ~ paths to an absolute path."""
    text = raw.strip().strip('"').strip("'")
    if text.startswith("~"):
        text = str(home) + text[1:]
    git_bash = re.match(r"^/([a-zA-Z])(/.*)?$", text)
    if git_bash:
        text = f"{git_bash.group(1)}:{git_bash.group(2) or '/'}"
    path = Path(text)
    if not path.is_absolute() and not PureWindowsPath(text).is_absolute():
        path = cwd / path
    return Path(os.path.normpath(str(path)))


def _within(path: Path, root: Path) -> bool:
    p, r = os.path.normcase(str(path)), os.path.normcase(str(root))
    return p == r or p.startswith(r.rstrip("\\/") + os.sep)


def evaluate_path(
    tool: str,
    tool_input: dict[str, object],
    *,
    project_dir: Path,
    home: Path,
    temp_root: Path,
    autorun: bool = False,
) -> str | None:
    """Return a block reason for a file-tool call, or None when it is allowed."""
    raw = tool_input.get("file_path") or tool_input.get("notebook_path") or tool_input.get("path")
    if not raw or not isinstance(raw, str):
        return None  # Glob/Grep without a path search the working directory
    path = _to_path(raw, project_dir, home)
    roots = [project_dir, home / ".claude", temp_root]
    if not any(_within(path, root) for root in roots):
        return (
            f"{tool} outside the project is blocked ({path}). Allowed: the project, "
            "~/.claude and the Claude temp/scratchpad directory."
        )
    if SECRET_PATH.search(str(path).replace("\\", "/")):
        return f"{tool} on a secret file is blocked ({path.name}); use .env.example."
    if tool in WRITE_TOOLS and autorun:
        if _within(path, project_dir):
            rel = os.path.normcase(os.path.relpath(path, project_dir)).replace("\\", "/")
            for protected in PROTECTED_PATHS:
                p = os.path.normcase(protected).replace("\\", "/")
                if rel == p or (p.endswith("/") and rel.startswith(p)):
                    return f"{rel} is a guardrail file; only the owner changes it."
        elif _within(path, home / ".claude") and not _within(path, home / ".claude" / "projects"):
            return "User-level Claude settings are owner-only in autonomous mode."
    return None


def _default_temp_root() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    base = Path(local) / "Temp" if local else Path(os.environ.get("TEMP", "/tmp"))
    return base / "claude"


SELF_TEST: list[tuple[str, bool]] = [
    ("cat .env", True),
    ("Get-Content .\\.env", True),
    ("type .env.local", True),
    ("cp .env.example .env", True),
    ("cat .env.example", False),
    ("git check-ignore -v .env", False),
    ("cd repo && git check-ignore -q .env && echo ignored", False),
    ("git status; cat .env", True),
    ("echo ok | type .env", True),
    ("grep -n S3 .env.example", False),
    ("docker compose config", True),
    ("docker compose config --quiet", False),
    ("docker compose --profile core config --services", False),
    ("docker inspect crm_kafka", True),
    ("docker inspect --format '{{.State.Health.Status}}' crm_kafka", False),
    ("printenv", True),
    ("env | sort", True),
    ("env FOO=1 python x.py", False),
    ("Get-ChildItem env:", True),
    ("gh auth token", True),
    ("gh pr create --fill", False),
    ("git -c core.x=y reset --hard HEAD~1", True),
    ("git push --force origin main", True),
    ("git push --force-with-lease origin feat/x", True),
    ("git push origin main", True),
    ("git push origin HEAD:main", True),
    ("git push -u origin feat/x", False),
    ("git push -u origin chore/autonomous-run", False),
    ("docker compose down -v", True),
    ("docker compose stop", False),
    ("docker volume rm crm_kafka_data", True),
    ("docker rm crm_kafka", True),
    ("docker compose up -d", False),
    ("docker compose build spark-master", False),
    ("rm -rf data", True),
    ("rm -rf htmlcov", True),
    ("rm -r build", True),
    ("rm -f -r build", True),
    ("rm build/x.txt", False),
    ("git rm -r --cached build", False),
    ("Remove-Item -Recurse -Force build", True),
    ("Remove-Item build -Recurse", True),
    ("Remove-Item build -rec -Force", True),
    ("Remove-Item build\\x.txt", False),
    # `--rm` is a docker flag, not a delete (false positive seen in W2.D8 and W2.D12)
    ("docker compose --profile tools run --rm spark-test pytest tests/spark -q -rs", False),
    ("docker compose run --rm -T seaweedfs-init -c 'aws s3 ls s3://b/ --recursive'", False),
    ("docker compose run --rm x sh -c 'rm -rf /data'", True),
    ("docker run --rm img; rm -r build", True),
    ("rm --recursive build", True),
    ("Remove-Item -Path build --rm-like-flag -Recurse", True),
    ("del /s /q build", True),
    ("rd /s /q build", True),
    ("curl https://example.com", True),
    ("curl.exe -sS https://example.com", True),
    ("wget https://example.com", True),
    ("Invoke-WebRequest https://example.com -OutFile x", True),
    ("iwr https://example.com", True),
    ("Invoke-RestMethod https://api.github.com", True),
    ("$r = irm https://example.com", True),
    ("$r = Invoke-RestMethod https://example.com", True),
    ("docker compose exec seaweedfs curl -s localhost:8333", False),
    ("python -m pytest tests/unit -q", False),
    ("cat infra/terraform/terraform.tfstate", True),
    ("terraform plan -var-file=dev.tfvars", True),
    ("aws configure export-credentials --profile dev", True),
    ("pytest tests/unit -q", False),
    ("ruff check src tests", False),
    ("claude -p hi --dangerously-skip-permissions", True),
    # Review regressions (2026-11-06): push/merge parsing and host mounts.
    ("git push", False),
    ("git push origin HEAD:refs/heads/main", True),
    ('git push origin "main"', True),
    ("git push --mirror origin", True),
    ("git push origin --delete feat/x", True),
    ("git --no-pager push --force origin feat/x", True),
    ("git -C . push origin main", True),
    ("git -c core.x=y push origin master", True),
    ("git push origin feat/main-fix", False),
    ("gh pr merge 7 --admin --merge", True),
    ("gh pr merge 7 --merge", False),
    ("docker compose run --rm -v C:/Users/admin/.ssh:/x spark-test ls", True),
    ("docker run --mount type=bind,src=/,dst=/h alpine", True),
    ("docker compose --profile tools run --rm spark-test pytest tests/spark -v", False),
]

AUTORUN_SELF_TEST: list[tuple[str, bool]] = [
    ("claude -p 'do more'", True),
    ("git status; claude --version", True),
    ("Set-Content .claude/settings.json '{}'", True),
    ("python scripts/autonomous/run_queue.py --dry-run", True),
    ("cat .claude\\hooks\\guard_commands.py", True),
    ("python .claude/hooks/guard_commands.py --self-test", False),
    ("pytest tests/unit -q", False),
    ("git commit -m 'docs: update claude notes'", False),
    ("git push -u origin feat/w2-d7-spark", False),
    ("git push", True),
    ("git push origin HEAD", True),
    ("cat .claude//settings.json", True),
    ("cat .claude/./settings.json", True),
    ("Set-Content .github/workflows/ci.yml x", True),
    ("Get-Content CLAUDE.md", True),
    ("Add-Content .claude/skills/autonomous-run/SKILL.md x", True),
]


PATH_CASES: list[tuple[str, dict[str, object], bool, bool]] = [
    ("Read", {"file_path": "C:/repo/src/x.py"}, False, False),
    ("Read", {"file_path": "src/x.py"}, False, False),
    ("Read", {"file_path": "/c/repo/docs/PROGRESS.md"}, False, False),
    ("Read", {"file_path": "C:/Users/u/Documents/private.txt"}, False, True),
    ("Read", {"file_path": "../other-project/x.py"}, False, True),
    ("Read", {"file_path": "C:/repo/../outside.txt"}, False, True),
    ("Read", {"file_path": "~/.claude/CLAUDE.md"}, False, False),
    ("Read", {"file_path": "~/.ssh/id_rsa"}, False, True),
    ("Read", {"file_path": "C:/Users/u/AppData/Local/Temp/claude/s/out.txt"}, False, False),
    ("Grep", {"pattern": "x"}, False, False),
    ("Grep", {"pattern": "x", "path": "D:/"}, False, True),
    ("Glob", {"pattern": "*", "path": "C:/Windows"}, False, True),
    ("Edit", {"file_path": "C:/repo/.claude/settings.json"}, False, False),
    ("Edit", {"file_path": "C:/repo/.claude/settings.json"}, True, True),
    ("Write", {"file_path": "C:/repo/.claude/hooks/guard_commands.py"}, True, True),
    ("Write", {"file_path": "C:/repo/scripts/autonomous/limits.py"}, True, True),
    ("Edit", {"file_path": "C:/repo/.claude/agents/run-supervisor.md"}, True, True),
    ("Edit", {"file_path": "C:/repo/.claude/agents/implementer.md"}, True, True),
    ("Edit", {"file_path": "C:/repo/docs/TASKS.md"}, True, False),
    ("Read", {"file_path": "C:/repo/.claude/settings.json"}, True, False),
    # Review regressions (2026-11-06): case folding, wider guardrail set, user settings.
    ("Write", {"file_path": "C:/repo/.CLAUDE/Settings.json"}, True, True),
    ("Write", {"file_path": "C:/repo/.claude/Hooks/guard_commands.py"}, True, True),
    ("Write", {"file_path": "C:/repo/SCRIPTS/AUTONOMOUS/run_queue.py"}, True, True),
    ("Write", {"file_path": "C:/repo/.github/workflows/ci.yml"}, True, True),
    ("Edit", {"file_path": "C:/repo/.claude/skills/autonomous-run/SKILL.md"}, True, True),
    ("Edit", {"file_path": "C:/repo/CLAUDE.md"}, True, True),
    ("Write", {"file_path": "C:/repo/.claude/runs/run_start.json"}, True, True),
    ("Write", {"file_path": "C:/repo/.claude/learning/labs/W2_LAB.md"}, True, False),
    ("Write", {"file_path": "~/.claude/settings.json"}, True, True),
    ("Write", {"file_path": "~/.claude/projects/p/memory/x.md"}, True, False),
    ("Write", {"file_path": "~/.claude/settings.json"}, False, False),
    ("Read", {"file_path": "C:/repo/.env.staging"}, False, True),
    ("Read", {"file_path": "C:/repo/.env.example"}, False, False),
    ("Read", {"file_path": "C:/repo/infra/terraform/terraform.tfstate"}, False, True),
]


def _path_self_test() -> list[str]:
    project = Path("C:/repo")
    home = Path("C:/Users/u")
    temp = Path("C:/Users/u/AppData/Local/Temp/claude")
    failures = []
    for tool, tool_input, autorun, expect_block in PATH_CASES:
        reason = evaluate_path(
            tool, tool_input, project_dir=project, home=home, temp_root=temp, autorun=autorun
        )
        if (reason is not None) != expect_block:
            failures.append(f"{tool} {tool_input} autorun={autorun}")
    return failures


def self_test() -> int:
    failures = [
        f"{cmd!r} expected {'block' if exp else 'allow'}"
        for cmd, exp in SELF_TEST
        if (evaluate(cmd) is not None) != exp
    ]
    failures += [
        f"[autorun] {cmd!r} expected {'block' if exp else 'allow'}"
        for cmd, exp in AUTORUN_SELF_TEST
        if (evaluate(cmd, autorun=True) is not None) != exp
    ]
    failures += [f"[path] {f}" for f in _path_self_test()]
    total = len(SELF_TEST) + len(AUTORUN_SELF_TEST) + len(PATH_CASES)
    for failure in failures:
        sys.stderr.write(f"FAIL: {failure}\n")
    sys.stdout.write(f"guard self-test: {total - len(failures)}/{total} passed\n")
    return 1 if failures else 0


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    autorun = os.environ.get("LAKEHOUSE_AUTORUN") == "1"
    try:
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8-sig"))
        if not isinstance(payload, dict):
            raise ValueError("payload is not an object")
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        if autorun:  # unattended: fail closed
            sys.stderr.write("Blocked by .claude/hooks/guard_commands.py: unreadable hook input\n")
            return 2
        return 0  # interactive: do not break the session; permission rules still apply
    tool = str(payload.get("tool_name", ""))
    tool_input = payload.get("tool_input") or {}
    reason: str | None = None
    if tool in FILE_TOOLS and isinstance(tool_input, dict):
        project_dir = Path(os.environ.get("CLAUDE_PROJECT_DIR") or payload.get("cwd") or Path.cwd())
        reason = evaluate_path(
            tool,
            tool_input,
            project_dir=Path(os.path.normpath(str(project_dir))),
            home=Path.home(),
            temp_root=_default_temp_root(),
            autorun=autorun,
        )
    elif isinstance(tool_input, dict):
        reason = evaluate(str(tool_input.get("command", "")), autorun=autorun)
    if reason:
        sys.stderr.write(f"Blocked by .claude/hooks/guard_commands.py: {reason}\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
