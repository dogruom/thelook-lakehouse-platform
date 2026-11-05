"""PreToolUse guard for Bash and PowerShell commands.

Blocks shell commands that would print secrets or destroy data and that prefix-based
permission rules in .claude/settings.json cannot catch reliably (option reordering,
`git -c ...`, piping, PowerShell aliases).

Contract (Claude Code hooks): JSON on stdin, exit 0 = allow, exit 2 = block with the
reason on stderr. Run `python .claude/hooks/guard_commands.py --self-test` to verify.
"""

from __future__ import annotations

import json
import re
import sys


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

BLOCK_RULES: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"(?i)(^|[;&|(]\s*|\bexec\s+\S+\s+)(printenv|env)\s*($|[|;&>)])"),
        "Dumping environment variables can print secrets. Check a single variable "
        "with `[ -n \"$VAR\" ] && echo set` instead.",
    ),
    (
        re.compile(r"(?i)\b(Get-ChildItem|gci|dir|ls)\s+env:"),
        "Listing env: can print secrets. Test a single variable instead.",
    ),
    (
        re.compile(r"(?i)\bdocker\s+compose\b(?=.*\bconfig\b)(?!.*--(quiet|services|volumes|profiles)\b)"),
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
        re.compile(r"(?i)\bgit\b(\s+-c\s+\S+)*\s+(reset\s+--hard|clean\b|filter-branch|filter-repo)"),
        "Destructive git operation (reset --hard / clean / history rewrite) requires the owner.",
    ),
    (
        re.compile(r"(?i)\bgit\b(\s+-c\s+\S+)*\s+push\b.*(\s--force(-with-lease)?\b|\s-f\b|\s\+\S)"),
        "Force push is not allowed without explicit owner approval.",
    ),
    (
        re.compile(r"(?i)\bdocker\s+compose\b.*\bdown\b.*(\s-v\b|--volumes|--rmi)"),
        "`docker compose down -v/--rmi` deletes volumes or images. Use `docker compose stop`.",
    ),
    (
        re.compile(r"(?i)\bdocker\s+((system|volume|image|container|builder|network)\s+prune|volume\s+rm|rmi|rm|compose\s+rm)\b"),
        "Deleting Docker containers, images or volumes requires explicit owner approval.",
    ),
    (
        re.compile(r"(?i)\b(rm|Remove-Item)\b.*\s(-[a-z]*r[a-z]*|-Recurse)\b.*(\s|^)(\.git|data|docs|src|/|~|\*)(/|\\)?(\s|$)"),
        "Recursive deletion of a repository, data or home path is blocked.",
    ),
]


def evaluate(command: str) -> str | None:
    """Return a block reason, or None when the command is allowed."""
    for pattern, reason in BLOCK_RULES:
        if pattern.search(command):
            return reason
    segments = re.split(r"\|\||&&|[;|\n]", command)
    if any(SECRET_PATH.search(seg) and not SAFE_SECRET_MENTION.search(seg) for seg in segments):
        return (
            "The command references a secret file (.env, *.tfstate, *.tfvars, keys). "
            "Never read or modify it from the agent; document settings in .env.example."
        )
    return None


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
    ("git -c core.x=y reset --hard HEAD~1", True),
    ("git push --force origin main", True),
    ("git push -u origin feat/x", False),
    ("docker compose down -v", True),
    ("docker compose stop", False),
    ("docker volume rm crm_kafka_data", True),
    ("docker rm crm_kafka", True),
    ("docker compose up -d", False),
    ("rm -rf data", True),
    ("rm -rf htmlcov", False),
    ("cat infra/terraform/terraform.tfstate", True),
    ("terraform plan -var-file=dev.tfvars", True),
    ("aws configure export-credentials --profile dev", True),
    ("pytest tests/unit -q", False),
    ("ruff check src tests", False),
]


def self_test() -> int:
    failures = [(cmd, exp) for cmd, exp in SELF_TEST if (evaluate(cmd) is not None) != exp]
    for cmd, exp in failures:
        sys.stderr.write(f"FAIL: {cmd!r} expected {'block' if exp else 'allow'}\n")
    sys.stdout.write(f"guard self-test: {len(SELF_TEST) - len(failures)}/{len(SELF_TEST)} passed\n")
    return 1 if failures else 0


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0  # malformed input: do not break the session; permission rules still apply
    command = str((payload.get("tool_input") or {}).get("command", ""))
    reason = evaluate(command)
    if reason:
        sys.stderr.write(f"Blocked by .claude/hooks/guard_commands.py: {reason}\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
