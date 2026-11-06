<#
.SYNOPSIS
  Launcher for the unattended queue runner (scripts/autonomous/run_queue.py).

.DESCRIPTION
  Uses the repo venv (.venv\Scripts\python.exe) and forwards every argument unchanged.
  Create .claude\runs\STOP to halt the run at the next checkpoint.
  A halt (exit 7: supervisor, guardrail change, queue violation) leaves a STOP file behind; after
  reviewing it, the owner must delete it (Remove-Item .claude\runs\STOP) before restarting.

.EXAMPLE
  .\scripts\autonomous\run_queue.ps1 --dry-run
  .\scripts\autonomous\run_queue.ps1 --max-iterations 5 --supervise-every 2

  Exit codes (same as the Python script):
    0 complete / paused (max iterations) / dry-run    2 configuration error
    3 STOP file present                               4 waiting for owner approval
    5 usage limit resets beyond 24 h                  6 too many consecutive failures
    7 run-supervisor halted the run                 130 interrupted
#>
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$script = Join-Path $PSScriptRoot 'run_queue.py'

if (-not (Test-Path -LiteralPath $python)) {
    [Console]::Error.WriteLine("Python venv not found: $python (create it first, see CLAUDE.md Commands)")
    exit 2
}

& $python $script @args
exit $LASTEXITCODE
