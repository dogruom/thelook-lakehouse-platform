"""AWS Glue entry point: run one lakehouse job from the uploaded wheel (D-103).

Glue runs this file as the job script (``s3://<bucket>/artifacts/current/glue_entry.py``) and
passes the job's default and per-run arguments on ``sys.argv``::

    --lakehouse_job <job> --lakehouse_bucket <bucket> [--lakehouse_args '["--table","raw_orders"]']

Every other argument (``--JOB_NAME``, ``--conf``, ``--TempDir``, ...) is Glue's own and ignored
here. The wheel (``--additional-python-modules``) provides the ``lakehouse`` package. The script
sets ``LAKEHOUSE_ENV=aws``, ``LAKEHOUSE_CATALOG=glue`` and ``LAKEHOUSE_BUCKET``, runs
``lakehouse.common.jobs.main([job, *args])`` and exits with its code, so a failed job fails the
Glue run. ``awsglue`` is deliberately not imported: the job framework creates the Spark session.
Only the job name and the bucket are logged.
"""

from __future__ import annotations

import json
import os
import re
import sys
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Sequence

JOB_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,62}\Z")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_FLAGS = ("--lakehouse_job", "--lakehouse_bucket", "--lakehouse_args")
_EXIT_USAGE = 2


class GlueArgError(ValueError):
    """The Glue argument vector is unusable."""


def _collect(argv: Sequence[str]) -> dict[str, str]:
    """Values of the ``--lakehouse_*`` flags (``--flag value`` or ``--flag=value``)."""
    found: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        flag, sep, inline = token.partition("=")
        index += 1
        if flag not in _FLAGS:
            continue  # a Glue-reserved or foreign argument
        if sep:
            value = inline
        elif index < len(argv):
            value = argv[index]
            index += 1
        else:
            raise GlueArgError(f"{flag} needs a value")
        if flag in found:
            raise GlueArgError(f"{flag} given twice")
        found[flag] = value
    return found


def parse_glue_argv(argv: Sequence[str]) -> tuple[str, str, list[str]]:
    """``(job, bucket, job_args)`` from the Glue argument vector (without the program name)."""
    found = _collect(argv)
    job = found.get("--lakehouse_job")
    if job is None:
        raise GlueArgError("--lakehouse_job is required")
    if not JOB_PATTERN.match(job):
        raise GlueArgError("--lakehouse_job is not a valid job name")
    bucket = found.get("--lakehouse_bucket")
    if bucket is None:
        raise GlueArgError("--lakehouse_bucket is required")
    if not BUCKET_PATTERN.match(bucket):
        raise GlueArgError("--lakehouse_bucket is not a valid bucket name")
    raw_args = found.get("--lakehouse_args", "[]")
    try:
        args = json.loads(raw_args)
    except json.JSONDecodeError:
        raise GlueArgError("--lakehouse_args must be a JSON list of strings") from None
    if not isinstance(args, list) or not all(isinstance(item, str) for item in args):
        raise GlueArgError("--lakehouse_args must be a JSON list of strings")
    return job, bucket, args


def main(argv: Sequence[str] | None = None) -> int:
    """Run the job; returns the process exit code (0 ok, 1 job failed, 2 bad arguments)."""
    try:
        job, bucket, args = parse_glue_argv(sys.argv[1:] if argv is None else argv)
    except GlueArgError as exc:
        print(f"glue_entry: {exc}", file=sys.stderr)
        return _EXIT_USAGE
    os.environ["LAKEHOUSE_ENV"] = "aws"
    os.environ["LAKEHOUSE_CATALOG"] = "glue"
    os.environ["LAKEHOUSE_BUCKET"] = bucket
    print(f"glue_entry: job={job} bucket={bucket} extra_args={len(args)}", flush=True)
    from lakehouse.common.jobs import main as run_job  # needs the installed wheel

    return int(run_job([job, *args]))


if __name__ == "__main__":
    sys.exit(main())
