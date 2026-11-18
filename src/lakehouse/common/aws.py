"""AWS naming shared by the cloud entry points (landing, orchestration, ``scripts/cloud``).

The single source of truth for the data-bucket naming rule (D-102): a bucket this project writes
to on AWS is always called ``crm-lakehouse-<suffix>``, so a typo or a foreign bucket is refused
before any object is written.
"""

from __future__ import annotations

import re
from typing import Final


AWS_BUCKET_RE: Final = re.compile(r"crm-lakehouse-[a-z0-9-]+")


def is_data_bucket(value: object) -> bool:
    """True for a ``crm-lakehouse-*`` bucket name (a bare name, no URI, no path)."""
    return isinstance(value, str) and AWS_BUCKET_RE.fullmatch(value) is not None
