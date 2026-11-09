"""spark-submit primary resource: runs a Spark job by name; the code comes from --py-files."""

from __future__ import annotations

import sys

from lakehouse.common.jobs import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
