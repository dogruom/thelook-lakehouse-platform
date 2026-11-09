"""Container entrypoint: render hive-site.xml from the environment, then exec the command (D-060).

Why: Spark's built-in Hive 2.3 client reads the metastore JDBC settings (including the password)
from ``hive-site.xml`` on the classpath. The password must not live in the repository, the image,
``spark-defaults.conf`` or a command line, so it is rendered at container start from the
``HIVE_METASTORE_DB_*`` variables into a container-local file readable only by the ``spark`` user.
``HADOOP_CONF_DIR`` (set in docker-compose.yml) points Spark's launcher at that directory, so
``docker exec ... spark-submit`` processes see the same file.

Without ``HIVE_METASTORE_DB_PASSWORD`` nothing is rendered: the container still starts and jobs
fall back to whatever catalog ``LAKEHOUSE_CATALOG`` selects (``in_memory`` by default).
The script never prints a value; it logs key names only.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from xml.sax.saxutils import escape


def _property(name: str, value: str) -> str:
    return f"  <property><name>{name}</name><value>{escape(value)}</value></property>"


def render(env: dict[str, str]) -> str | None:
    """hive-site.xml text, or ``None`` when the metastore password is not configured."""
    password = env.get("HIVE_METASTORE_DB_PASSWORD", "")
    if not password:
        return None
    host = env.get("HIVE_METASTORE_DB_HOST", "postgres:5432")
    database = env.get("HIVE_METASTORE_DB_NAME", "hive_metastore")
    user = env.get("HIVE_METASTORE_DB_USER", "hive")
    props = [
        _property("javax.jdo.option.ConnectionURL", f"jdbc:postgresql://{host}/{database}"),
        _property("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver"),
        _property("javax.jdo.option.ConnectionUserName", user),
        _property("javax.jdo.option.ConnectionPassword", password),
        # No schematool in the Spark distribution: DataNucleus creates the metastore tables and
        # columns on first use (Hive 2.3 layout); version verification is off for that reason.
        # Foreign keys are NOT auto-created. Observed in W2.D11: with them, the first CREATE
        # TABLE self-deadlocks on PostgreSQL (a lazy ALTER TABLE ... ADD FOREIGN KEY on a second
        # connection waits for the still-open INSERT transaction on TBLS). autoCreateAll would
        # switch constraints back on, so the three flags are set one by one. Proper fix: the
        # official Hive schema script (ADR-0004, D-060).
        _property("datanucleus.schema.autoCreateAll", "false"),
        _property("datanucleus.schema.autoCreateTables", "true"),
        _property("datanucleus.schema.autoCreateColumns", "true"),
        _property("datanucleus.schema.autoCreateConstraints", "false"),
        _property("hive.metastore.schema.verification", "false"),
    ]
    return '<?xml version="1.0"?>\n<configuration>\n' + "\n".join(props) + "\n</configuration>\n"


def main(argv: list[str]) -> None:
    target_dir = Path(os.environ.get("HADOOP_CONF_DIR", "/tmp/hive-conf"))
    text = render(dict(os.environ))
    target = target_dir / "hive-site.xml"
    if text is None:
        target.unlink(missing_ok=True)
        print("render_hive_site: HIVE_METASTORE_DB_PASSWORD not set; no hive-site.xml", flush=True)
    else:
        target_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        target_dir.chmod(0o700)  # also tighten a directory left by an earlier start
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        target.chmod(0o600)
        print(f"render_hive_site: wrote {target} (mode 600)", flush=True)
    if len(argv) > 1:
        os.execvp(argv[1], argv[1:])


if __name__ == "__main__":
    main(sys.argv)
