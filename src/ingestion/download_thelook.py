"""
TheLook eCommerce Dataset Downloader
=====================================
Sprint 2.1 — src/ingestion/download_thelook.py

Download Strategy (in priority order):
  1. Kaggle API       — full dataset (~500 MB), requires KAGGLE_USERNAME + KAGGLE_KEY
  2. Synthetic Data   — schema-compliant generated data (~few MB), zero external dependency

Design Principles (disk-aware):
  - Streaming downloads: never load entire file into RAM
  - Chunk-based processing: configurable chunk size
  - Automatic cleanup: temp files removed after conversion
  - Progress reporting: real-time bytes/MB display
  - Idempotent: skip already-downloaded tables unless --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import logging
import os
import random
import shutil
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLES = [
    "users",
    "orders",
    "order_items",
    "products",
    "inventory_items",
    "distribution_centers",
]

# Verified working dataset on Kaggle (contains all 6 batch tables + events)
KAGGLE_DATASET_SLUG = "daichiuchigashima/thelook-ecommerce"

# Kaggle v2 uses KGAT_ prefixed tokens stored in ~/.kaggle/access_token
# or classic username+key in ~/.kaggle/kaggle.json
# Set env vars: KAGGLE_USERNAME + KAGGLE_KEY  OR  KAGGLE_TOKEN

# Default output directory (inside Airflow container or local)
DEFAULT_DATA_DIR = os.getenv("THELOOK_DATA_DIR", "/opt/airflow/data/raw")

# Synthetic row counts per table (disk-friendly defaults)
SYNTHETIC_ROW_COUNTS: dict[str, int] = {
    "distribution_centers": 10,
    "users": 2_000,
    "products": 500,
    "inventory_items": 3_000,
    "orders": 1_500,
    "order_items": 4_500,
}

# Download chunk size: 8 MB — good balance of RAM vs IO
CHUNK_SIZE_BYTES = 8 * 1024 * 1024

# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------


def compute_sha256(path: Path, chunk_size: int = CHUNK_SIZE_BYTES) -> str:
    """Compute SHA-256 of a file without loading it entirely into memory."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def write_checksum_file(csv_path: Path) -> Path:
    """Write sha256 checksum file alongside the CSV."""
    checksum = compute_sha256(csv_path)
    checksum_path = csv_path.with_suffix(".csv.sha256")
    checksum_path.write_text(f"{checksum}  {csv_path.name}\n", encoding="utf-8")
    return checksum_path


def verify_checksum(csv_path: Path) -> bool:
    """Return True if the checksum file matches the CSV. False if missing/mismatched."""
    checksum_path = csv_path.with_suffix(".csv.sha256")
    if not checksum_path.exists():
        return False
    stored = checksum_path.read_text(encoding="utf-8").split()[0]
    actual = compute_sha256(csv_path)
    return stored == actual


# ---------------------------------------------------------------------------
# Kaggle source
# ---------------------------------------------------------------------------


def _is_kaggle_available() -> bool:
    """Return True if the kaggle package and credentials are available."""
    try:
        import kaggle  # noqa: F401  (lazy import — not required)

        # kaggle package sets up auth from KAGGLE_USERNAME + KAGGLE_KEY env vars
        # or from ~/.kaggle/kaggle.json
        username = os.getenv("KAGGLE_USERNAME", "")
        key = os.getenv("KAGGLE_KEY", "")
        kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
        return bool((username and key) or kaggle_json.exists())
    except ImportError:
        return False


def download_via_kaggle(output_dir: Path, force: bool = False) -> list[Path]:
    """
    Download TheLook dataset from Kaggle.

    Uses streaming ZIP extraction to minimize peak disk usage:
    - Download → temp dir
    - Extract CSV per table
    - Move to output_dir
    - Delete zip

    Returns list of downloaded CSV paths.
    """
    from kaggle import KaggleApi

    api = KaggleApi()
    api.authenticate()

    output_dir.mkdir(parents=True, exist_ok=True)

    # Check which tables already exist (idempotency)
    existing = {t for t in TABLES if (output_dir / f"{t}.csv").exists() and not force}
    if existing and not force:
        log.info(
            "kaggle.skipping_existing",
            tables=sorted(existing),
            tip="Use --force to re-download",
        )

    tables_to_download = [t for t in TABLES if t not in existing]
    if not tables_to_download:
        log.info("kaggle.all_tables_present", tables=TABLES)
        return [output_dir / f"{t}.csv" for t in TABLES]

    log.info(
        "kaggle.download_start",
        dataset=KAGGLE_DATASET_SLUG,
        tables=tables_to_download,
    )

    with tempfile.TemporaryDirectory(prefix="thelook_kaggle_") as tmp:
        tmp_path = Path(tmp)
        start = time.monotonic()

        # Download full zip into temp dir
        api.dataset_download_files(
            KAGGLE_DATASET_SLUG,
            path=str(tmp_path),
            unzip=False,
            quiet=False,
        )

        zip_files = list(tmp_path.glob("*.zip"))
        if not zip_files:
            raise FileNotFoundError("Kaggle download produced no zip file.")

        zip_path = zip_files[0]
        zip_size_mb = zip_path.stat().st_size / (1024 ** 2)
        log.info("kaggle.zip_downloaded", size_mb=round(zip_size_mb, 1), path=str(zip_path))

        # Extract only the tables we need
        # Files in zip use prefix: thelook_ecommerce.<table>.csv
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            for table in tables_to_download:
                # Match both prefixed (thelook_ecommerce.users.csv) and bare (users.csv)
                candidates = [
                    m for m in members
                    if m == f"{table}.csv"
                    or m == f"thelook_ecommerce.{table}.csv"
                    or m.endswith(f"/{table}.csv")
                ]
                if not candidates:
                    log.warning("kaggle.table_not_found_in_zip", table=table, members=members)
                    continue

                member = candidates[0]
                dest = output_dir / f"{table}.csv"
                log.info("kaggle.extracting", table=table, member=member, dest=str(dest))

                with zf.open(member) as src, dest.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=CHUNK_SIZE_BYTES)

                size_mb = dest.stat().st_size / (1024 ** 2)
                checksum_path = write_checksum_file(dest)
                log.info(
                    "kaggle.table_ready",
                    table=table,
                    size_mb=round(size_mb, 1),
                    checksum=checksum_path.name,
                )

        elapsed = time.monotonic() - start
        log.info("kaggle.download_complete", elapsed_s=round(elapsed, 1))

    return [output_dir / f"{t}.csv" for t in TABLES if (output_dir / f"{t}.csv").exists()]


# ---------------------------------------------------------------------------
# Synthetic data generator
# ---------------------------------------------------------------------------

_RNG = random.Random(42)  # deterministic seed → reproducible runs


def _rand_date(start: datetime, end: datetime) -> str:
    delta = int((end - start).total_seconds())
    offset = _RNG.randint(0, delta)
    return (start + timedelta(seconds=offset)).strftime("%Y-%m-%d %H:%M:%S UTC")


def _rand_email(first: str, last: str, user_id: int) -> str:
    domains = ["example.com", "mail.test", "demo.org", "thelook.test"]
    return f"{first.lower()}.{last.lower()}{user_id}@{_RNG.choice(domains)}"


def generate_distribution_centers(n: int) -> list[dict[str, Any]]:
    centers = [
        {"id": 1, "name": "Memphis TN", "latitude": 35.1495, "longitude": -90.0490},
        {"id": 2, "name": "Chicago IL", "latitude": 41.8781, "longitude": -87.6298},
        {"id": 3, "name": "Houston TX", "latitude": 29.7604, "longitude": -95.3698},
        {"id": 4, "name": "Los Angeles CA", "latitude": 34.0522, "longitude": -118.2437},
        {"id": 5, "name": "New York NY", "latitude": 40.7128, "longitude": -74.0060},
        {"id": 6, "name": "Philadelphia PA", "latitude": 39.9526, "longitude": -75.1652},
        {"id": 7, "name": "New Orleans LA", "latitude": 29.9511, "longitude": -90.0715},
        {"id": 8, "name": "Charleston SC", "latitude": 32.7765, "longitude": -79.9311},
        {"id": 9, "name": "Savannah GA", "latitude": 32.0835, "longitude": -81.0998},
        {"id": 10, "name": "Mobile AL", "latitude": 30.6954, "longitude": -88.0399},
    ]
    return centers[:n]


def generate_users(n: int) -> list[dict[str, Any]]:
    first_names = ["Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Hank",
                   "Iris", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
                   "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
                   "Yara", "Zoe"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                  "Davis", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White",
                  "Harris", "Martin", "Thompson", "Moore", "Young", "Allen"]
    genders = ["M", "F"]
    countries = ["USA", "UK", "Canada", "Australia", "Germany", "France", "Brazil", "Japan"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
              "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    traffic_sources = ["Search", "Organic", "Facebook", "Email", "Display"]

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=3 * 365)
    rows = []
    for i in range(1, n + 1):
        first = _RNG.choice(first_names)
        last = _RNG.choice(last_names)
        city_idx = _RNG.randint(0, len(cities) - 1)
        created = _rand_date(start, now)
        rows.append({
            "id": i,
            "first_name": first,
            "last_name": last,
            "email": _rand_email(first, last, i),
            "age": _RNG.randint(18, 70),
            "gender": _RNG.choice(genders),
            "state": states[city_idx],
            "street_address": f"{_RNG.randint(1, 9999)} Main St",
            "postal_code": f"{_RNG.randint(10000, 99999)}",
            "city": cities[city_idx],
            "country": countries[0],  # mostly USA
            "latitude": round(_RNG.uniform(25.0, 49.0), 4),
            "longitude": round(_RNG.uniform(-125.0, -66.0), 4),
            "traffic_source": _RNG.choice(traffic_sources),
            "created_at": created,
        })
    return rows


def generate_products(n: int) -> list[dict[str, Any]]:
    categories = [
        ("Intimates", "Socks & Hosiery"),
        ("Swim", "Swimwear"),
        ("Pants", "Trousers"),
        ("Tops & Tees", "Knit Tops"),
        ("Jeans", "Straight Leg"),
        ("Outerwear & Coats", "Overcoats"),
        ("Suits", "Blazers"),
        ("Active", "Sport Jackets"),
        ("Accessories", "Belts"),
        ("Socks & Hosiery", "Socks"),
    ]
    brands = ["Allegra K", "Calvin Klein", "Carhartt", "Hanes", "Dockers",
              "Quiksilver", "Diesel", "Levi's", "Columbia", "Nike", "Adidas",
              "Under Armour", "Tommy Hilfiger", "Ralph Lauren", "Gap"]
    depts = ["Mens", "Womens", "Kids"]
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=365)
    rows = []
    for i in range(1, n + 1):
        cat, sub = _RNG.choice(categories)
        retail = round(_RNG.uniform(5.0, 250.0), 2)
        cost = round(retail * _RNG.uniform(0.3, 0.7), 2)
        rows.append({
            "id": i,
            "cost": cost,
            "category": cat,
            "name": f"{_RNG.choice(brands)} {sub} #{i}",
            "brand": _RNG.choice(brands),
            "retail_price": retail,
            "department": _RNG.choice(depts),
            "sku": f"SKU-{i:06d}",
            "distribution_center_id": _RNG.randint(1, 10),
            "created_at": _rand_date(start, now),
        })
    return rows


def generate_inventory_items(n: int, n_products: int, n_centers: int) -> list[dict[str, Any]]:
    statuses = ["in stock", "sold", "returned"]
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=2 * 365)
    rows = []
    for i in range(1, n + 1):
        created = _rand_date(start, now)
        rows.append({
            "id": i,
            "product_id": _RNG.randint(1, n_products),
            "created_at": created,
            "sold_at": _rand_date(
                datetime.fromisoformat(created.replace(" UTC", "+00:00")),
                now,
            ) if _RNG.random() > 0.4 else "",
            "cost": round(_RNG.uniform(5.0, 150.0), 2),
            "product_category": "Tops & Tees",
            "product_name": f"Product #{_RNG.randint(1, n_products)}",
            "product_brand": "Brand X",
            "product_retail_price": round(_RNG.uniform(10.0, 200.0), 2),
            "product_department": _RNG.choice(["Mens", "Womens", "Kids"]),
            "product_sku": f"SKU-{_RNG.randint(1, n_products):06d}",
            "product_distribution_center_id": _RNG.randint(1, n_centers),
        })
    return rows


def generate_orders(n: int, n_users: int) -> list[dict[str, Any]]:
    statuses = ["Complete", "Returned", "Cancelled", "Processing", "Shipped"]
    genders = ["M", "F"]
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=2 * 365)
    rows = []
    for i in range(1, n + 1):
        created = _rand_date(start, now)
        created_dt = datetime.fromisoformat(created.replace(" UTC", "+00:00"))
        shipped = _rand_date(
            created_dt,
            min(created_dt + timedelta(days=7), now),
        ) if _RNG.random() > 0.2 else ""
        delivered = _rand_date(
            datetime.fromisoformat(shipped.replace(" UTC", "+00:00")),
            min(datetime.fromisoformat(shipped.replace(" UTC", "+00:00")) + timedelta(days=14), now),
        ) if shipped and _RNG.random() > 0.1 else ""
        rows.append({
            "order_id": i,
            "user_id": _RNG.randint(1, n_users),
            "status": _RNG.choice(statuses),
            "gender": _RNG.choice(genders),
            "created_at": created,
            "returned_at": _rand_date(
                datetime.fromisoformat(created.replace(" UTC", "+00:00")),
                now,
            ) if _RNG.random() > 0.85 else "",
            "shipped_at": shipped,
            "delivered_at": delivered,
            "num_of_item": _RNG.randint(1, 5),
        })
    return rows


def generate_order_items(n: int, orders: list[dict], n_products: int, n_inventory: int) -> list[dict[str, Any]]:
    statuses = ["Complete", "Returned", "Cancelled", "Processing", "Shipped"]
    rows = []
    item_id = 1
    order_pool = [o for o in orders for _ in range(o.get("num_of_item", 1))]
    _RNG.shuffle(order_pool)
    for idx in range(min(n, len(order_pool))):
        order = order_pool[idx]
        sale_price = round(_RNG.uniform(8.0, 180.0), 2)
        rows.append({
            "id": item_id,
            "order_id": order["order_id"],
            "user_id": order["user_id"],
            "product_id": _RNG.randint(1, n_products),
            "inventory_item_id": _RNG.randint(1, n_inventory),
            "status": order["status"],
            "created_at": order["created_at"],
            "shipped_at": order.get("shipped_at", ""),
            "delivered_at": order.get("delivered_at", ""),
            "returned_at": order.get("returned_at", ""),
            "sale_price": sale_price,
        })
        item_id += 1
    return rows


def generate_synthetic_data(output_dir: Path, force: bool = False) -> list[Path]:
    """
    Generate schema-compliant synthetic TheLook data and write to CSV files.

    Rows are deterministically generated (seed=42) — same seed → same data.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    existing = {t for t in TABLES if (output_dir / f"{t}.csv").exists() and not force}
    if existing:
        log.info("synthetic.skipping_existing", tables=sorted(existing))

    tables_to_generate = [t for t in TABLES if t not in existing]
    if not tables_to_generate:
        log.info("synthetic.all_tables_present", tables=TABLES)
        return [output_dir / f"{t}.csv" for t in TABLES]

    log.info("synthetic.generation_start", tables=tables_to_generate, seed=42)

    # Generate in dependency order
    n = SYNTHETIC_ROW_COUNTS
    centers = generate_distribution_centers(n["distribution_centers"])
    users = generate_users(n["users"])
    products = generate_products(n["products"])
    inventory = generate_inventory_items(n["inventory_items"], n["products"], n["distribution_centers"])
    orders = generate_orders(n["orders"], n["users"])
    order_items = generate_order_items(n["order_items"], orders, n["products"], n["inventory_items"])

    table_data: dict[str, list[dict]] = {
        "distribution_centers": centers,
        "users": users,
        "products": products,
        "inventory_items": inventory,
        "orders": orders,
        "order_items": order_items,
    }

    written: list[Path] = []
    for table in TABLES:
        if table not in tables_to_generate:
            written.append(output_dir / f"{table}.csv")
            continue

        rows = table_data[table]
        dest = output_dir / f"{table}.csv"
        if not rows:
            log.warning("synthetic.empty_table", table=table)
            continue

        fieldnames = list(rows[0].keys())
        with dest.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        size_kb = dest.stat().st_size / 1024
        checksum_path = write_checksum_file(dest)
        log.info(
            "synthetic.table_written",
            table=table,
            rows=len(rows),
            size_kb=round(size_kb, 1),
            checksum=checksum_path.name,
        )
        written.append(dest)

    log.info("synthetic.generation_complete", tables=TABLES)
    return written


# ---------------------------------------------------------------------------
# Metadata sidecar
# ---------------------------------------------------------------------------


def write_metadata(output_dir: Path, source: str, paths: list[Path]) -> Path:
    """Write a metadata JSON sidecar describing the download run."""
    import json

    meta = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "source": source,
        "tables": {},
    }
    for path in paths:
        if path.exists():
            meta["tables"][path.stem] = {
                "file": path.name,
                "size_bytes": path.stat().st_size,
                "size_mb": round(path.stat().st_size / (1024 ** 2), 3),
                "sha256": compute_sha256(path),
            }

    meta_path = output_dir / "download_metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("metadata.written", path=str(meta_path))
    return meta_path


# ---------------------------------------------------------------------------
# Disk space guard
# ---------------------------------------------------------------------------


def check_disk_space(path: Path, required_mb: float = 200.0) -> None:
    """
    Raise RuntimeError if free disk space is below required_mb.

    Default threshold: 200 MB — enough for CSV + headroom.
    For full Kaggle download: pass required_mb=800.
    """
    usage = shutil.disk_usage(str(path))
    free_mb = usage.free / (1024 ** 2)
    log.info(
        "disk.space_check",
        free_mb=round(free_mb, 0),
        required_mb=required_mb,
        path=str(path),
    )
    if free_mb < required_mb:
        raise RuntimeError(
            f"Insufficient disk space: {free_mb:.0f} MB free, "
            f"{required_mb:.0f} MB required at {path}. "
            "Free up space or reduce SYNTHETIC_ROW_COUNTS."
        )


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------


def run(
    output_dir: Path,
    force: bool = False,
    source_override: str | None = None,
) -> dict[str, Any]:
    """
    Download TheLook dataset with automatic source selection.

    Args:
        output_dir:      Directory to write CSV files.
        force:           Re-download even if files already exist.
        source_override: Force a specific source: "kaggle" | "synthetic"

    Returns:
        Dict with keys: source, paths, metadata_path
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Disk guard — check with appropriate threshold
    use_kaggle = (source_override == "kaggle") or (
        source_override is None and _is_kaggle_available()
    )
    required_mb = 1_000.0 if use_kaggle else 50.0
    check_disk_space(output_dir, required_mb=required_mb)

    source = "unknown"
    paths: list[Path] = []

    if source_override == "synthetic" or not use_kaggle:
        if source_override == "kaggle":
            raise EnvironmentError(
                "Kaggle source requested but kaggle package or credentials not found. "
                "Install: pip install kaggle  and set KAGGLE_USERNAME + KAGGLE_KEY env vars."
            )
        log.info(
            "downloader.using_synthetic",
            reason="Kaggle not available" if source_override is None else "forced by --source synthetic",
        )
        paths = generate_synthetic_data(output_dir, force=force)
        source = "synthetic"
    else:
        log.info("downloader.using_kaggle", dataset=KAGGLE_DATASET_SLUG)
        try:
            paths = download_via_kaggle(output_dir, force=force)
            source = "kaggle"
        except Exception as exc:
            log.warning(
                "downloader.kaggle_failed",
                error=str(exc),
                fallback="synthetic",
            )
            paths = generate_synthetic_data(output_dir, force=force)
            source = "synthetic_fallback"

    # Verify checksums
    bad = []
    for path in paths:
        if path.exists() and not verify_checksum(path):
            bad.append(path.name)
    if bad:
        log.warning("checksum.mismatch", files=bad)
    else:
        log.info("checksum.all_valid", count=len(paths))

    meta_path = write_metadata(output_dir, source, paths)

    return {
        "source": source,
        "paths": paths,
        "metadata_path": meta_path,
        "output_dir": str(output_dir),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="download_thelook",
        description="Download TheLook eCommerce dataset (Kaggle or synthetic fallback).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-select source (Kaggle if available, else synthetic):
  python download_thelook.py --output-dir /opt/airflow/data/raw

  # Force synthetic data (great for testing, minimal disk):
  python download_thelook.py --source synthetic

  # Force Kaggle (requires KAGGLE_USERNAME + KAGGLE_KEY env vars):
  python download_thelook.py --source kaggle

  # Re-download even if files exist:
  python download_thelook.py --force

Environment Variables:
  THELOOK_DATA_DIR    Default output directory (overridden by --output-dir)
  KAGGLE_USERNAME     Kaggle API username
  KAGGLE_KEY          Kaggle API key
        """,
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Output directory for CSV files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--source",
        choices=["kaggle", "synthetic"],
        default=None,
        help="Force a specific download source (default: auto-detect)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download/regenerate even if files already exist",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="Print the list of tables and exit",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.list_tables:
        print("TheLook tables:")
        for t in TABLES:
            n = SYNTHETIC_ROW_COUNTS.get(t, "?")
            print(f"  {t:<30} (synthetic rows: {n})")
        return

    result = run(
        output_dir=Path(args.output_dir),
        force=args.force,
        source_override=args.source,
    )

    print("\n" + "=" * 60)
    print(f"✅ Download complete!")
    print(f"   Source    : {result['source']}")
    print(f"   Output    : {result['output_dir']}")
    print(f"   Tables    : {len(result['paths'])}")
    print(f"   Metadata  : {result['metadata_path']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
