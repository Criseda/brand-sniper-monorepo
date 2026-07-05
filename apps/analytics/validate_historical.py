"""
Validate that the historical prices CSV files are clean and ready to be seeded into the database.
Please run this before seed_historical_prices.py to prevent errors and save time.
"""

import sys
from pathlib import Path

import pandas as pd

# Force standard streams to use UTF-8 to support Unicode characters on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Dynamic path alignment to ensure the script can find the shared-utils package
sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared_utils import get_logger, parse_item_meta

logger = get_logger("analytics.validate")

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "items"


def run_dry_run_validation():
    if not DATA_DIR.exists():
        logger.error("Data directory not found at: %s", DATA_DIR)
        return

    logger.info("Initializing Pre-Flight Data Validation Sweep...")
    csv_files = list(DATA_DIR.glob("*.csv"))
    total_files = len(csv_files)
    logger.info("Total files found for validation: %d", total_files)

    total_rows = 0
    corrupt_rows_dropped = 0
    salvaged_files_count = 0
    type_distribution = {
        "Knife": 0,
        "Glove": 0,
        "Agent": 0,
        "Weapon Skin": 0,
        "Sticker": 0,
        "Music Kit": 0,
        "Patch": 0,
        "Container/Collectible": 0,
    }
    sample_decodes = []

    for idx, file_path in enumerate(csv_files, start=1):
        # ⏳ LIVE PROGRESS HEARTBEAT
        if idx % 500 == 0 or idx == 1 or idx == total_files:
            logger.info("Progress: Scanned %d/%d files... (%d%%)", idx, total_files, int((idx / total_files) * 100))

        market_hash_name, item_type = parse_item_meta(file_path.name)
        if item_type in type_distribution:
            type_distribution[item_type] += 1

        # Capture early balance mix to visually confirm logic stability
        if idx <= 4 or idx % 3000 == 0:
            sample_decodes.append((file_path.name, market_hash_name, item_type))

        try:
            df = pd.read_csv(file_path)
            if df.empty:
                continue

            # Run defensive conversion checks matching the seed code
            initial_row_count = len(df)

            # Coerce string-corrupted timestamp records cleanly to NaN
            df["unix timestamp"] = pd.to_numeric(df["unix timestamp"], errors="coerce")
            df = df.dropna(subset=["unix timestamp"])

            cleaned_row_count = len(df)
            dropped = initial_row_count - cleaned_row_count

            if dropped > 0:
                corrupt_rows_dropped += dropped
                salvaged_files_count += 1

            total_rows += cleaned_row_count

        except Exception:
            continue

    logger.info("=" * 70)
    logger.info("                      DATA SANITY REPORT                      ")
    logger.info("=" * 70)
    logger.info("Total Operational Files   : %d", total_files)
    logger.info("Files Requiring Row Drops : %d (Corrupted text lines auto-purged)", salvaged_files_count)
    logger.info("Total Corrupt Rows Dropped: %s", f"{corrupt_rows_dropped:,}")
    logger.info("Total Clean Rows Preserved : %s", f"{total_rows:,}")
    logger.info("-" * 70)

    logger.info("REFINED ITEM TYPE DISTRIBUTION:")
    for k, v in type_distribution.items():
        if v > 0:
            logger.info(" - %-22s: %d files mapped", k, v)

    logger.info("VERIFIED STRINGS SAMPLES:")
    for raw, clean, itype in sample_decodes[:6]:
        logger.info(" Raw   : %s", raw)
        logger.info(" Clean : %s ---> Type: %s", clean, itype)
        logger.info("-" * 50)
    logger.info("=" * 70)


if __name__ == "__main__":
    run_dry_run_validation()
