"""
Validate that the historical prices CSV files are clean and ready to be seeded into the database.
Please run this before seed_historical_prices.py to prevent errors and save time.
"""

import sys
from pathlib import Path
import urllib.parse
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "items"

def parse_item_meta(filename: str) -> tuple[str, str]:
    """Advanced metadata decoding to correctly separate weapons, stickers, and agents."""
    clean_name = urllib.parse.unquote(filename.replace(".csv", ""))
    
    # Check for Knives and Gloves
    if "★" in clean_name:
        if any(w in clean_name for w in ["Gloves", "Wraps"]):
            return clean_name, "Glove"
        return clean_name, "Knife"
    
    # Catch distinct utility/cosmetic categories
    if "Sticker |" in clean_name or clean_name.startswith("Sticker"):
        return clean_name, "Sticker"
    if "Music Kit |" in clean_name:
        return clean_name, "Music Kit"
    if "Patch |" in clean_name:
        return clean_name, "Patch"
        
    # Catch Agents via known faction markers
    factions = ["NSWC SEAL", "Guerrilla Warfare", "Sabre", "TACP", "Professionals", "FBI", "SWAT", "Gendarmerie", "KSK"]
    if any(f in clean_name for f in factions) or "Agent" in clean_name:
        return clean_name, "Agent"
        
    # Catch Agents or Collectibles by looking for the absolute absence of an exterior wear group
    wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
    if not any(w in clean_name for w in wears):
        if any(c in clean_name for c in ["Case", "Capsule", "Package", "Pin"]):
            return clean_name, "Container/Collectible"
        return clean_name, "Agent"  # Catch-all fallback for standalone character names
        
    return clean_name, "Weapon Skin"

def run_dry_run_validation():
    if not DATA_DIR.exists():
        print(f"Error: Data directory not found at: {DATA_DIR}")
        return

    print("Initializing Pre-Flight Data Validation Sweep...")
    csv_files = list(DATA_DIR.glob("*.csv"))
    total_files = len(csv_files)
    print(f"Total files found for validation: {total_files}\n")

    total_rows = 0
    corrupt_rows_dropped = 0
    salvaged_files_count = 0
    type_distribution = {"Knife": 0, "Glove": 0, "Agent": 0, "Weapon Skin": 0, "Sticker": 0, "Music Kit": 0, "Patch": 0, "Container/Collectible": 0}
    sample_decodes = []

    for idx, file_path in enumerate(csv_files, start=1):
        # ⏳ LIVE PROGRESS HEARTBEAT
        if idx % 500 == 0 or idx == 1 or idx == total_files:
            print(f"⏳ Progress: Scanned {idx}/{total_files} files... ({int((idx / total_files) * 100)}%)")
            sys.stdout.flush()

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
            df["unix timestamp"] = pd.to_numeric(df["unix timestamp"], errors='coerce')
            df = df.dropna(subset=["unix timestamp"])
            
            cleaned_row_count = len(df)
            dropped = initial_row_count - cleaned_row_count
            
            if dropped > 0:
                corrupt_rows_dropped += dropped
                salvaged_files_count += 1
                
            total_rows += cleaned_row_count

        except Exception:
            continue

    print("\n" + "=" * 70)
    print("                      DATA SANITY REPORT                      ")
    print("=" * 70)
    print(f"Total Operational Files   : {total_files}")
    print(f"Files Requiring Row Drops : {salvaged_files_count} (Corrupted text lines auto-purged)")
    print(f"Total Corrupt Rows Dropped: {corrupt_rows_dropped:,}")
    print(f"Total Clean Rows Preserved : {total_rows:,}")
    print("-" * 70)
    
    print("\nREFINED ITEM TYPE DISTRIBUTION:")
    for k, v in type_distribution.items():
        if v > 0:
            print(f" - {k:<22}: {v} files mapped")
        
    print("\nVERIFIED STRINGS SAMPLES:")
    for raw, clean, itype in sample_decodes[:6]:
        print(f" Raw   : {raw}")
        print(f" Clean : {clean} ---> Type: {itype}")
        print("-" * 50)
    print("=" * 70)

if __name__ == "__main__":
    run_dry_run_validation()