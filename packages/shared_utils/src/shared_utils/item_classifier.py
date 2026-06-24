import urllib.parse
import re

def parse_item_meta(name_or_filename: str) -> tuple[str, str]:
    """
    Decodes a filename or asset name, resolving the clean market_hash_name 
    and classifying its structural category (e.g. Knife, Glove, Weapon Skin, Agent).
    """
    clean_name = urllib.parse.unquote(name_or_filename.replace(".csv", ""))
    
    if "★" in clean_name:
        if any(w in clean_name for w in ["Gloves", "Wraps"]):
            item_type = "Glove"
        else:
            item_type = "Knife"
    elif "Sticker |" in clean_name or clean_name.startswith("Sticker"):
        item_type = "Sticker"
    elif "Music Kit |" in clean_name:
        item_type = "Music Kit"
    elif "Patch |" in clean_name:
        item_type = "Patch"
    elif any(f in clean_name for f in ["NSWC SEAL", "Guerrilla Warfare", "Sabre", "TACP", "Professionals", "FBI", "SWAT", "Gendarmerie", "KSK"]) or "Agent" in clean_name:
        item_type = "Agent"
    else:
        wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
        if not any(w in clean_name for w in wears):
            if any(c in clean_name for c in ["Case", "Capsule", "Package", "Pin"]):
                item_type = "Container/Collectible"
            else:
                item_type = "Agent"
        else:
            item_type = "Weapon Skin"
            
    return clean_name, item_type

def parse_version_from_name(name: str) -> tuple[str, str | None]:
    """
    Parses a version/phase (e.g., Phase 3, Ruby, Sapphire) from a structured name.
    Example:
      "★ Butterfly Knife | Doppler (Phase 3) (Factory New)"
      returns: ("★ Butterfly Knife | Doppler (Factory New)", "Phase 3")
    """
    wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
    for wear in wears:
        if name.endswith(wear):
            rest = name[:-len(wear)].strip()
            # Match parentheses at the end of the rest string, e.g. "(Phase 3)"
            match = re.search(r"\(([^)]+)\)$", rest)
            if match:
                version = match.group(1)
                base_name_without_wear = rest[:-len(match.group(0))].strip()
                base_name = f"{base_name_without_wear} {wear}"
                return base_name, version
            break
    return name, None

