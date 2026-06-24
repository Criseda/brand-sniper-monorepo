import urllib.parse

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
