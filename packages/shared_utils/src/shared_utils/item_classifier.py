import urllib.parse

WEAR_SUFFIXES = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]


def _require_str(value: object, arg_name: str) -> str:
    """Validates that a name argument is a non-empty string, raising TypeError otherwise."""
    if not isinstance(value, str):
        raise TypeError(
            f"{arg_name} must be a string, got {type(value).__name__}. Refusing to classify a non-string item name."
        )
    return value


def parse_item_meta(name_or_filename: str) -> tuple[str, str]:
    """
    Decodes a filename or asset name, resolving the clean market_hash_name
    and classifying its structural category (e.g. Knife, Glove, Weapon Skin, Agent).

    Raises TypeError if name_or_filename is not a string.
    """
    raw_name = _require_str(name_or_filename, "name_or_filename")
    clean_name = urllib.parse.unquote(raw_name.replace(".csv", ""))

    if "\u2605" in clean_name:
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
    elif (
        any(
            f in clean_name
            for f in [
                "NSWC SEAL",
                "Guerrilla Warfare",
                "Sabre",
                "TACP",
                "Professionals",
                "FBI",
                "SWAT",
                "Gendarmerie",
                "KSK",
            ]
        )
        or "Agent" in clean_name
    ):
        item_type = "Agent"
    else:
        if not any(w in clean_name for w in WEAR_SUFFIXES):
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
      "\u2605 Butterfly Knife | Doppler (Phase 3) (Factory New)"
      returns: ("\u2605 Butterfly Knife | Doppler (Factory New)", "Phase 3")

    Raises TypeError if name is not a string.
    """
    name = _require_str(name, "name")
    for wear in WEAR_SUFFIXES:
        if name.endswith(wear):
            rest = name[: -len(wear)].strip()
            # Match parentheses at the end of the rest string, e.g. "(Phase 3)"
            if rest.endswith(")"):
                open_idx = rest.rfind("(")
                if open_idx != -1 and open_idx < len(rest) - 2:
                    content = rest[open_idx + 1 : -1]
                    if ")" not in content and "(" not in content:
                        version = content
                        base_name_without_wear = rest[:open_idx].strip()
                        base_name = f"{base_name_without_wear} {wear}"
                        return base_name, version
            break
    return name, None


def build_versioned_name(market_hash_name: str, version: str | None) -> str:
    """
    Inserts a version/phase string before the wear suffix in a market hash name.
    Inverse of parse_version_from_name.

    Examples:
      build_versioned_name("AK-47 | Redline (Field-Tested)", "Phase 3")
        -> "AK-47 | Redline (Phase 3) (Field-Tested)"

      build_versioned_name("AK-47 | Redline", "Phase 3")
        -> "AK-47 | Redline (Phase 3)"

      build_versioned_name("AK-47 | Redline (Field-Tested)", None)
        -> "AK-47 | Redline (Field-Tested)"

    Raises TypeError if market_hash_name is not a string.
    """
    market_hash_name = _require_str(market_hash_name, "market_hash_name")
    if not version or version == "default":
        return market_hash_name

    for wear in WEAR_SUFFIXES:
        if market_hash_name.endswith(wear):
            base_name = market_hash_name[: -len(wear)].strip()
            return f"{base_name} ({version}) {wear}"

    return f"{market_hash_name} ({version})"
