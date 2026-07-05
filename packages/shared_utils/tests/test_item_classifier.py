import pytest
from shared_utils.item_classifier import (
    parse_item_meta,
    parse_version_from_name,
    build_versioned_name,
)


class TestParseItemMeta:
    def test_weapon_skin(self):
        name, typ = parse_item_meta("AK-47 | Redline (Field-Tested)")
        assert name == "AK-47 | Redline (Field-Tested)"
        assert typ == "Weapon Skin"

    def test_knife_star(self):
        name, typ = parse_item_meta("\u2605 Butterfly Knife | Doppler (Factory New)")
        assert name == "\u2605 Butterfly Knife | Doppler (Factory New)"
        assert typ == "Knife"

    def test_glove(self):
        name, typ = parse_item_meta("\u2605 Specialist Gloves | Crimson Web (Minimal Wear)")
        assert name == "\u2605 Specialist Gloves | Crimson Web (Minimal Wear)"
        assert typ == "Glove"

    def test_sticker(self):
        name, typ = parse_item_meta("Sticker | Titan (Holo) (Katowice 2014)")
        assert name == "Sticker | Titan (Holo) (Katowice 2014)"
        assert typ == "Sticker"

    def test_sticker_no_pipe(self):
        name, typ = parse_item_meta("Sticker Capsule")
        assert typ == "Sticker"

    def test_music_kit(self):
        name, typ = parse_item_meta("Music Kit | Austin Wintory, Journey")
        assert name == "Music Kit | Austin Wintory, Journey"
        assert typ == "Music Kit"

    def test_patch(self):
        name, typ = parse_item_meta("Patch | Virtus.Pro (Foil) (Atlanta 2017)")
        assert name == "Patch | Virtus.Pro (Foil) (Atlanta 2017)"
        assert typ == "Patch"

    def test_container_case(self):
        name, typ = parse_item_meta("Operation Phoenix Case")
        assert name == "Operation Phoenix Case"
        assert typ == "Container/Collectible"

    def test_container_capsule(self):
        name, typ = parse_item_meta("CS20 Capsule")
        assert typ == "Container/Collectible"

    def test_agent_by_keyword(self):
        name, typ = parse_item_meta("Elite Crew | FBI (Field-Tested)")
        assert typ == "Agent"

    def test_agent_fallback_no_wear(self):
        name, typ = parse_item_meta("Some Agent Skin")
        assert typ == "Agent"

    def test_url_encoded_csv(self):
        name, typ = parse_item_meta("AK-47%20|%20Redline%20(Field-Tested).csv")
        assert name == "AK-47 | Redline (Field-Tested)"
        assert typ == "Weapon Skin"


class TestParseVersionFromName:
    def test_with_phase(self):
        base, version = parse_version_from_name(
            "\u2605 Butterfly Knife | Doppler (Phase 3) (Factory New)"
        )
        assert base == "\u2605 Butterfly Knife | Doppler (Factory New)"
        assert version == "Phase 3"

    def test_with_gem(self):
        base, version = parse_version_from_name(
            "★ Karambit | Doppler (Ruby) (Factory New)"
        )
        assert base == "★ Karambit | Doppler (Factory New)"
        assert version == "Ruby"

    def test_no_version(self):
        base, version = parse_version_from_name("AK-47 | Redline (Field-Tested)")
        assert base == "AK-47 | Redline (Field-Tested)"
        assert version is None

    def test_no_wear_suffix(self):
        base, version = parse_version_from_name("AK-47 | Redline")
        assert base == "AK-47 | Redline"
        assert version is None


class TestBuildVersionedName:
    def test_with_version_and_wear(self):
        result = build_versioned_name("AK-47 | Redline (Field-Tested)", "Phase 3")
        assert result == "AK-47 | Redline (Phase 3) (Field-Tested)"

    def test_with_version_no_wear(self):
        result = build_versioned_name("AK-47 | Redline", "Phase 3")
        assert result == "AK-47 | Redline (Phase 3)"

    def test_none_version(self):
        result = build_versioned_name("AK-47 | Redline (Field-Tested)", None)
        assert result == "AK-47 | Redline (Field-Tested)"

    def test_default_version(self):
        result = build_versioned_name("AK-47 | Redline (Field-Tested)", "default")
        assert result == "AK-47 | Redline (Field-Tested)"

    def test_roundtrip(self):
        original = "\u2605 Butterfly Knife | Doppler (Phase 3) (Factory New)"
        base, version = parse_version_from_name(original)
        rebuilt = build_versioned_name(base, version)
        assert rebuilt == original
