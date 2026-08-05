import pytest
from shared_utils.item_classifier import (
    build_versioned_name,
    parse_item_meta,
    parse_version_from_name,
)


@pytest.mark.parametrize(
    ("raw_name", "expected_name", "expected_type"),
    [
        pytest.param(
            "AK-47 | Redline (Field-Tested)",
            "AK-47 | Redline (Field-Tested)",
            "Weapon Skin",
            id="weapon_skin",
        ),
        pytest.param(
            "\u2605 Butterfly Knife | Doppler (Factory New)",
            "\u2605 Butterfly Knife | Doppler (Factory New)",
            "Knife",
            id="knife_star",
        ),
        pytest.param(
            "\u2605 Specialist Gloves | Crimson Web (Minimal Wear)",
            "\u2605 Specialist Gloves | Crimson Web (Minimal Wear)",
            "Glove",
            id="glove",
        ),
        pytest.param(
            "Sticker | Titan (Holo) (Katowice 2014)",
            "Sticker | Titan (Holo) (Katowice 2014)",
            "Sticker",
            id="sticker",
        ),
        pytest.param(
            "Sticker Capsule",
            None,
            "Sticker",
            id="sticker_no_pipe",
        ),
        pytest.param(
            "Music Kit | Austin Wintory, Journey",
            "Music Kit | Austin Wintory, Journey",
            "Music Kit",
            id="music_kit",
        ),
        pytest.param(
            "Patch | Virtus.Pro (Foil) (Atlanta 2017)",
            "Patch | Virtus.Pro (Foil) (Atlanta 2017)",
            "Patch",
            id="patch",
        ),
        pytest.param(
            "Operation Phoenix Case",
            "Operation Phoenix Case",
            "Container/Collectible",
            id="container_case",
        ),
        pytest.param("CS20 Capsule", None, "Container/Collectible", id="container_capsule"),
        pytest.param("Elite Crew | FBI (Field-Tested)", None, "Agent", id="agent_by_keyword"),
        pytest.param("Some Agent Skin", None, "Agent", id="agent_fallback_no_wear"),
        pytest.param(
            "AK-47%20|%20Redline%20(Field-Tested).csv",
            "AK-47 | Redline (Field-Tested)",
            "Weapon Skin",
            id="url_encoded_csv",
        ),
    ],
)
def test_parse_item_meta(raw_name, expected_name, expected_type):
    name, typ = parse_item_meta(raw_name)
    if expected_name is not None:
        assert name == expected_name
    assert typ == expected_type


@pytest.mark.parametrize(
    ("raw_name", "expected_base", "expected_version"),
    [
        pytest.param(
            "\u2605 Butterfly Knife | Doppler (Phase 3) (Factory New)",
            "\u2605 Butterfly Knife | Doppler (Factory New)",
            "Phase 3",
            id="with_phase",
        ),
        pytest.param(
            "★ Karambit | Doppler (Ruby) (Factory New)",
            "★ Karambit | Doppler (Factory New)",
            "Ruby",
            id="with_gem",
        ),
        pytest.param(
            "AK-47 | Redline (Field-Tested)",
            "AK-47 | Redline (Field-Tested)",
            None,
            id="no_version",
        ),
        pytest.param("AK-47 | Redline", "AK-47 | Redline", None, id="no_wear_suffix"),
    ],
)
def test_parse_version_from_name(raw_name, expected_base, expected_version):
    base, version = parse_version_from_name(raw_name)
    assert base == expected_base
    assert version == expected_version


@pytest.mark.parametrize(
    ("base", "version", "expected"),
    [
        pytest.param(
            "AK-47 | Redline (Field-Tested)",
            "Phase 3",
            "AK-47 | Redline (Phase 3) (Field-Tested)",
            id="with_version_and_wear",
        ),
        pytest.param("AK-47 | Redline", "Phase 3", "AK-47 | Redline (Phase 3)", id="with_version_no_wear"),
        pytest.param(
            "AK-47 | Redline (Field-Tested)",
            None,
            "AK-47 | Redline (Field-Tested)",
            id="none_version",
        ),
        pytest.param(
            "AK-47 | Redline (Field-Tested)",
            "default",
            "AK-47 | Redline (Field-Tested)",
            id="default_version",
        ),
    ],
)
def test_build_versioned_name(base, version, expected):
    assert build_versioned_name(base, version) == expected


def test_parse_build_roundtrip():
    original = "\u2605 Butterfly Knife | Doppler (Phase 3) (Factory New)"
    base, version = parse_version_from_name(original)
    rebuilt = build_versioned_name(base, version)
    assert rebuilt == original


@pytest.mark.parametrize("bad_name", [None, 123, ["name"], {"name": "x"}], ids=["none", "int", "list", "dict"])
def test_parse_item_meta_rejects_non_string(bad_name):
    with pytest.raises(TypeError):
        parse_item_meta(bad_name)


@pytest.mark.parametrize("bad_name", [None, 123], ids=["none", "int"])
def test_parse_version_from_name_rejects_non_string(bad_name):
    with pytest.raises(TypeError):
        parse_version_from_name(bad_name)


@pytest.mark.parametrize("bad_name", [None, 123], ids=["none", "int"])
def test_build_versioned_name_rejects_non_string_base(bad_name):
    with pytest.raises(TypeError):
        build_versioned_name(bad_name, "Phase 3")
