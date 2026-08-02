import logging

import validate_historical


def _write_csv(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def test_missing_data_dir_logs_error(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(validate_historical, "DATA_DIR", tmp_path / "does-not-exist")

    with caplog.at_level(logging.ERROR, logger="analytics.validate"):
        validate_historical.run_dry_run_validation()

    assert "Data directory not found" in caplog.text


def test_empty_data_dir_reports_zero_files(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(validate_historical, "DATA_DIR", tmp_path)

    with caplog.at_level(logging.INFO, logger="analytics.validate"):
        validate_historical.run_dry_run_validation()

    assert "Total Operational Files   : 0" in caplog.text


def test_valid_files_counted_and_classified(monkeypatch, tmp_path, caplog):
    _write_csv(
        tmp_path / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        ["name,unix timestamp,price,quantity", "AK-47 | Redline,1700000000,10.5,3", "AK-47 | Redline,1700000001,11.0,2"],
    )
    _write_csv(
        tmp_path / "%E2%98%85%20Butterfly%20Knife%20%7C%20Doppler%20(Factory%20New).csv",
        ["name,unix timestamp,price,quantity", "★ Butterfly Knife | Doppler,1700000000,700.0,1"],
    )
    monkeypatch.setattr(validate_historical, "DATA_DIR", tmp_path)

    with caplog.at_level(logging.INFO, logger="analytics.validate"):
        validate_historical.run_dry_run_validation()

    assert "Total Operational Files   : 2" in caplog.text
    assert "Total Clean Rows Preserved : 3" in caplog.text
    assert "Weapon Skin" in caplog.text and "1 files mapped" in caplog.text
    assert "Knife" in caplog.text and "1 files mapped" in caplog.text


def test_corrupt_timestamp_rows_are_dropped(monkeypatch, tmp_path, caplog):
    _write_csv(
        tmp_path / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [
            "name,unix timestamp,price,quantity",
            "AK-47 | Redline,1700000000,10.5,3",
            "AK-47 | Redline,garbage,10.5,3",
            "AK-47 | Redline,also-bad,11.0,1",
        ],
    )
    monkeypatch.setattr(validate_historical, "DATA_DIR", tmp_path)

    with caplog.at_level(logging.INFO, logger="analytics.validate"):
        validate_historical.run_dry_run_validation()

    assert "Files Requiring Row Drops : 1" in caplog.text
    assert "Total Corrupt Rows Dropped: 2" in caplog.text
    assert "Total Clean Rows Preserved : 1" in caplog.text


def test_unreadable_file_is_skipped(monkeypatch, tmp_path, caplog):
    (tmp_path / "broken.csv").write_bytes(b"\xff\xfe not a csv")
    monkeypatch.setattr(validate_historical, "DATA_DIR", tmp_path)

    with caplog.at_level(logging.INFO, logger="analytics.validate"):
        validate_historical.run_dry_run_validation()

    assert "Skipping corrupt or unreadable file" in caplog.text
    assert "Total Clean Rows Preserved : 0" in caplog.text
