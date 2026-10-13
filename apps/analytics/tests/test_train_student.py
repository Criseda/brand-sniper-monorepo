import json
import math
import random
from datetime import datetime, timedelta
from types import SimpleNamespace

import numpy as np
import onnx
import pytest
import train_student
from baseline_scorecard import DecisionLog, LoggedDecision, read_decision_log
from label_outcomes import FinalLabels, LabelConfig
from shared_utils import FEATURE_NAMES, FEATURE_VERSION
from train_student import (
    LabeledListing,
    TrainingInputs,
    build_dataset,
    candidate_verdict,
    check_no_look_ahead,
    check_onnx,
    choose_threshold,
    feature_matrix,
    log_to_mlflow,
    mlflow_metrics,
    mlflow_params,
    render_markdown,
    size_rule,
    to_onnx,
    train_model,
    train_student_flow,
    walk_forward_folds,
)

T0 = datetime(2026, 10, 1)
HORIZON = timedelta(days=14)
AS_OF = datetime(2026, 12, 31)


def _epoch(moment: datetime) -> int:
    return int((moment - datetime(1970, 1, 1)).total_seconds())


def _student(price_ratio: float, **overrides) -> dict:
    features: dict[str, float | None] = {name: 0.0 for name in FEATURE_NAMES}
    features.update({"price_to_window_mean": price_ratio, "z_source": 1, "float_value": None})
    features.update(overrides)
    return features


def _decision(listing_id, decided_at, approve=False, student=None, reason="below_threshold", price_cents=1000):
    features = {"z_source": "hybrid"}
    if student is not None:
        features["student"] = student
    row = {
        "type": "decision",
        "timestamp": _epoch(decided_at),
        "venue": "skinport",
        "listing_id": listing_id,
        "event_type": "listed" if listing_id is not None else None,
        "market_hash_name": "AK-47 | Slate (Field-Tested)",
        "price_cents": price_cents,
        "approve": approve,
        "reason": reason,
        "score": -1.0,
        "features": features,
    }
    return LoggedDecision(row=row, build_id=1, look_ahead=False)


def _log(decisions, strategy="zscore_dre_features", feature_version=FEATURE_VERSION) -> DecisionLog:
    return DecisionLog(
        header={"strategy": strategy, "config": {"feature_version": feature_version}},
        decisions=decisions,
        summary=None,
        sha256="sha",
        venue="skinport",
        start=T0,
        end=T0 + timedelta(days=60),
    )


def _label(listing_id, decided_at, profitable, margin=None):
    return {
        "listing_id": listing_id,
        "is_profitable": profitable,
        "resale_net_margin_cents": margin if margin is not None else (100 if profitable else -100),
        "label_available_at": decided_at + HORIZON,
    }


def _labels(rows) -> FinalLabels:
    return FinalLabels(version="v1", contract=LabelConfig().contract(), as_of=AS_OF, rows=rows)


def synthetic(count=4000, days=40, seed=7):
    """Listings over `days` whose profitability depends on the price ratio: cheap listings tend to win."""
    rng = random.Random(seed)
    decisions, labels = [], []
    for index in range(count):
        decided_at = T0 + timedelta(seconds=int(index * days * 86_400 / count))
        ratio = rng.uniform(0.5, 1.2)
        profitable = rng.random() < (0.85 if ratio < 0.7 else 0.08)
        margin = rng.randint(50, 400) if profitable else -rng.randint(50, 400)
        approve = ratio < 0.75 and rng.random() < 0.5
        decisions.append(_decision(str(index), decided_at, approve=approve, student=_student(ratio, z_score=(ratio - 1) * 10)))
        labels.append(_label(str(index), decided_at, profitable, margin))
    return _log(decisions), _labels(labels)


def _row(listing_id, decided_at, profitable=True, label_available_at=None, ratio=0.8):
    return LabeledListing(
        listing_id=listing_id,
        decided_at=decided_at,
        market_hash_name="x",
        price_cents=1000,
        features=tuple([ratio] + [0.0] * (len(FEATURE_NAMES) - 1)),
        rules_approved=False,
        rules_reason="below_threshold",
        label_available_at=label_available_at or decided_at + HORIZON,
        is_profitable=profitable,
        net_margin_cents=100 if profitable else -100,
    )


# --- Dataset ---


def test_dataset_joins_the_first_decision_of_each_labeled_listing():
    at = T0 + timedelta(hours=1)
    decisions = [
        _decision(None, at, approve=True),  # REST snapshot: no label
        _decision("A", at, approve=True, student=_student(0.6), reason="support_floor"),
        _decision("A", at + timedelta(minutes=5), student=_student(0.9)),  # later sighting of A
        _decision("B", at, student=_student(0.9)),  # neutral label
        _decision("C", at, student=_student(0.9)),  # no final label
        _decision("D", at, approve=True),  # duplicate decision, never scored: no features
        _decision("E", datetime(2026, 9, 26), student=_student(0.9)),  # before tradable REST prices
    ]
    labels = _labels(
        [_label("A", at, True), {**_label("B", at, True), "is_profitable": None}, _label("D", at, True), _label("E", at, False)]
    )

    rows = build_dataset(_log(decisions), labels)

    assert [row.listing_id for row in rows] == ["A"]
    assert rows[0].rules_approved is True
    assert rows[0].rules_reason == "support_floor"
    assert rows[0].features[0] == 0.6
    assert math.isnan(rows[0].features[FEATURE_NAMES.index("float_value")])


def test_dataset_refuses_a_log_without_student_features_or_with_another_feature_version():
    with pytest.raises(ValueError, match="zscore_dre_features"):
        build_dataset(_log([], strategy="zscore_dre_sweep"), _labels([]))
    with pytest.raises(ValueError, match="feature version"):
        build_dataset(_log([], feature_version=FEATURE_VERSION + 1), _labels([]))


def test_size_rule_needs_enough_rows_and_enough_of_the_rarer_outcome():
    many = [_row(str(index), T0, profitable=index < 150) for index in range(1200)]

    assert size_rule(many) is None
    assert "fewer than 1000" in size_rule(many[:999])
    assert "rarer outcome" in size_rule([_row(str(index), T0, profitable=index < 50) for index in range(1200)])


# --- Temporal leakage ---


def test_folds_never_train_on_a_label_from_after_the_cutoff():
    rows = [_row(str(day), T0 + timedelta(days=day)) for day in range(40)]

    folds = walk_forward_folds(rows, fold_hours=24)

    assert folds
    for fold in folds:
        check_no_look_ahead(fold)
        assert all(row.label_available_at <= fold.cutoff and row.decided_at < fold.cutoff for row in fold.train)
        assert all(fold.cutoff <= row.decided_at < fold.end for row in fold.validation)
    # The 14 day horizon keeps every training listing at least 14 days before the validation window.
    fold = next(fold for fold in folds if fold.cutoff == T0 + timedelta(days=20))
    assert max(row.decided_at for row in fold.train) == T0 + timedelta(days=6)


def test_a_label_available_after_the_cutoff_is_left_out_even_when_decided_before():
    late_label = _row("late", T0, label_available_at=T0 + timedelta(days=30))
    rows = [late_label, _row("early", T0), _row("validation", T0 + timedelta(days=20))]

    fold = next(fold for fold in walk_forward_folds(rows, fold_hours=24) if fold.validation)

    assert [row.listing_id for row in fold.train] == ["early"]


def test_exploratory_folds_let_training_labels_overlap_the_validation_window():
    rows = [_row(str(day), T0 + timedelta(days=day)) for day in range(5)]

    strict = walk_forward_folds(rows, fold_hours=24)
    exploratory = walk_forward_folds(rows, fold_hours=24, exploratory=True)

    assert all(not fold.train for fold in strict)
    assert [len(fold.train) for fold in exploratory] == [1, 2, 3, 4]


def test_check_no_look_ahead_rejects_a_leaking_fold():
    cutoff = T0 + timedelta(days=20)
    leaking = train_student.Fold(
        cutoff=cutoff, end=cutoff + timedelta(days=1), train=[_row("x", T0 + timedelta(days=10))], validation=[]
    )
    future = train_student.Fold(cutoff=cutoff, end=cutoff + timedelta(days=1), train=[_row("y", cutoff)], validation=[])
    outside = train_student.Fold(cutoff=cutoff, end=cutoff + timedelta(days=1), train=[], validation=[_row("z", T0)])

    with pytest.raises(AssertionError, match="label from after the cutoff"):
        check_no_look_ahead(leaking)
    check_no_look_ahead(leaking, exploratory=True)
    with pytest.raises(AssertionError, match="decided after the cutoff"):
        check_no_look_ahead(future)
    with pytest.raises(AssertionError, match="outside"):
        check_no_look_ahead(outside)


def test_fold_hours_must_be_positive():
    with pytest.raises(ValueError):
        walk_forward_folds([], fold_hours=0)
    assert walk_forward_folds([], fold_hours=1) == []


# --- Model, threshold and ONNX ---


def test_threshold_maximises_net_pnl_with_a_minimum_number_of_trades():
    probabilities = np.linspace(1.0, 0.0, 100)
    margins = np.array([100.0] * 40 + [-100.0] * 60)

    assert choose_threshold(probabilities, margins) == pytest.approx(probabilities[39])
    assert choose_threshold(probabilities[:10], margins[:10]) == 0.5
    # Even when every trade loses, at least MIN_SAMPLE are approved.
    assert choose_threshold(probabilities, -np.abs(margins)) == pytest.approx(probabilities[29])


def test_onnx_export_matches_the_native_model_and_carries_its_signature():
    log, labels = synthetic(count=1500, days=10)
    rows = build_dataset(log, labels)
    model = train_model(rows)

    onnx_model = to_onnx(model, {"feature_version": str(FEATURE_VERSION)})
    gap = check_onnx(onnx_model, model, feature_matrix(rows))

    assert gap <= 1e-4
    assert [item.name for item in onnx_model.graph.input] == ["features"]
    assert [item.name for item in onnx_model.graph.output] == ["probability"]
    assert {entry.key: entry.value for entry in onnx_model.metadata_props} == {"feature_version": str(FEATURE_VERSION)}
    probabilities = model.predict(feature_matrix(rows))
    assert ((0 < probabilities) & (probabilities < 1)).all()


def test_onnx_check_rejects_a_model_that_differs_from_the_native_one():
    log, labels = synthetic(count=1500, days=10)
    rows = build_dataset(log, labels)
    model = train_model(rows)
    onnx_model = to_onnx(model, {})
    model.platt_intercept += 1.0

    with pytest.raises(ValueError, match="differ"):
        check_onnx(onnx_model, model, feature_matrix(rows))


# --- Candidate rule ---


def _cell(status="ok", precision=0.5, total=0, labeled=40):
    return {
        "status": status,
        "trades": labeled,
        "labeled": labeled,
        "precision": {"value": precision, "low": 0.0, "high": 1.0} if status == "ok" else None,
        "net_pnl_cents": {"total": total, "mean": 0, "mean_low": 0, "mean_high": 0} if status == "ok" else None,
    }


@pytest.mark.parametrize(
    ("rules", "at_threshold", "at_volume", "exploratory", "expected"),
    [
        (_cell(total=-100), _cell(total=50), _cell(precision=0.4), False, True),
        (_cell(precision=0.5), _cell(total=-500), _cell(precision=0.6), False, True),
        (_cell(total=100, precision=0.5), _cell(total=50), _cell(precision=0.5), False, False),
        (_cell(status="insufficient_data", labeled=5), _cell(total=50), _cell(precision=0.9), False, False),
        (_cell(total=-100), _cell(status="insufficient_data", labeled=5), _cell(precision=0.4), False, False),
        (_cell(total=-100), _cell(total=50), _cell(precision=0.9), True, False),
    ],
    ids=["more_pnl", "more_precise_at_equal_volume", "no_better", "rules_too_few", "model_too_few", "exploratory"],
)
def test_candidate_only_when_the_model_beats_the_rules_out_of_sample(rules, at_threshold, at_volume, exploratory, expected):
    comparison = {"rules": rules, "model_at_threshold": at_threshold, "model_at_rules_volume": at_volume}

    candidate, reason = candidate_verdict(comparison, exploratory)

    assert candidate is expected
    assert reason


# --- End to end ---


def test_training_run_evaluates_walk_forward_and_exports_a_model():
    log, labels = synthetic()

    result = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=24 * 5))
    report = result.report

    evaluated = [fold for fold in report["folds"] if "skipped" not in fold]
    assert evaluated
    assert any("skipped" in fold for fold in report["folds"])  # The first folds have no label old enough.
    assert report["pooled"]["metrics"]["roc_auc"] > 0.8
    assert report["pooled"]["comparison"]["listings"] == sum(fold["validation_rows"] for fold in evaluated)
    assert report["registrable"] is True
    assert result.onnx_model is not None
    assert report["model"]["onnx_parity_max_gap"] <= 1e-4
    assert report["labels"]["config"]["horizon_seconds"] == 14 * 86_400
    json.dumps(report)  # The report is plain JSON.
    markdown = render_markdown(report)
    assert "## Out of sample, all folds" in markdown
    assert "Live rules" in markdown


def test_training_is_reproducible():
    log, labels = synthetic(count=2500, days=30)

    first = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=24 * 5))
    second = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=24 * 5))

    assert first.report["pooled"] == second.report["pooled"]
    assert first.onnx_model.SerializeToString() == second.onnx_model.SerializeToString()


def test_too_little_data_trains_nothing_and_registers_nothing():
    log, labels = synthetic(count=300, days=30)

    result = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=24 * 5))

    assert result.model is None
    assert result.report["pooled"] is None
    assert result.report["registrable"] is False
    assert result.report["candidate"] is False
    assert any("No final model" in note for note in result.report["notes"])
    assert "## Folds" in render_markdown(result.report)


def test_an_exploratory_run_is_never_registrable_or_a_candidate():
    log, labels = synthetic(count=2500, days=4)

    result = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=12, exploratory=True))

    assert [fold for fold in result.report["folds"] if "skipped" not in fold]
    assert result.report["registrable"] is False
    assert result.report["candidate"] is False
    assert "Exploratory run" in render_markdown(result.report)


# --- MLflow and the flow ---


class FakeMlflowClient:
    def __init__(self, registered=()):
        self.params, self.metrics, self.artifacts, self.registered = {}, {}, [], list(registered)
        self.versions, self.status = [], None

    def get_experiment_by_name(self, name):
        return None

    def create_experiment(self, name):
        return "exp"

    def create_run(self, experiment_id, run_name, tags):
        self.tags = tags
        return SimpleNamespace(info=SimpleNamespace(run_id="run-1", artifact_uri="mlflow-artifacts:/exp/run-1/artifacts"))

    def log_param(self, run_id, key, value):
        self.params[key] = value

    def log_metric(self, run_id, key, value):
        self.metrics[key] = value

    def log_artifact(self, run_id, path, artifact_path=None):
        self.artifacts.append(path)

    def search_registered_models(self, filter_string):
        return self.registered

    def create_registered_model(self, name):
        self.registered.append(name)

    def create_model_version(self, name, source, run_id, tags):
        self.versions.append((name, source, tags))
        return SimpleNamespace(version=1)

    def set_terminated(self, run_id, status):
        self.status = status


def test_mlflow_logs_the_label_config_and_registers_a_registrable_model(monkeypatch, tmp_path):
    log, labels = synthetic()
    result = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=24 * 5))
    paths = train_student.write_reports(result.report, result.onnx_model, tmp_path)
    client = FakeMlflowClient()
    monkeypatch.setattr(train_student, "MlflowClient", lambda tracking_uri: client)

    run_id, version = log_to_mlflow(result.report, paths)

    assert (run_id, version) == ("run-1", "1")
    assert client.params["label_version"] == "v1"
    assert client.params["label.min_comparable_sales"] == "3"
    assert client.params["feature_version"] == str(FEATURE_VERSION)
    assert "roc_auc" in client.metrics
    assert client.registered == ["brand-sniper-student"]
    assert client.versions[0][1].endswith("/student/student_model.onnx")
    assert client.status == "FINISHED"
    assert onnx.load(str(tmp_path / "student_model.onnx")).graph.output[0].name == "probability"
    assert set(mlflow_params(result.report)) >= {"decision_log_sha256", "exploratory", "model.num_leaves"}
    assert mlflow_metrics(result.report)["candidate"] in (0.0, 1.0)


def test_mlflow_registers_nothing_for_an_unregistrable_run(monkeypatch, tmp_path):
    log, labels = synthetic(count=2500, days=4)
    result = train_student.train_student(TrainingInputs(log=log, labels=labels, fold_hours=12, exploratory=True))
    paths = train_student.write_reports(result.report, result.onnx_model, tmp_path)
    client = FakeMlflowClient()
    monkeypatch.setattr(train_student, "MlflowClient", lambda tracking_uri: client)

    _, version = log_to_mlflow(result.report, paths)

    assert version is None
    assert client.versions == []


def test_mlflow_marks_the_run_failed_when_logging_breaks(monkeypatch, tmp_path):
    log, labels = synthetic(count=300, days=30)
    result = train_student.train_student(TrainingInputs(log=log, labels=labels))
    client = FakeMlflowClient()

    def broken(run_id, key, value):
        raise RuntimeError("down")

    client.log_metric = broken
    monkeypatch.setattr(train_student, "MlflowClient", lambda tracking_uri: client)

    with pytest.raises(RuntimeError):
        log_to_mlflow(result.report, [])
    assert client.status == "FAILED"


def _write_log(path, log: DecisionLog):
    source = f"database:skinport:{log.start.isoformat()}/{log.end.isoformat()}"
    lines = [{"type": "run", "strategy": log.strategy, "config": log.config, "source": source}]
    lines += [decision.row for decision in log.decisions]
    lines.append({"type": "summary"})
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")


def test_cli_flow_reads_the_log_and_labels_and_writes_the_report(monkeypatch, tmp_path):
    log, labels = synthetic(count=2500, days=4)
    log_path = tmp_path / "decisions.jsonl"
    _write_log(log_path, log)
    calls = []

    async def fake_fetch(version, as_of, include_revisable=False):
        calls.append((version, as_of, include_revisable))
        return labels

    monkeypatch.setattr(train_student, "fetch_final_labels", fake_fetch)

    assert read_decision_log(log_path).strategy == "zscore_dre_features"
    status = train_student.main(
        [
            "--decisions",
            str(log_path),
            "--as-of",
            "2026-12-31T00:00",
            "--fold-hours",
            "12",
            "--exploratory",
            "--no-mlflow",
            "--out-dir",
            str(tmp_path / "out"),
        ]
    )

    assert status == 0
    assert calls == [("v1", datetime(2026, 12, 31), True)]
    report = json.loads((tmp_path / "out" / "student_model.json").read_text(encoding="utf-8"))
    assert report["exploratory"] is True
    assert (tmp_path / "out" / "student_model.md").exists()


@pytest.mark.asyncio
async def test_flow_logs_to_mlflow_when_asked(monkeypatch, tmp_path):
    log, labels = synthetic(count=300, days=30)
    log_path = tmp_path / "decisions.jsonl"
    _write_log(log_path, log)

    async def fake_fetch(version, as_of, include_revisable=False):
        return labels

    logged = []
    monkeypatch.setattr(train_student, "fetch_final_labels", fake_fetch)
    monkeypatch.setattr(train_student, "log_to_mlflow", lambda report, paths: logged.append(paths) or ("run", None))

    report = await train_student_flow(decisions=str(log_path), as_of=AS_OF, out_dir=str(tmp_path / "out"))

    assert report["registrable"] is False
    assert len(logged) == 1
