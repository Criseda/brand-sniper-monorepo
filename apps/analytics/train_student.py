"""
Student model (#234): a LightGBM classifier of whether a feed listing is profitable, trained on market
outcome labels with walk forward validation, calibrated, exported to ONNX and compared with the live rules.

The input is a decision log from the replay harness made with the `zscore_dre_features` strategy: the live
decisions, plus the student features of every feed listing (shared_utils.features), computed from the
price windows and baselines the listener had at that moment. Rows join to the final labels of one label
version (`outcome_data.fetch_final_labels`). Neutral labels (too few comparable sales) are left out.

Validation is walk forward only. A fold with cutoff t trains on listings decided before t whose label was
available by t (`label_available_at <= t`), and validates on the listings decided in the next window. The
14 day label horizon therefore puts at least 14 days between a training listing and a validation listing.
The model and the live rules are scored on the same validation listings with the #249 scorecard metrics
(precision with a Wilson interval, fee aware net P&L, insufficient data below 30 labeled trades), and on
the DRE's own deals (#333) next to the fee floor (#331) and the ratio of the latest baseline price to the price.

An exploratory run (`--exploratory`) also reads labels that are not final yet and lets training labels
overlap the validation window. It exercises the pipeline on the labels that exist; its numbers are not a
valid evaluation, and it never registers a model.

Usage (from apps/analytics), or replay and training at once with `make student START=... END=...`:
    uv run python train_student.py --decisions ../../data/student/decisions.jsonl

See docs/roadmap_proven_edge.md, sections 4.5 and 4.6.
"""

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from shared_utils import setup_script_environment

REPO_ROOT = setup_script_environment(__file__)

import lightgbm
import numpy as np
import onnx
import onnxmltools
import onnxruntime
from baseline_scorecard import (
    FEATURES_STRATEGY,
    MIN_SAMPLE,
    REASON_BELOW_FEE_FLOOR,
    TICK_KIND_LISTING,
    DecisionLog,
    Trade,
    dollars,
    git_commit,
    is_scorable,
    parse_utc,
    percent,
    read_decision_log,
    scorable_start,
    score_trades,
    set_outcome,
)
from mlflow.client import MlflowClient
from onnx import TensorProto, helper
from onnxmltools.convert.common.data_types import FloatTensorType
from outcome_data import fetch_final_labels
from outcome_labels import LABEL_VERSION, FinalLabels, label_mlflow_params
from prefect import flow
from shared_utils import FEATURE_NAMES, FEATURE_VERSION, feature_vector, get_logger, utc_now_naive
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

logger = get_logger("analytics.student")

REPORT_VERSION = 3
MLFLOW_EXPERIMENT = "student-model"
REGISTERED_MODEL = "brand-sniper-student"

# The flow refuses to train a fold, or register a model, on fewer labeled listings than this, or with
# fewer than MIN_MINORITY_CLASS of the rarer outcome. Profitable listings are the rare class (about 4%).
MIN_TRAIN_ROWS = 1000
MIN_MINORITY_CLASS = 100

# A fold's training rows are cut into this many blocks in time. Each block after the first is scored by a
# model fitted on the blocks before it, and those out of sample scores calibrate the probabilities and pick
# the operating threshold. See docs/benchmarks/student_threshold.md for why.
CALIBRATION_BLOCKS = 5
DEFAULT_FOLD_HOURS = 24
# The log stores whole seconds and the label the edge receive time in milliseconds.
FIRST_SIGHTING_TOLERANCE = timedelta(seconds=1)
CALIBRATION_BINS = 10

# Fixed hyperparameters: small trees and a seed, so a rerun on the same inputs trains the same model.
MODEL_PARAMS: dict[str, Any] = {
    "n_estimators": 200,
    "learning_rate": 0.05,
    "num_leaves": 15,
    "min_child_samples": 40,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.9,
    "reg_lambda": 1.0,
    "random_state": 234,
    "deterministic": True,
    "force_row_wise": True,
    "n_jobs": 1,
    "verbose": -1,
}

ONNX_INPUT = "features"
ONNX_OUTPUT = "probability"
ONNX_TARGET_OPSET = 15
# The ONNX model and the native model may differ by float32 rounding, never by more than this.
ONNX_PARITY_TOLERANCE = 1e-4


# --- Dataset ---


@dataclass(frozen=True)
class LabeledListing:
    """One feed listing: its features at decision time, the live rules' verdict, and its market outcome."""

    listing_id: str
    decided_at: datetime
    market_hash_name: str
    price_cents: int
    features: tuple[float, ...]
    rules_approved: bool
    rules_reason: str
    label_available_at: datetime
    is_profitable: bool
    net_margin_cents: int

    @property
    def dre_approved(self) -> bool:
        """Whether the DRE approved the listing before the fee floor (#331): approved, or rejected only by it."""
        return self.rules_approved or self.rules_reason == REASON_BELOW_FEE_FLOOR


def build_dataset(log: DecisionLog, labels: FinalLabels) -> list[LabeledListing]:
    """
    Every feed listing with a non neutral label whose first sighting was scored, in decision order.

    A label describes the listing as first seen (`listed_at`), so only the decision on that sighting counts.
    A listing whose first sighting was not scored (it happened before the log starts, or, in logs made
    before #312, it repeated the item's previous price) is left out rather than joined to a later sighting.
    Decisions before tradable REST prices filled the windows, or made with a later baseline build, are left
    out, as in the scorecard.
    """
    if log.strategy != FEATURES_STRATEGY:
        raise ValueError(f"Training needs a log made with the {FEATURES_STRATEGY} strategy; this one used {log.strategy}")
    logged_version = log.config.get("feature_version")
    if logged_version != FEATURE_VERSION:
        raise ValueError(f"The log holds feature version {logged_version}; this code computes version {FEATURE_VERSION}")

    by_listing = {str(row["listing_id"]): row for row in labels.rows}
    start = scorable_start(log)
    rows: list[LabeledListing] = []
    seen: set[str] = set()
    for decision in log.decisions:
        row = decision.row
        if decision.tick_kind != TICK_KIND_LISTING:
            continue
        listing_id = str(row["listing_id"])
        if listing_id in seen:
            continue
        seen.add(listing_id)
        student = (row.get("features") or {}).get("student")
        label = by_listing.get(listing_id)
        if student is None or not is_scorable(decision, start) or label is None or label["is_profitable"] is None:
            continue
        if abs(decision.decided_at - label["listed_at"]) > FIRST_SIGHTING_TOLERANCE:
            continue
        rows.append(
            LabeledListing(
                listing_id=listing_id,
                decided_at=decision.decided_at,
                market_hash_name=row["market_hash_name"],
                price_cents=int(row["price_cents"]),
                features=tuple(feature_vector(student)),
                rules_approved=bool(row["approve"]),
                rules_reason=str(row["reason"]),
                label_available_at=label["label_available_at"],
                is_profitable=bool(label["is_profitable"]),
                net_margin_cents=int(label["resale_net_margin_cents"]),
            )
        )
    return rows


def size_rule(rows: Sequence[LabeledListing]) -> str | None:
    """Why `rows` are too few, or too unbalanced, to train on; None when they are enough."""
    positives = sum(1 for row in rows if row.is_profitable)
    minority = min(positives, len(rows) - positives)
    if len(rows) < MIN_TRAIN_ROWS:
        return f"{len(rows)} labeled listings, fewer than {MIN_TRAIN_ROWS}"
    if minority < MIN_MINORITY_CLASS:
        return f"{minority} listings of the rarer outcome, fewer than {MIN_MINORITY_CLASS}"
    return None


# --- Walk forward folds ---


@dataclass(frozen=True)
class Fold:
    cutoff: datetime
    end: datetime
    train: list[LabeledListing]
    validation: list[LabeledListing]


def walk_forward_folds(rows: Sequence[LabeledListing], fold_hours: int, exploratory: bool = False) -> list[Fold]:
    """
    Expanding window folds with cutoffs every `fold_hours`, from the first decision to the last. A window
    without listings (my PC was off) makes no fold.

    A fold trains on listings decided before its cutoff whose label was available by the cutoff, and
    validates on the listings decided in [cutoff, cutoff + fold_hours). `exploratory` drops the label
    availability condition, so training labels may use market data from the validation window.
    """
    if fold_hours < 1:
        raise ValueError("fold_hours must be at least 1")
    if not rows:
        return []
    step = timedelta(hours=fold_hours)
    first = min(row.decided_at for row in rows)
    last = max(row.decided_at for row in rows)
    first_cutoff = first.replace(minute=0, second=0, microsecond=0) + step
    folds = []
    cutoff = first_cutoff
    while cutoff <= last:
        end = cutoff + step
        train = [row for row in rows if row.decided_at < cutoff and (exploratory or row.label_available_at <= cutoff)]
        validation = [row for row in rows if cutoff <= row.decided_at < end]
        if validation:
            folds.append(Fold(cutoff=cutoff, end=end, train=train, validation=validation))
        cutoff = end
    return folds


def check_no_look_ahead(fold: Fold, exploratory: bool = False) -> None:
    """Raises when a fold trains on anything from after its cutoff, or validates on anything before it."""
    for row in fold.train:
        if row.decided_at >= fold.cutoff:
            raise AssertionError(f"Training listing {row.listing_id} was decided after the cutoff {fold.cutoff}")
        if not exploratory and row.label_available_at > fold.cutoff:
            raise AssertionError(f"Training listing {row.listing_id} has a label from after the cutoff {fold.cutoff}")
    for row in fold.validation:
        if not fold.cutoff <= row.decided_at < fold.end:
            raise AssertionError(f"Validation listing {row.listing_id} is outside [{fold.cutoff}, {fold.end})")


# --- Model ---


@dataclass
class StudentModel:
    """LightGBM on the features, a Platt scaling of its raw score, and the operating threshold."""

    booster: lightgbm.LGBMClassifier
    platt_slope: float
    platt_intercept: float
    threshold: float
    calibration_rows: int

    def raw_scores(self, features: np.ndarray) -> np.ndarray:
        return np.asarray(self.booster.booster_.predict(features, raw_score=True), dtype=np.float64)

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Calibrated probability that each listing is profitable."""
        return 1.0 / (1.0 + np.exp(-(self.platt_slope * self.raw_scores(features) + self.platt_intercept)))


def feature_matrix(rows: Sequence[LabeledListing]) -> np.ndarray:
    return np.array([row.features for row in rows], dtype=np.float32).reshape(len(rows), len(FEATURE_NAMES))


def outcomes(rows: Sequence[LabeledListing]) -> np.ndarray:
    return np.array([int(row.is_profitable) for row in rows], dtype=np.int64)


def choose_threshold(probabilities: np.ndarray, margins: np.ndarray) -> float:
    """
    The probability threshold with the largest total net P&L on the calibration rows, approving at least
    MIN_SAMPLE of them; 0.5 when there are too few rows to choose.
    """
    if len(probabilities) < MIN_SAMPLE:
        return 0.5
    order = np.argsort(-probabilities, kind="stable")
    cumulative = np.cumsum(margins[order])
    best_index = MIN_SAMPLE - 1 + int(np.argmax(cumulative[MIN_SAMPLE - 1 :]))
    return float(probabilities[order][best_index])


def fit_booster(rows: Sequence[LabeledListing]) -> lightgbm.LGBMClassifier:
    booster = lightgbm.LGBMClassifier(**MODEL_PARAMS)
    booster.fit(feature_matrix(rows), outcomes(rows))
    return booster


def walk_forward_raw_scores(ordered: Sequence[LabeledListing]) -> tuple[list[LabeledListing], np.ndarray]:
    """
    Out of sample raw scores of the training rows, from an inner walk forward over CALIBRATION_BLOCKS blocks
    in time: each block after the first is scored by a model fitted on every row before it. A block whose
    earlier rows hold only one outcome has no score. Every row is from before the fold cutoff, so nothing
    here looks past it.
    """
    edges = [len(ordered) * index // CALIBRATION_BLOCKS for index in range(CALIBRATION_BLOCKS + 1)]
    scored: list[LabeledListing] = []
    raw_scores: list[float] = []
    for block in range(1, CALIBRATION_BLOCKS):
        earlier, held_out = ordered[: edges[block]], ordered[edges[block] : edges[block + 1]]
        if not held_out or len(set(outcomes(earlier).tolist())) < 2:
            continue
        booster = fit_booster(earlier)
        raw_scores.extend(np.asarray(booster.booster_.predict(feature_matrix(held_out), raw_score=True)).tolist())
        scored.extend(held_out)
    return scored, np.array(raw_scores, dtype=np.float64)


def train_model(rows: Sequence[LabeledListing]) -> StudentModel:
    """
    Fits LightGBM on every row. The Platt scaling and the operating threshold come from the out of sample
    scores of `walk_forward_raw_scores`, which cover about 80% of the rows rather than a small newest slice:
    with a few hundred calibration rows, approving at least MIN_SAMPLE of them forced the threshold low.
    """
    ordered = sorted(rows, key=lambda row: (row.decided_at, row.listing_id))
    calibration_rows, raw = walk_forward_raw_scores(ordered)
    model = StudentModel(
        booster=fit_booster(ordered),
        platt_slope=1.0,
        platt_intercept=0.0,
        threshold=0.5,
        calibration_rows=len(calibration_rows),
    )

    calibration_y = outcomes(calibration_rows)
    if len(set(calibration_y.tolist())) == 2:
        platt = LogisticRegression(C=np.inf, solver="lbfgs").fit(raw.reshape(-1, 1), calibration_y)
        model.platt_slope = float(platt.coef_[0][0])
        model.platt_intercept = float(platt.intercept_[0])
    probabilities = 1.0 / (1.0 + np.exp(-(model.platt_slope * raw + model.platt_intercept)))
    margins = np.array([row.net_margin_cents for row in calibration_rows], dtype=np.float64)
    model.threshold = choose_threshold(probabilities, margins)
    return model


# --- ONNX ---


def _split_thresholds(booster: lightgbm.LGBMClassifier) -> dict[int, set[float]]:
    """LightGBM's split thresholds (doubles) per feature index."""
    thresholds: dict[int, set[float]] = defaultdict(set)
    stack = [tree["tree_structure"] for tree in booster.booster_.dump_model()["tree_info"]]
    while stack:
        node = stack.pop()
        if "split_index" in node:
            thresholds[int(node["split_feature"])].add(float(node["threshold"]))
            stack.extend([node["left_child"], node["right_child"]])
    return thresholds


def round_thresholds_down(converted: onnx.ModelProto, booster: lightgbm.LGBMClassifier) -> int:
    """
    Makes the ONNX trees split float32 inputs exactly as LightGBM does; returns how many thresholds moved.

    LightGBM splits on `x <= t` with a double t, often halfway between two float32 values, and the converter
    stores t as float32. When that rounds t up onto the next float32 value, an input equal to it goes left in
    ONNX and right in LightGBM. The largest float32 at or below t gives the same answer for every float32 x.
    """
    thresholds = _split_thresholds(booster)
    moved = 0
    for node in converted.graph.node:
        if not node.op_type.startswith("TreeEnsemble"):
            continue
        attributes = {attribute.name: attribute for attribute in node.attribute}
        features = list(attributes["nodes_featureids"].ints)
        modes = list(attributes["nodes_modes"].strings)
        values = attributes["nodes_values"].floats
        for index, value in enumerate(values):
            if modes[index] != b"BRANCH_LEQ":
                continue
            stored = np.float32(value)
            originals = [threshold for threshold in thresholds[features[index]] if np.float32(threshold) == stored]
            if originals and all(threshold < float(stored) for threshold in originals):
                values[index] = float(np.nextafter(stored, np.float32(-np.inf)))
                moved += 1
    return moved


def to_onnx(model: StudentModel, metadata: dict[str, str]) -> onnx.ModelProto:
    """
    One ONNX graph: float32 features [n, len(FEATURE_NAMES)] in, calibrated probability [n] out.

    The LightGBM converter gives class probabilities; the graph takes the profitable one, turns it back into
    the raw score (log odds) and applies the Platt scaling.
    """
    converted = onnxmltools.convert_lightgbm(
        model.booster,
        initial_types=[(ONNX_INPUT, FloatTensorType([None, len(FEATURE_NAMES)]))],
        zipmap=False,
        target_opset=ONNX_TARGET_OPSET,
    )
    round_thresholds_down(converted, model.booster)
    graph = converted.graph
    graph.name = REGISTERED_MODEL  # The converter names the graph with a random ID; the same model gives the same bytes.
    probabilities = next(output.name for output in graph.output if output.name != "label")
    graph.initializer.extend(
        [
            helper.make_tensor("student_profitable_class", TensorProto.INT64, [], [1]),
            helper.make_tensor("student_one", TensorProto.FLOAT, [], [1.0]),
            helper.make_tensor("student_platt_slope", TensorProto.FLOAT, [], [model.platt_slope]),
            helper.make_tensor("student_platt_intercept", TensorProto.FLOAT, [], [model.platt_intercept]),
        ]
    )
    graph.node.extend(
        [
            helper.make_node("Gather", [probabilities, "student_profitable_class"], ["student_p_profitable"], axis=1),
            helper.make_node("Sub", ["student_one", "student_p_profitable"], ["student_p_other"]),
            helper.make_node("Div", ["student_p_profitable", "student_p_other"], ["student_odds"]),
            helper.make_node("Log", ["student_odds"], ["student_log_odds"]),
            helper.make_node("Mul", ["student_log_odds", "student_platt_slope"], ["student_scaled"]),
            helper.make_node("Add", ["student_scaled", "student_platt_intercept"], ["student_calibrated_log_odds"]),
            helper.make_node("Sigmoid", ["student_calibrated_log_odds"], [ONNX_OUTPUT]),
        ]
    )
    del graph.output[:]
    graph.output.append(helper.make_tensor_value_info(ONNX_OUTPUT, TensorProto.FLOAT, [None]))
    for key, value in sorted(metadata.items()):
        entry = converted.metadata_props.add()
        entry.key, entry.value = key, value
    onnx.checker.check_model(converted)
    return converted


def check_onnx(onnx_model: onnx.ModelProto, model: StudentModel, features: np.ndarray) -> float:
    """Checks the input and output signature and that ONNX matches the native model; returns the largest gap."""
    session = onnxruntime.InferenceSession(onnx_model.SerializeToString(), providers=["CPUExecutionProvider"])
    inputs = session.get_inputs()
    outputs = session.get_outputs()
    if [item.name for item in inputs] != [ONNX_INPUT] or inputs[0].shape[1] != len(FEATURE_NAMES):
        raise ValueError(f"Unexpected ONNX input signature: {[(item.name, item.shape) for item in inputs]}")
    if [item.name for item in outputs] != [ONNX_OUTPUT]:
        raise ValueError(f"Unexpected ONNX output signature: {[item.name for item in outputs]}")
    onnx_probabilities = np.asarray(session.run([ONNX_OUTPUT], {ONNX_INPUT: features})[0], dtype=np.float64)
    gap = float(np.max(np.abs(onnx_probabilities - model.predict(features)))) if len(features) else 0.0
    if not gap <= ONNX_PARITY_TOLERANCE:
        raise ValueError(f"ONNX and native predictions differ by {gap}, more than {ONNX_PARITY_TOLERANCE}")
    return gap


# --- Evaluation ---


def as_trades(rows: Sequence[LabeledListing], rule: str) -> list[Trade]:
    """Listings as scorecard trades with their label, so the #249 metrics apply unchanged."""
    trades = []
    for row in rows:
        trade = Trade(
            decided_at=row.decided_at,
            tick_kind=TICK_KIND_LISTING,
            listing_id=row.listing_id,
            market_hash_name=row.market_hash_name,
            price_cents=row.price_cents,
            rule=rule,
            z_source=None,
            build_id=None,
        )
        set_outcome(trade, row.is_profitable, row.net_margin_cents)
        trades.append(trade)
    return trades


def calibration_curve(probabilities: np.ndarray, actual: np.ndarray) -> list[dict[str, float | int]]:
    """Mean predicted probability and observed profitable share in equal width probability bins."""
    curve = []
    bins = np.minimum((probabilities * CALIBRATION_BINS).astype(int), CALIBRATION_BINS - 1)
    for index in range(CALIBRATION_BINS):
        in_bin = bins == index
        count = int(in_bin.sum())
        if count:
            curve.append(
                {
                    "bin": index,
                    "count": count,
                    "mean_predicted": float(probabilities[in_bin].mean()),
                    "observed": float(actual[in_bin].mean()),
                }
            )
    return curve


def ranking_metrics(probabilities: np.ndarray, actual: np.ndarray) -> dict[str, Any]:
    both_classes = len(set(actual.tolist())) == 2
    return {
        "roc_auc": float(roc_auc_score(actual, probabilities)) if both_classes else None,
        "pr_auc": float(average_precision_score(actual, probabilities)) if both_classes else None,
        "brier": float(brier_score_loss(actual, probabilities)) if len(actual) else None,
        "base_rate": float(actual.mean()) if len(actual) else None,
        "calibration": calibration_curve(probabilities, actual),
    }


def compare(rows: Sequence[LabeledListing], probabilities: np.ndarray, model_approves: Sequence[bool]) -> dict[str, Any]:
    """The live rules, the model at its threshold, and the model at the rules' volume, on the same listings.

    `model_approves` says which listings the model approved at its operating threshold. At the rules' volume
    the model takes as many listings as the rules approved, the most probable first."""
    rules = [row for row in rows if row.rules_approved]
    at_threshold = [row for row, approved in zip(rows, model_approves, strict=True) if approved]
    order = np.argsort(-probabilities, kind="stable")
    equal_volume = [rows[int(index)] for index in order[: len(rules)]]
    return {
        "listings": len(rows),
        "rules": score_trades(as_trades(rules, "rules")),
        "model_at_threshold": score_trades(as_trades(at_threshold, "student")),
        "model_at_rules_volume": score_trades(as_trades(equal_volume, "student")),
    }


def latest_price_ratio(rows: Sequence[LabeledListing]) -> np.ndarray:
    """
    The baseline's latest price over the price of each listing, the ranking that beat the CFO in #250; 0, the
    lowest, without a baseline price.
    """
    column = FEATURE_NAMES.index("price_to_latest")
    ratios = []
    for row in rows:
        price_to_latest = row.features[column]
        ratios.append(1.0 / price_to_latest if price_to_latest > 0 else 0.0)  # NaN compares False
    return np.array(ratios, dtype=np.float64)


def _roc_auc(scores: np.ndarray, actual: np.ndarray) -> float | None:
    return float(roc_auc_score(actual, scores)) if len(set(actual.tolist())) == 2 else None


def _top(rows: Sequence[LabeledListing], scores: np.ndarray, count: int) -> list[LabeledListing]:
    order = np.argsort(-scores, kind="stable")
    return [rows[int(index)] for index in order[:count]]


def compare_on_dre_approvals(rows: Sequence[LabeledListing], probabilities: np.ndarray) -> dict[str, Any]:
    """
    How the model ranks the DRE's own deals (#333), next to the simple rules, on the same listings.

    The DRE's deals are the listings it approved before the fee floor (#331). The fee floor keeps the ones the
    live rules approved; the ratio of the latest baseline price to the price, and the model, each keep as many,
    the highest first, so the three are compared at the same number of trades.
    """
    indices = [index for index, row in enumerate(rows) if row.dre_approved]
    approvals = [rows[index] for index in indices]
    approval_probabilities = probabilities[indices] if indices else np.array([], dtype=np.float64)
    ratios = latest_price_ratio(approvals)
    actual = outcomes(approvals)
    fee_floor = [row for row in approvals if row.rules_approved]
    return {
        "approvals": len(approvals),
        "model_roc_auc": _roc_auc(approval_probabilities, actual),
        "ratio_roc_auc": _roc_auc(ratios, actual),
        "dre": score_trades(as_trades(approvals, "dre")),
        "fee_floor": score_trades(as_trades(fee_floor, "fee_floor")),
        "ratio_at_fee_floor_volume": score_trades(as_trades(_top(approvals, ratios, len(fee_floor)), "ratio")),
        "model_at_fee_floor_volume": score_trades(
            as_trades(_top(approvals, approval_probabilities, len(fee_floor)), "student")
        ),
    }


def _total(cell: dict[str, Any]) -> float | None:
    return cell["net_pnl_cents"]["total"] if cell["status"] == "ok" else None


def _precision(cell: dict[str, Any]) -> float | None:
    return cell["precision"]["value"] if cell["status"] == "ok" else None


def candidate_verdict(comparison: dict[str, Any], exploratory: bool) -> tuple[bool, str]:
    """
    A model is a candidate only when, out of sample, it makes more net P&L than the rules at its threshold,
    or is more precise than the rules at the rules' volume, with enough labeled trades on both sides.
    """
    if exploratory:
        return False, "exploratory run: labels overlap the validation windows and include revisable ones"
    rules = comparison["rules"]
    if rules["status"] != "ok":
        return False, f"the rules have {rules['labeled']} labeled trades out of sample, fewer than {MIN_SAMPLE}"
    reasons = []
    model_total, rules_total = _total(comparison["model_at_threshold"]), _total(rules)
    if model_total is not None and rules_total is not None and model_total > rules_total:
        reasons.append(f"net P&L {dollars(model_total)} against {dollars(rules_total)} for the rules")
    model_precision, rules_precision = _precision(comparison["model_at_rules_volume"]), _precision(rules)
    if model_precision is not None and rules_precision is not None and model_precision > rules_precision:
        reasons.append(f"precision {percent(model_precision)} against {percent(rules_precision)} at the rules' volume")
    if reasons:
        return True, "beats the rules out of sample: " + "; ".join(reasons)
    return False, "does not beat the rules out of sample on net P&L or on precision at equal volume"


# --- The run ---


@dataclass(frozen=True)
class TrainingInputs:
    log: DecisionLog
    labels: FinalLabels
    fold_hours: int = DEFAULT_FOLD_HOURS
    exploratory: bool = False


@dataclass
class TrainingResult:
    report: dict[str, Any]
    model: StudentModel | None = None
    onnx_model: onnx.ModelProto | None = None
    notes: list[str] = field(default_factory=list)


def evaluate_folds(
    rows: Sequence[LabeledListing], fold_hours: int, exploratory: bool
) -> tuple[list[dict[str, Any]], list[LabeledListing], np.ndarray, list[bool]]:
    """Trains one model per fold. Returns the fold reports and, pooled over the evaluated folds, the out of
    sample listings, their predictions, and whether each fold's model approved them at its threshold."""
    fold_reports: list[dict[str, Any]] = []
    pooled_rows: list[LabeledListing] = []
    pooled_probabilities: list[float] = []
    pooled_approves: list[bool] = []
    for fold in walk_forward_folds(rows, fold_hours, exploratory):
        check_no_look_ahead(fold, exploratory)
        report: dict[str, Any] = {
            "cutoff": fold.cutoff.isoformat(),
            "end": fold.end.isoformat(),
            "train_rows": len(fold.train),
            "train_profitable": sum(1 for row in fold.train if row.is_profitable),
            "validation_rows": len(fold.validation),
        }
        too_small = size_rule(fold.train)
        if too_small is not None:
            report["skipped"] = too_small
            fold_reports.append(report)
            continue
        model = train_model(fold.train)
        probabilities = model.predict(feature_matrix(fold.validation))
        report["threshold"] = model.threshold
        report["metrics"] = ranking_metrics(probabilities, outcomes(fold.validation))
        approves = (probabilities >= model.threshold).tolist()
        report["comparison"] = compare(fold.validation, probabilities, approves)
        fold_reports.append(report)
        pooled_rows.extend(fold.validation)
        pooled_probabilities.extend(probabilities.tolist())
        pooled_approves.extend(approves)
    return fold_reports, pooled_rows, np.array(pooled_probabilities, dtype=np.float64), pooled_approves


def train_student(inputs: TrainingInputs) -> TrainingResult:
    log, labels = inputs.log, inputs.labels
    rows = build_dataset(log, labels)
    notes: list[str] = []
    if inputs.exploratory:
        notes.append(
            "Exploratory run: it reads labels that are not final and lets training labels overlap the validation "
            "windows. Its numbers check the pipeline; they are not an evaluation, and no model is registered."
        )
    if not log.config.get("fee_floor"):
        notes.append(
            "The log was replayed without the fee floor (#331), so the fee floor row on the DRE's deals holds every "
            "DRE approval."
        )

    fold_reports, pooled_rows, pooled_probabilities, pooled_approves = evaluate_folds(
        rows, inputs.fold_hours, inputs.exploratory
    )
    evaluated = [fold for fold in fold_reports if "skipped" not in fold]
    pooled: dict[str, Any] | None = None
    candidate, verdict = False, "no fold had enough training data"
    if evaluated:
        pooled = {
            "metrics": ranking_metrics(pooled_probabilities, outcomes(pooled_rows)),
            "comparison": compare(pooled_rows, pooled_probabilities, pooled_approves),
            "dre_approvals": compare_on_dre_approvals(pooled_rows, pooled_probabilities),
        }
        candidate, verdict = candidate_verdict(pooled["comparison"], inputs.exploratory)
    else:
        notes.append("No fold had enough training listings to train on; see the size rule.")

    final_rule = size_rule(rows)
    model: StudentModel | None = None
    onnx_model: onnx.ModelProto | None = None
    onnx_gap: float | None = None
    if final_rule is None:
        model = train_model(rows)
        onnx_model = to_onnx(model, _onnx_metadata(labels, model, candidate))
        onnx_gap = check_onnx(onnx_model, model, feature_matrix(rows))
    else:
        notes.append(f"No final model: {final_rule}.")

    registrable = model is not None and not inputs.exploratory and bool(evaluated)
    report = {
        "report_version": REPORT_VERSION,
        "as_of": labels.as_of.isoformat(),
        "git_commit": git_commit(),
        "exploratory": inputs.exploratory,
        "data": {
            "venue": log.venue,
            "start": scorable_start(log).isoformat(),
            "end": log.end.isoformat(),
            "decision_log_sha256": log.sha256,
            "strategy": log.strategy,
            "listings": len(rows),
            "profitable": sum(1 for row in rows if row.is_profitable),
            "first_decision": min(row.decided_at for row in rows).isoformat() if rows else None,
            "last_decision": max(row.decided_at for row in rows).isoformat() if rows else None,
        },
        "labels": {"version": labels.version, "config": labels.contract, "final_only": not inputs.exploratory},
        "features": {"version": FEATURE_VERSION, "names": list(FEATURE_NAMES)},
        "model": {
            "params": MODEL_PARAMS,
            "calibration_blocks": CALIBRATION_BLOCKS,
            "calibration_rows": model.calibration_rows if model else None,
            "platt_slope": model.platt_slope if model else None,
            "platt_intercept": model.platt_intercept if model else None,
            "threshold": model.threshold if model else None,
            "onnx_parity_max_gap": onnx_gap,
        },
        "rules": {
            "min_train_rows": MIN_TRAIN_ROWS,
            "min_minority_class": MIN_MINORITY_CLASS,
            "min_sample": MIN_SAMPLE,
            "fold_hours": inputs.fold_hours,
        },
        "folds": fold_reports,
        "pooled": pooled,
        "candidate": candidate,
        "verdict": verdict,
        "registrable": registrable,
        "notes": notes,
    }
    return TrainingResult(report=report, model=model, onnx_model=onnx_model, notes=notes)


def _onnx_metadata(labels: FinalLabels, model: StudentModel, candidate: bool) -> dict[str, str]:
    return {
        "feature_names": json.dumps(list(FEATURE_NAMES)),
        "feature_version": str(FEATURE_VERSION),
        "label_version": labels.version,
        "threshold": repr(model.threshold),
        "candidate": str(candidate).lower(),
    }


# --- Reports ---


def _cell_text(cell: dict[str, Any]) -> list[str]:
    if cell["status"] != "ok":
        return [str(cell["trades"]), str(cell["labeled"]), "insufficient data", "insufficient data"]
    precision = cell["precision"]
    pnl = cell["net_pnl_cents"]
    return [
        str(cell["trades"]),
        str(cell["labeled"]),
        f"{percent(precision['value'])} ({percent(precision['low'])} to {percent(precision['high'])})",
        f"{dollars(pnl['total'])}, {dollars(pnl['mean'])} per trade "
        f"({dollars(pnl['mean_low'])} to {dollars(pnl['mean_high'])})",
    ]


def _number(value: float | None, digits: int = 3) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def _data_line(data: dict[str, Any]) -> str:
    if not data["listings"]:
        return f"- Data: no scored {data['venue']} feed listing in the log has a profitable or unprofitable label yet."
    return (
        f"- Data: {data['venue']} feed listings decided from {data['first_decision']} to {data['last_decision']} UTC, "
        f"{data['listings']} with a profitable or unprofitable label ({data['profitable']} profitable). A listing "
        "whose first sighting came before the log starts has no features."
    )


def _dre_approvals_section(dre: dict[str, Any]) -> list[str]:
    lines = [
        "## On the DRE's deals",
        "",
        f"{dre['approvals']} of the out of sample listings were approved by the DRE before the fee floor (#331). "
        f"Among them, ROC-AUC {_number(dre['model_roc_auc'])} for the model and {_number(dre['ratio_roc_auc'])} for "
        "the ratio of the baseline's latest price to the price. The ratio and the model keep as many of the deals "
        "as the fee floor does, the highest first.",
        "",
        "| Who decides | Trades | Labeled | Precision (95% CI) | Net P&L |",
        "|---|---|---|---|---|",
    ]
    for title, key in (
        ("DRE without the fee floor", "dre"),
        ("Fee floor", "fee_floor"),
        ("Ratio at the fee floor's volume", "ratio_at_fee_floor_volume"),
        ("Model at the fee floor's volume", "model_at_fee_floor_volume"),
    ):
        lines.append("| " + " | ".join([title, *_cell_text(dre[key])]) + " |")
    lines.append("")
    return lines


def render_markdown(report: dict[str, Any]) -> str:
    data = report["data"]
    lines = ["# Student model", ""]
    lines.append(
        "A LightGBM model of whether a feed listing is profitable, trained on outcome labels with walk forward "
        "validation and compared with the live rules on the same out of sample listings (#234). "
        "Generated by `apps/analytics/train_student.py`."
    )
    lines.append("")
    for note in report["notes"]:
        lines.extend([f"> {note}", ""])
    lines.extend(
        [
            "## Run",
            "",
            _data_line(data),
            f"- Labels: `{report['labels']['version']}` as of {report['as_of']} UTC, "
            + ("final labels only." if report["labels"]["final_only"] else "including labels that may still be revised."),
            f"- Features: version {report['features']['version']}, "
            + ", ".join(f"`{name}`" for name in report["features"]["names"])
            + ".",
            f"- Folds every {report['rules']['fold_hours']} hours. A fold needs {report['rules']['min_train_rows']} training "
            f"listings and {report['rules']['min_minority_class']} of the rarer outcome; a metric needs "
            f"{report['rules']['min_sample']} labeled trades.",
            f"- Decision log sha256 `{data['decision_log_sha256']}`, commit `{report['git_commit']}`.",
            "",
            f"**Verdict:** {'candidate' if report['candidate'] else 'not a candidate'}: {report['verdict']}.",
            "",
        ]
    )
    pooled = report["pooled"]
    if pooled is not None:
        metrics = pooled["metrics"]
        comparison = pooled["comparison"]
        lines.extend(
            [
                "## Out of sample, all folds",
                "",
                f"{comparison['listings']} listings. ROC-AUC {_number(metrics['roc_auc'])}, "
                f"PR-AUC {_number(metrics['pr_auc'])} "
                f"(base rate {_number(metrics['base_rate'])}), Brier score {_number(metrics['brier'], 4)}.",
                "",
                "| Who decides | Trades | Labeled | Precision (95% CI) | Net P&L |",
                "|---|---|---|---|---|",
            ]
        )
        for title, key in (
            ("Live rules", "rules"),
            ("Model at its threshold", "model_at_threshold"),
            ("Model at the rules' volume", "model_at_rules_volume"),
        ):
            lines.append("| " + " | ".join([title, *_cell_text(comparison[key])]) + " |")
        lines.extend(["", "### Calibration", "", "| Predicted | Listings | Mean predicted | Observed |", "|---|---|---|---|"])
        for entry in metrics["calibration"]:
            low = entry["bin"] / CALIBRATION_BINS
            lines.append(
                f"| {low:.1f} to {low + 1 / CALIBRATION_BINS:.1f} | {entry['count']} "
                f"| {entry['mean_predicted']:.3f} | {entry['observed']:.3f} |"
            )
        lines.append("")
        lines.extend(_dre_approvals_section(pooled["dre_approvals"]))
    lines.extend(
        [
            "## Folds",
            "",
            "| Cutoff (UTC) | Training listings | Validation listings | ROC-AUC "
            "| Rules: trades, precision | Model: trades, precision | Note |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for fold in report["folds"]:
        if "skipped" in fold:
            lines.append(
                f"| {fold['cutoff']} | {fold['train_rows']} | {fold['validation_rows']} | | | | skipped: {fold['skipped']} |"
            )
            continue
        comparison = fold["comparison"]
        rules, model_cell = comparison["rules"], comparison["model_at_threshold"]
        lines.append(
            f"| {fold['cutoff']} | {fold['train_rows']} | {fold['validation_rows']} | {_number(fold['metrics']['roc_auc'])} "
            f"| {rules['trades']}, {_cell_text(rules)[2]} | {model_cell['trades']}, {_cell_text(model_cell)[2]} | |"
        )
    lines.append("")
    return "\n".join(lines)


def write_reports(report: dict[str, Any], onnx_model: onnx.ModelProto | None, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "student_model.json"
    markdown_path = out_dir / "student_model.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    markdown_path.write_text(render_markdown(report), encoding="utf-8", newline="\n")
    paths = [json_path, markdown_path]
    if onnx_model is not None:
        model_path = out_dir / "student_model.onnx"
        model_path.write_bytes(onnx_model.SerializeToString())
        paths.append(model_path)
    return paths


# --- MLflow ---


def mlflow_params(report: dict[str, Any]) -> dict[str, str]:
    params = {
        "venue": report["data"]["venue"],
        "decision_log_sha256": report["data"]["decision_log_sha256"],
        "as_of": report["as_of"],
        "git_commit": str(report["git_commit"]),
        "exploratory": str(report["exploratory"]).lower(),
        "feature_version": str(report["features"]["version"]),
        "feature_names": json.dumps(report["features"]["names"]),
        "fold_hours": str(report["rules"]["fold_hours"]),
        "min_train_rows": str(report["rules"]["min_train_rows"]),
        "min_minority_class": str(report["rules"]["min_minority_class"]),
        "label_final_only": str(report["labels"]["final_only"]).lower(),
    }
    params.update(label_mlflow_params(report["labels"]["version"], report["labels"]["config"]))
    params.update({f"model.{key}": str(value) for key, value in sorted(MODEL_PARAMS.items())})
    return params


def mlflow_metrics(report: dict[str, Any]) -> dict[str, float]:
    metrics: dict[str, float] = {
        "listings": report["data"]["listings"],
        "evaluated_folds": sum(1 for fold in report["folds"] if "skipped" not in fold),
        "candidate": float(report["candidate"]),
    }
    pooled = report["pooled"]
    if pooled is not None:
        for key in ("roc_auc", "pr_auc", "brier"):
            if pooled["metrics"][key] is not None:
                metrics[key] = pooled["metrics"][key]
        for side in ("rules", "model_at_threshold", "model_at_rules_volume"):
            cell = pooled["comparison"][side]
            metrics[f"{side}_trades"] = cell["trades"]
            if cell["status"] == "ok":
                metrics[f"{side}_precision"] = cell["precision"]["value"]
                metrics[f"{side}_net_pnl_total_cents"] = cell["net_pnl_cents"]["total"]
        dre = pooled["dre_approvals"]
        metrics["dre_approvals"] = dre["approvals"]
        for key in ("model_roc_auc", "ratio_roc_auc"):
            if dre[key] is not None:
                metrics[f"dre_{key}"] = dre[key]
        for side in ("fee_floor", "ratio_at_fee_floor_volume", "model_at_fee_floor_volume"):
            cell = dre[side]
            if cell["status"] == "ok":
                metrics[f"dre_{side}_net_pnl_total_cents"] = cell["net_pnl_cents"]["total"]
    if report["model"]["threshold"] is not None:
        metrics["threshold"] = report["model"]["threshold"]
    return metrics


def log_to_mlflow(report: dict[str, Any], artifacts: Sequence[Path]) -> tuple[str, str | None]:
    """Logs the run and, when the run is registrable, registers its ONNX model. Returns (run ID, model version)."""
    client = MlflowClient(tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT)
    experiment_id = experiment.experiment_id if experiment else client.create_experiment(MLFLOW_EXPERIMENT)
    run_name = f"student {report['data']['first_decision'] or ''}"[:250]
    tags = {"report_version": str(REPORT_VERSION), "candidate": str(report["candidate"]).lower()}
    run = client.create_run(experiment_id, run_name=run_name, tags=tags)
    run_id = run.info.run_id
    version: str | None = None
    try:
        for key, value in mlflow_params(report).items():
            client.log_param(run_id, key, value)
        for key, metric in mlflow_metrics(report).items():
            client.log_metric(run_id, key, float(metric))
        for artifact in artifacts:
            client.log_artifact(run_id, str(artifact), artifact_path="student")
        onnx_files = [artifact for artifact in artifacts if artifact.suffix == ".onnx"]
        if report["registrable"] and onnx_files:
            if not client.search_registered_models(filter_string=f"name = '{REGISTERED_MODEL}'"):
                client.create_registered_model(REGISTERED_MODEL)
            model_version = client.create_model_version(
                REGISTERED_MODEL,
                source=f"{run.info.artifact_uri}/student/{onnx_files[0].name}",
                run_id=run_id,
                tags={"candidate": str(report["candidate"]).lower(), "label_version": report["labels"]["version"]},
            )
            version = str(model_version.version)
    except Exception:
        client.set_terminated(run_id, status="FAILED")
        raise
    client.set_terminated(run_id, status="FINISHED")
    return run_id, version


# --- Flow and CLI ---


@flow(name="student-model-training")
async def train_student_flow(
    decisions: str,
    label_version: str = LABEL_VERSION,
    as_of: datetime | None = None,
    fold_hours: int = DEFAULT_FOLD_HOURS,
    exploratory: bool = False,
    out_dir: str | None = None,
    use_mlflow: bool = True,
) -> dict[str, Any]:
    """Reads the decision log and the labels, trains and evaluates the student model, writes the report."""
    as_of = as_of or utc_now_naive()
    log = read_decision_log(Path(decisions))
    labels = await fetch_final_labels(label_version, as_of, include_revisable=exploratory)
    result = train_student(TrainingInputs(log=log, labels=labels, fold_hours=fold_hours, exploratory=exploratory))
    report = result.report
    paths = write_reports(report, result.onnx_model, Path(out_dir) if out_dir else REPO_ROOT / "docs" / "benchmarks")
    logger.info(
        "[STUDENT] %d labeled listings, %d evaluated folds, %s: %s",
        report["data"]["listings"],
        sum(1 for fold in report["folds"] if "skipped" not in fold),
        "candidate" if report["candidate"] else "not a candidate",
        report["verdict"],
    )
    for note in report["notes"]:
        logger.warning("[STUDENT] %s", note)
    if use_mlflow:
        run_id, version = log_to_mlflow(report, paths)
        logger.info(
            "[STUDENT] Logged MLflow run %s%s",
            run_id,
            f", registered {REGISTERED_MODEL} version {version}" if version else ", nothing registered",
        )
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train and evaluate the student model on outcome labels (#234).")
    parser.add_argument("--decisions", type=Path, required=True, help="Decision log made with the zscore_dre_features strategy")
    parser.add_argument("--label-version", default=LABEL_VERSION, help=f"Label version (default {LABEL_VERSION})")
    parser.add_argument("--as-of", type=parse_utc, help="Read labels as of this time (ISO, UTC; default now)")
    parser.add_argument("--fold-hours", type=int, default=DEFAULT_FOLD_HOURS, help="Validation window of each fold")
    parser.add_argument("--out-dir", type=Path, help="Where to write the report and model (default docs/benchmarks)")
    parser.add_argument(
        "--exploratory",
        action="store_true",
        help="Also use labels that are not final and let training labels overlap validation (never registers)",
    )
    parser.add_argument("--no-mlflow", action="store_true", help="Write the report without logging an MLflow run")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    asyncio.run(
        train_student_flow(
            decisions=str(args.decisions),
            label_version=args.label_version,
            as_of=args.as_of,
            fold_hours=args.fold_hours,
            exploratory=args.exploratory,
            out_dir=str(args.out_dir) if args.out_dir else None,
            use_mlflow=not args.no_mlflow,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    from shared_utils import validate_required_env

    validate_required_env(["DATABASE_URL"])
    sys.exit(main())
