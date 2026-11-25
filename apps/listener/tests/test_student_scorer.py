import json
import math

import aiohttp
import numpy as np
import onnx
import pytest
from onnx import TensorProto, helper
from shared_utils import FEATURE_NAMES, FEATURE_VERSION
from student_scorer import (
    StudentModelError,
    StudentScorer,
    artifact_download_url,
    fetch_registered_model,
)

# One weight per feature, all different, so a feature fed in the wrong place changes the score.
WEIGHTS = np.array([0.1 * (index + 1) * (-1) ** index for index in range(len(FEATURE_NAMES))], dtype=np.float32)
BIAS = -0.3


def logistic_graph(metadata: dict[str, str] | None = None, input_name="features", output_name="probability") -> bytes:
    """A stand-in for the exported student: probability = sigmoid(features . WEIGHTS + BIAS), same signature."""
    weights = helper.make_tensor("weights", TensorProto.FLOAT, [len(FEATURE_NAMES), 1], WEIGHTS.tolist())
    bias = helper.make_tensor("bias", TensorProto.FLOAT, [1], [BIAS])
    shape = helper.make_tensor("shape", TensorProto.INT64, [1], [-1])
    graph = helper.make_graph(
        [
            helper.make_node("MatMul", [input_name, "weights"], ["log_odds_column"]),
            helper.make_node("Add", ["log_odds_column", "bias"], ["shifted"]),
            helper.make_node("Reshape", ["shifted", "shape"], ["log_odds"]),
            helper.make_node("Sigmoid", ["log_odds"], [output_name]),
        ],
        "student",
        [helper.make_tensor_value_info(input_name, TensorProto.FLOAT, [None, len(FEATURE_NAMES)])],
        [helper.make_tensor_value_info(output_name, TensorProto.FLOAT, [None])],
        initializer=[weights, bias, shape],
    )
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 15)])
    model.ir_version = 8
    for key, value in (metadata if metadata is not None else student_metadata()).items():
        entry = model.metadata_props.add()
        entry.key, entry.value = key, value
    onnx.checker.check_model(model)
    return model.SerializeToString()


def student_metadata(**overrides) -> dict[str, str]:
    metadata = {
        "feature_names": json.dumps(list(FEATURE_NAMES)),
        "feature_version": str(FEATURE_VERSION),
        "label_version": "v1",
        "threshold": "0.6",
        "candidate": "false",
    }
    metadata.update(overrides)
    return metadata


def features(seed: int) -> dict[str, float]:
    return {name: round(0.37 * (index + 1) + seed, 3) for index, name in enumerate(FEATURE_NAMES)}


def test_scores_match_the_graph_on_the_same_feature_row():
    scorer = StudentScorer.from_bytes(logistic_graph(), "brand-sniper-student/7")

    for seed in range(3):
        row = features(seed)
        expected = 1 / (1 + math.exp(-(float(np.dot([row[name] for name in FEATURE_NAMES], WEIGHTS)) + BIAS)))
        score = scorer.score(row)
        assert score.probability == pytest.approx(expected, abs=1e-5)
        assert score.threshold == 0.6
        assert score.would_approve is (score.probability >= 0.6)
    assert (scorer.model_version, scorer.feature_version) == ("brand-sniper-student/7", FEATURE_VERSION)


@pytest.mark.parametrize(
    ("metadata", "message"),
    [
        (student_metadata(feature_version=str(FEATURE_VERSION - 1)), "trained on feature version"),
        (student_metadata(feature_names=json.dumps(list(reversed(FEATURE_NAMES)))), "trained on feature version"),
        (student_metadata(threshold="1.5"), "outside 0 to 1"),
        ({"feature_version": str(FEATURE_VERSION)}, "no usable feature metadata"),
        (student_metadata(threshold="high"), "no usable feature metadata"),
    ],
    ids=["old_feature_version", "other_feature_order", "threshold_out_of_range", "no_names", "bad_threshold"],
)
def test_a_model_trained_on_other_features_is_refused(metadata, message):
    with pytest.raises(StudentModelError, match=message):
        StudentScorer.from_bytes(logistic_graph(metadata), "brand-sniper-student/1")


def test_a_corrupt_graph_is_refused():
    with pytest.raises(StudentModelError, match="cannot load"):
        StudentScorer.from_bytes(b"not an onnx graph", "brand-sniper-student/1")


def test_artifact_url_comes_from_the_registered_source():
    url = artifact_download_url("http://mlflow-server:5000/", "mlflow-artifacts:/4/run/artifacts/student/student_model.onnx")

    assert url == "http://mlflow-server:5000/api/2.0/mlflow-artifacts/artifacts/4/run/artifacts/student/student_model.onnx"
    with pytest.raises(StudentModelError, match="expected mlflow-artifacts"):
        artifact_download_url("http://mlflow-server:5000", "s3://bucket/model.onnx")


class FakeResponse:
    def __init__(self, status=200, body=None, content=b""):
        self.status, self._body, self._content = status, body, content

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._body

    async def read(self):
        return self._content


class FakeSession:
    def __init__(self, responses):
        self.responses, self.requests = list(responses), []

    def get(self, url, params=None, timeout=None):
        self.requests.append((url, params))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.mark.asyncio
async def test_fetch_registered_model_looks_up_the_version_then_downloads_its_artifact():
    source = "mlflow-artifacts:/4/run/artifacts/student/student_model.onnx"
    session = FakeSession([FakeResponse(body={"model_version": {"source": source}}), FakeResponse(content=b"graph")])

    assert await fetch_registered_model(session, "http://mlflow:5000", "brand-sniper-student", "3") == b"graph"
    assert session.requests == [
        ("http://mlflow:5000/api/2.0/mlflow/model-versions/get", {"name": "brand-sniper-student", "version": "3"}),
        ("http://mlflow:5000/api/2.0/mlflow-artifacts/artifacts/4/run/artifacts/student/student_model.onnx", None),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("responses", "message"),
    [
        ([FakeResponse(status=404)], "has no model"),
        ([FakeResponse(body={"model_version": {"source": "mlflow-artifacts:/x"}}), FakeResponse(status=500)], "download"),
        ([FakeResponse(body={})], "cannot fetch"),
        ([aiohttp.ClientConnectionError("refused")], "cannot fetch"),
    ],
    ids=["unknown_version", "artifact_missing", "malformed_answer", "unreachable"],
)
async def test_fetch_registered_model_fails_with_a_model_error(responses, message):
    with pytest.raises(StudentModelError, match=message):
        await fetch_registered_model(FakeSession(responses), "http://mlflow:5000", "brand-sniper-student", "3")
