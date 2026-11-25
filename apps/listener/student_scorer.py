"""
The student model (#234) inside the listener: an ONNX graph scored in process (#236).

`StudentScorer` loads the graph that `apps/analytics/train_student.py` exported and registered in MLflow, checks
that it was trained on the features this code computes (`shared_utils.features`), and turns one listing's
features into the calibrated probability that it is profitable. It never decides anything itself: the shadow
mode in `student_shadow.py` records what it would have decided while the rules keep deciding.

The registered graph is fetched over MLflow's REST API with aiohttp (`fetch_registered_model`), so the listener
image needs no MLflow client.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import aiohttp
import numpy as np
import onnxruntime
from shared_utils import FEATURE_NAMES, FEATURE_VERSION, feature_vector

# The graph's input and output, as `train_student.to_onnx` names them.
ONNX_INPUT = "features"
ONNX_OUTPUT = "probability"
MLFLOW_ARTIFACTS_SCHEME = "mlflow-artifacts:/"
MLFLOW_REQUEST_TIMEOUT_SECONDS = 30


class StudentModelError(RuntimeError):
    """The model cannot be used: missing, unreadable, or trained on other features."""


@dataclass(frozen=True)
class StudentScore:
    probability: float
    threshold: float

    @property
    def would_approve(self) -> bool:
        return self.probability >= self.threshold


class StudentScorer:
    """One loaded student graph. Scoring is synchronous and takes well under a millisecond."""

    def __init__(self, session: onnxruntime.InferenceSession, model_version: str, threshold: float) -> None:
        self._session = session
        self.model_version = model_version
        self.threshold = threshold
        self.feature_version = FEATURE_VERSION

    @classmethod
    def from_bytes(cls, model_bytes: bytes, model_version: str) -> "StudentScorer":
        """Loads a graph and checks its metadata against this code's features. Raises StudentModelError."""
        try:
            options = onnxruntime.SessionOptions()
            # One listing at a time on the event loop's thread: more threads only add scheduling overhead.
            options.intra_op_num_threads = 1
            options.inter_op_num_threads = 1
            session = onnxruntime.InferenceSession(model_bytes, options, providers=["CPUExecutionProvider"])
        except Exception as error:
            raise StudentModelError(f"cannot load the ONNX graph of {model_version}: {error}") from error
        metadata = session.get_modelmeta().custom_metadata_map
        try:
            feature_names = tuple(json.loads(metadata["feature_names"]))
            feature_version = int(metadata["feature_version"])
            threshold = float(metadata["threshold"])
        except (KeyError, ValueError, TypeError) as error:
            raise StudentModelError(f"{model_version} has no usable feature metadata: {error}") from error
        if feature_version != FEATURE_VERSION or feature_names != FEATURE_NAMES:
            raise StudentModelError(
                f"{model_version} was trained on feature version {feature_version} {list(feature_names)}, "
                f"this listener computes version {FEATURE_VERSION} {list(FEATURE_NAMES)}"
            )
        if not 0.0 <= threshold <= 1.0:
            raise StudentModelError(f"{model_version} has threshold {threshold}, outside 0 to 1")
        return cls(session, model_version, threshold)

    def score(self, features: Mapping[str, Any]) -> StudentScore:
        """The model's score of one listing's features (`shared_utils.compute_features`)."""
        row = np.array([feature_vector(features)], dtype=np.float32)
        probabilities = self._session.run([ONNX_OUTPUT], {ONNX_INPUT: row})[0]
        return StudentScore(probability=float(probabilities[0]), threshold=self.threshold)


def artifact_download_url(tracking_uri: str, source: str) -> str:
    """The HTTP URL of a registered model version's artifact, from its `mlflow-artifacts:/` source."""
    if not source.startswith(MLFLOW_ARTIFACTS_SCHEME):
        raise StudentModelError(f"cannot download a model stored at {source}; expected {MLFLOW_ARTIFACTS_SCHEME}")
    path = source.removeprefix(MLFLOW_ARTIFACTS_SCHEME).lstrip("/")
    return f"{tracking_uri.rstrip('/')}/api/2.0/mlflow-artifacts/artifacts/{quote(path)}"


async def fetch_registered_model(session: aiohttp.ClientSession, tracking_uri: str, name: str, version: str) -> bytes:
    """Downloads the ONNX graph of a registered model version. Raises StudentModelError."""
    timeout = aiohttp.ClientTimeout(total=MLFLOW_REQUEST_TIMEOUT_SECONDS)
    lookup = f"{tracking_uri.rstrip('/')}/api/2.0/mlflow/model-versions/get"
    try:
        async with session.get(lookup, params={"name": name, "version": version}, timeout=timeout) as response:
            if response.status != 200:
                raise StudentModelError(f"MLflow has no model {name} version {version} (HTTP {response.status})")
            source = (await response.json())["model_version"]["source"]
        async with session.get(artifact_download_url(tracking_uri, source), timeout=timeout) as response:
            if response.status != 200:
                raise StudentModelError(f"cannot download {name} version {version} (HTTP {response.status})")
            return await response.read()
    except (aiohttp.ClientError, TimeoutError, KeyError, ValueError) as error:
        raise StudentModelError(f"cannot fetch {name} version {version} from MLflow: {error}") from error
