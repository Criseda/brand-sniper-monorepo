"""The listener's main module, loaded by its path."""

import importlib.util
import sys
from functools import cache
from pathlib import Path
from types import ModuleType

LISTENER_MAIN = Path(__file__).resolve().parents[1] / "main.py"
MODULE_NAME = "listener_main"


@cache
def listener_main() -> ModuleType:
    """
    apps/listener/main.py. A plain `import main` finds the backend's main.py under pytest, whose path lists
    apps/backend first, so the module is loaded from its file, once, under a name of its own.
    """
    spec = importlib.util.spec_from_file_location(MODULE_NAME, LISTENER_MAIN)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {LISTENER_MAIN}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module
