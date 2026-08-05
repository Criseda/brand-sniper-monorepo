import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def setup_script_environment(script_path: str | Path) -> Path:
    """Bootstraps a runnable app script: path alignment, UTF-8 streams, and .env loading.

    Loads the root .env (shared) first, then the app-specific .env with override,
    mirroring how the services do it. Returns the repository root.
    """
    script_file = Path(script_path).resolve()
    project_root = script_file.parents[2]

    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    load_dotenv(dotenv_path=project_root / ".env")
    app_env = script_file.parent / ".env"
    load_dotenv(dotenv_path=app_env, override=True)

    return project_root


def validate_required_env(required: list[str]) -> None:
    """Exits with a clear error when any required environment variable is missing."""
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        raise SystemExit(
            f"Missing required environment variable(s): {', '.join(missing)}. Add them to the root .env (see .env.example)."
        )
