import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def _is_workspace_root(candidate: Path) -> bool:
    """Returns True when a directory looks like the monorepo root."""
    marker = candidate / "pyproject.toml"
    if not marker.is_file():
        return False
    try:
        return "[tool.uv.workspace]" in marker.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return False


def _find_repo_root(script_file: Path) -> Path:
    """Walks up from a script to the monorepo root.

    Prefers the uv workspace root (pyproject.toml containing
    [tool.uv.workspace]) so nested app/package manifests are skipped,
    falling back to a .git entry for checkouts without the manifest.
    Raises RuntimeError when no marker is found.
    """
    for candidate in script_file.parents:
        if _is_workspace_root(candidate):
            return candidate
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError(
        f"Could not locate repository root from '{script_file}': "
        "no parent directory contains a uv workspace pyproject.toml or a .git entry. "
        "Ensure the script lives under the monorepo checkout."
    )


def _configure_utf8_streams() -> None:
    """Forces standard streams to UTF-8 where the runtime supports it."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")


def _load_env_files(project_root: Path, app_dir: Path) -> None:
    """Loads the root .env (shared) first, then app-specific overrides."""
    load_dotenv(dotenv_path=project_root / ".env")
    load_dotenv(dotenv_path=app_dir / ".env", override=True)


def setup_script_environment(script_path: str | Path) -> Path:
    """Bootstraps a runnable app script: path alignment, UTF-8 streams, and .env loading.

    Locates the repository root by walking up from the script to the uv
    workspace root (falling back to the .git entry) instead of assuming a
    fixed directory depth. Loads the root .env (shared) first, then the
    app-specific .env with override, mirroring how the services do it.
    Returns the repository root. Raises RuntimeError when the root marker
    cannot be found.
    """
    script_file = Path(script_path).resolve()
    project_root = _find_repo_root(script_file)

    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    _configure_utf8_streams()
    _load_env_files(project_root, script_file.parent)

    return project_root


def setup_service_environment(service_path: str | Path) -> Path:
    """Bootstraps a long-lived service entrypoint: UTF-8 streams and .env loading.

    Shared by service daemons (uvicorn/asyncio) so they do not duplicate the
    dotenv and stream setup. Unlike setup_script_environment, the repository
    root is not added to sys.path: services resolve workspace packages via
    the installed environment and local modules via their own directory.
    Returns the repository root. Raises RuntimeError when the root marker
    cannot be found.
    """
    service_file = Path(service_path).resolve()
    project_root = _find_repo_root(service_file)

    _configure_utf8_streams()
    _load_env_files(project_root, service_file.parent)

    return project_root


def validate_required_env(required: list[str]) -> None:
    """Exits with a clear error when any required environment variable is missing."""
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        raise SystemExit(
            f"Missing required environment variable(s): {', '.join(missing)}. Add them to the root .env (see .env.example)."
        )
