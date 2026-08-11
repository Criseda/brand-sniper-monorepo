import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer


def load_listener_main():
    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("listener_main_health_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load listener main module for health tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


listener_main = load_listener_main()
create_listener_health_app = listener_main.create_listener_health_app
start_sidecar_process = listener_main.start_sidecar_process


@pytest.mark.asyncio
async def test_listener_bulk_client_uses_api_key():
    session = await listener_main.get_http_session()
    try:
        assert session.headers["X-API-Key"] == "listener-test-key-that-is-at-least-32-characters"
    finally:
        await listener_main.close_http_session()


@pytest.mark.asyncio
async def test_listener_health_runs_on_asyncio_http_server():
    async with TestClient(TestServer(create_listener_health_app())) as client:
        response = await client.get("/health")
        assert response.status == 200
        assert await response.json() == {"status": "healthy"}


class _EmptyStream:
    async def readline(self) -> bytes:
        return b""


class _ExitedProcess:
    def __init__(self, returncode: int):
        self.returncode = returncode
        self.stdout = _EmptyStream()
        self.stderr = _EmptyStream()

    async def wait(self) -> int:
        return self.returncode


@pytest.mark.asyncio
async def test_sidecar_exit_is_fatal(monkeypatch, tmp_path):
    sidecar_path = tmp_path / "sidecar.js"
    sidecar_path.touch()
    process = _ExitedProcess(returncode=7)

    async def create_process(*_args, **_kwargs):
        return process

    monkeypatch.setattr(listener_main.asyncio, "create_subprocess_exec", create_process)

    with pytest.raises(RuntimeError, match="exited unexpectedly with code 7"):
        await start_sidecar_process(SimpleNamespace(sidecar_script_path=sidecar_path))


@pytest.mark.asyncio
async def test_missing_sidecar_script_is_fatal(tmp_path):
    missing_path = tmp_path / "missing-sidecar.js"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        await start_sidecar_process(SimpleNamespace(sidecar_script_path=missing_path))


@pytest.mark.asyncio
async def test_sidecar_start_failure_is_fatal(monkeypatch, tmp_path):
    sidecar_path = tmp_path / "sidecar.js"
    sidecar_path.touch()

    async def fail_to_create_process(*_args, **_kwargs):
        raise OSError("node executable not found")

    monkeypatch.setattr(listener_main.asyncio, "create_subprocess_exec", fail_to_create_process)

    with pytest.raises(RuntimeError, match="sidecar failed"):
        await start_sidecar_process(SimpleNamespace(sidecar_script_path=sidecar_path))
