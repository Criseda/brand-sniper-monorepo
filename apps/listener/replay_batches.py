import argparse
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from batch_delivery import RedisBatchStore, send_batch_with_retry
from dotenv import load_dotenv
from shared_utils import backend_api_headers, get_logger

project_root = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=project_root / ".env")
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logger = get_logger("listener.replay_batches")


@dataclass(frozen=True, slots=True)
class ReplayResult:
    attempted: int
    succeeded: int
    failed: int

    @property
    def exit_code(self) -> int:
        return 1 if self.failed else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay listener batches from the Redis dead-letter stream.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum batches to replay")
    return parser.parse_args()


async def replay(limit: int) -> ReplayResult:
    if limit < 1:
        raise ValueError("--limit must be at least 1")

    compute_host = os.getenv("COMPUTE_NODE_IP", "localhost")
    compute_port = os.getenv("COMPUTE_NODE_PORT", "8080")
    url = f"http://{compute_host}:{compute_port}/api/v1/ingest/bulk"
    redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    redis_password = os.getenv("REDIS_PASSWORD")
    max_attempts = int(os.getenv("LISTENER_BATCH_MAX_ATTEMPTS", "5"))
    base_delay = float(os.getenv("LISTENER_BATCH_RETRY_BASE_SECONDS", "0.5"))
    max_delay = float(os.getenv("LISTENER_BATCH_RETRY_MAX_SECONDS", "8"))
    attempted = 0
    succeeded = 0
    failed = 0

    async with (
        RedisBatchStore.from_url(redis_url, password=redis_password) as store,
        aiohttp.ClientSession(headers=backend_api_headers()) as session,
    ):

        async def get_session() -> aiohttp.ClientSession:
            return session

        async for batch in store.iter_dead_letters():
            attempted += 1
            try:
                await send_batch_with_retry(
                    batch,
                    url=url,
                    session_factory=get_session,
                    max_attempts=max_attempts,
                    base_delay_seconds=base_delay,
                    max_delay_seconds=max_delay,
                )
                await store.acknowledge_dead_letter(batch.record_id)
            except Exception as exc:
                failed += 1
                logger.error("[BATCH FLUSH] Replay failed for batch %s: %s", batch.batch_id, exc)
            else:
                succeeded += 1
            if attempted >= limit:
                break

    logger.info(
        "[BATCH FLUSH] Replay complete: %d attempted, %d succeeded, %d failed.",
        attempted,
        succeeded,
        failed,
    )
    return ReplayResult(attempted=attempted, succeeded=succeeded, failed=failed)


def main() -> int:
    arguments = parse_args()
    result = asyncio.run(replay(arguments.limit))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
