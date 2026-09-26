from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

DEPLOYMENTS_DIR = Path(__file__).resolve().parents[3] / "deployments"


def test_migrations_have_a_single_head():
    """Two heads make `alembic upgrade head` refuse to run, so a new migration must revise the current head."""
    config = Config(str(DEPLOYMENTS_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(DEPLOYMENTS_DIR / "migrations"))

    heads = ScriptDirectory.from_config(config).get_heads()

    assert len(heads) == 1, f"Alembic has several heads: {heads}"
