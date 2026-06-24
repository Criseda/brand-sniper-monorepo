import sys
from pathlib import Path

# Align paths dynamically to ensure shared_utils is discoverable
sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared_utils.db_connection import async_engine, async_session_maker

# Expose symbols with original names for backend compatibility
engine = async_engine
AsyncSessionLocal = async_session_maker