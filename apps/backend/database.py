from shared_utils.db_connection import async_engine, async_session_maker

engine = async_engine
AsyncSessionLocal = async_session_maker
