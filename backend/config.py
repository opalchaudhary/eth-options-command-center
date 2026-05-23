import logging
import os
from pathlib import Path

from dotenv import load_dotenv


logger = logging.getLogger(__name__)

BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BACKEND_DIR.parent
PROJECT_ENV_PATH = PROJECT_ROOT / ".env"
BACKEND_ENV_PATH = BACKEND_DIR / ".env"


def _load_env_file(path):
    if not path.exists():
        return False

    load_dotenv(dotenv_path=path, override=False)
    return True


PROJECT_ENV_FOUND = _load_env_file(PROJECT_ENV_PATH)
BACKEND_ENV_FOUND = _load_env_file(BACKEND_ENV_PATH)


def _secret_from_streamlit(key):
    try:
        import streamlit as st

        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass

    return None


def get_config_value(key, default=None):
    value = os.getenv(key)

    if value not in [None, ""]:
        return value

    streamlit_value = _secret_from_streamlit(key)

    if streamlit_value not in [None, ""]:
        return str(streamlit_value)

    return default


def get_bool_config(key, default=False):
    value = get_config_value(key)

    if value in [None, ""]:
        return default

    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def get_int_config(key, default):
    value = get_config_value(key)

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


DELTA_API_KEY = get_config_value("DELTA_API_KEY")
DELTA_API_SECRET = get_config_value("DELTA_API_SECRET")
SUPABASE_URL = get_config_value("SUPABASE_URL")
SUPABASE_KEY = get_config_value("SUPABASE_KEY")
FASTAPI_BACKEND_URL = get_config_value("FASTAPI_BACKEND_URL", "http://127.0.0.1:8000")
USE_FASTAPI_BACKEND = get_bool_config("USE_FASTAPI_BACKEND", True)
FUTURES_LIVE_TRADING_ENABLED = get_bool_config("FUTURES_LIVE_TRADING_ENABLED", False)
BACKEND_SCHEDULER_ENABLED = get_bool_config("BACKEND_SCHEDULER_ENABLED", True)
BACKEND_JOB_TIMEOUT_SECONDS = get_int_config("BACKEND_JOB_TIMEOUT_SECONDS", 50)


def _sync_env_value(key, value):
    if value not in [None, ""]:
        os.environ.setdefault(key, str(value))


_sync_env_value("DELTA_API_KEY", DELTA_API_KEY)
_sync_env_value("DELTA_API_SECRET", DELTA_API_SECRET)
_sync_env_value("SUPABASE_URL", SUPABASE_URL)
_sync_env_value("SUPABASE_KEY", SUPABASE_KEY)
_sync_env_value("FASTAPI_BACKEND_URL", FASTAPI_BACKEND_URL)
_sync_env_value("USE_FASTAPI_BACKEND", str(USE_FASTAPI_BACKEND).lower())
_sync_env_value("FUTURES_LIVE_TRADING_ENABLED", str(FUTURES_LIVE_TRADING_ENABLED).lower())
_sync_env_value("BACKEND_SCHEDULER_ENABLED", str(BACKEND_SCHEDULER_ENABLED).lower())
_sync_env_value("BACKEND_JOB_TIMEOUT_SECONDS", str(BACKEND_JOB_TIMEOUT_SECONDS))


def delta_status():
    private_api_configured = bool(DELTA_API_KEY and DELTA_API_SECRET)

    return {
        "public_api_mode": True,
        "api_key_configured": bool(DELTA_API_KEY),
        "api_secret_configured": bool(DELTA_API_SECRET),
        "private_api_configured": private_api_configured,
        "private_trading_enabled": bool(
            FUTURES_LIVE_TRADING_ENABLED and private_api_configured
        ),
    }


def supabase_status():
    return {
        "url_configured": bool(SUPABASE_URL),
        "key_configured": bool(SUPABASE_KEY),
    }


def log_startup_config():
    logger.info(".env file found at project root: %s", PROJECT_ENV_FOUND)
    logger.info(".env file found in backend folder: %s", BACKEND_ENV_FOUND)
    logger.info("Delta API key loaded: %s", bool(DELTA_API_KEY))
    logger.info("Delta API secret loaded: %s", bool(DELTA_API_SECRET))
    logger.info("Delta public API mode: %s", True)
    logger.info("Delta private API configured: %s", bool(DELTA_API_KEY and DELTA_API_SECRET))
    logger.info("Supabase URL loaded: %s", bool(SUPABASE_URL))
    logger.info("Supabase key loaded: %s", bool(SUPABASE_KEY))
    logger.info("Use FastAPI backend: %s", USE_FASTAPI_BACKEND)
    logger.info("Backend scheduler enabled: %s", BACKEND_SCHEDULER_ENABLED)
    logger.info("Backend job timeout seconds: %s", BACKEND_JOB_TIMEOUT_SECONDS)
    logger.info(
        "Futures live trading enabled: %s",
        bool(FUTURES_LIVE_TRADING_ENABLED and DELTA_API_KEY and DELTA_API_SECRET),
    )
