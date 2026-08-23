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
MOBILE_API_TOKEN = get_config_value("MOBILE_API_TOKEN")
FASTAPI_BACKEND_URL = get_config_value("FASTAPI_BACKEND_URL", "http://127.0.0.1:8000")
USE_FASTAPI_BACKEND = get_bool_config("USE_FASTAPI_BACKEND", True)
BACKEND_SCHEDULER_ENABLED = get_bool_config("BACKEND_SCHEDULER_ENABLED", True)
BACKEND_JOB_TIMEOUT_SECONDS = get_int_config("BACKEND_JOB_TIMEOUT_SECONDS", 50)
MARKET_REFRESH_INTERVAL_SECONDS = get_int_config("MARKET_REFRESH_INTERVAL_SECONDS", 900)
OPTION_CHAIN_REFRESH_INTERVAL_SECONDS = get_int_config("OPTION_CHAIN_REFRESH_INTERVAL_SECONDS", 1800)
SMC_REFRESH_INTERVAL_SECONDS = get_int_config("SMC_REFRESH_INTERVAL_SECONDS", 1800)
VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS = get_int_config("VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS", 3600)
RETENTION_CLEANUP_INTERVAL_SECONDS = get_int_config("RETENTION_CLEANUP_INTERVAL_SECONDS", 3600)
OPTION_CHAIN_REFRESH_MAX_EXPIRIES = get_int_config("OPTION_CHAIN_REFRESH_MAX_EXPIRIES", 2)
SUPABASE_HISTORY_READ_LIMIT = get_int_config("SUPABASE_HISTORY_READ_LIMIT", 150)
PROBABILITY_ENGINE_ENABLED = get_bool_config("PROBABILITY_ENGINE_ENABLED", False)
PROBABILITY_RETENTION_ENABLED = get_bool_config("PROBABILITY_RETENTION_ENABLED", False)
RICH_DATA_COLLECTION_ENABLED = get_bool_config("RICH_DATA_COLLECTION_ENABLED", False)
RICH_ORDERFLOW_REST_ENABLED = get_bool_config("RICH_ORDERFLOW_REST_ENABLED", True)
RICH_ORDERFLOW_WS_ENABLED = get_bool_config("RICH_ORDERFLOW_WS_ENABLED", False)
RICH_OPTIONS_SURFACE_ENABLED = get_bool_config("RICH_OPTIONS_SURFACE_ENABLED", False)
RICH_DERIVATIVES_INTERVAL_SECONDS = get_int_config("RICH_DERIVATIVES_INTERVAL_SECONDS", 300)
RICH_ORDERFLOW_INTERVAL_SECONDS = get_int_config("RICH_ORDERFLOW_INTERVAL_SECONDS", 60)
RICH_ORDERBOOK_INTERVAL_SECONDS = get_int_config("RICH_ORDERBOOK_INTERVAL_SECONDS", 60)
RICH_OPTIONS_SURFACE_INTERVAL_SECONDS = get_int_config("RICH_OPTIONS_SURFACE_INTERVAL_SECONDS", 600)
RICH_ORDERFLOW_WS_URL = get_config_value("RICH_ORDERFLOW_WS_URL", "wss://public-socket.india.delta.exchange")
RICH_ORDERFLOW_WS_CHANNEL = get_config_value("RICH_ORDERFLOW_WS_CHANNEL", "trades")
RICH_ORDERFLOW_WS_SYMBOL = get_config_value("RICH_ORDERFLOW_WS_SYMBOL", "ETHUSD")


def delta_account_credentials():
    accounts = []

    if DELTA_API_KEY and DELTA_API_SECRET:
        accounts.append({
            "id": "main",
            "label": get_config_value("DELTA_MAIN_ACCOUNT_NAME", "Main Account"),
            "api_key": DELTA_API_KEY,
            "api_secret": DELTA_API_SECRET,
            "kind": "main",
        })

    for index in range(1, 11):
        api_key = (
            get_config_value(f"DELTA_SUBWALLET_{index}_API_KEY")
            or get_config_value(f"DELTA_SUBACCOUNT_{index}_API_KEY")
        )
        api_secret = (
            get_config_value(f"DELTA_SUBWALLET_{index}_API_SECRET")
            or get_config_value(f"DELTA_SUBACCOUNT_{index}_API_SECRET")
        )

        if not (api_key and api_secret):
            continue

        label = (
            get_config_value(f"DELTA_SUBWALLET_{index}_NAME")
            or get_config_value(f"DELTA_SUBACCOUNT_{index}_NAME")
            or f"Sub Wallet {index}"
        )

        accounts.append({
            "id": f"subwallet_{index}",
            "label": label,
            "api_key": api_key,
            "api_secret": api_secret,
            "kind": "subwallet",
        })

    return accounts


def _sync_env_value(key, value):
    if value not in [None, ""]:
        os.environ.setdefault(key, str(value))


_sync_env_value("DELTA_API_KEY", DELTA_API_KEY)
_sync_env_value("DELTA_API_SECRET", DELTA_API_SECRET)
_sync_env_value("SUPABASE_URL", SUPABASE_URL)
_sync_env_value("SUPABASE_KEY", SUPABASE_KEY)
_sync_env_value("MOBILE_API_TOKEN", MOBILE_API_TOKEN)
_sync_env_value("FASTAPI_BACKEND_URL", FASTAPI_BACKEND_URL)
_sync_env_value("USE_FASTAPI_BACKEND", str(USE_FASTAPI_BACKEND).lower())
_sync_env_value("BACKEND_SCHEDULER_ENABLED", str(BACKEND_SCHEDULER_ENABLED).lower())
_sync_env_value("BACKEND_JOB_TIMEOUT_SECONDS", str(BACKEND_JOB_TIMEOUT_SECONDS))
_sync_env_value("MARKET_REFRESH_INTERVAL_SECONDS", MARKET_REFRESH_INTERVAL_SECONDS)
_sync_env_value("OPTION_CHAIN_REFRESH_INTERVAL_SECONDS", OPTION_CHAIN_REFRESH_INTERVAL_SECONDS)
_sync_env_value("SMC_REFRESH_INTERVAL_SECONDS", SMC_REFRESH_INTERVAL_SECONDS)
_sync_env_value("VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS", VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS)
_sync_env_value("RETENTION_CLEANUP_INTERVAL_SECONDS", RETENTION_CLEANUP_INTERVAL_SECONDS)
_sync_env_value("OPTION_CHAIN_REFRESH_MAX_EXPIRIES", OPTION_CHAIN_REFRESH_MAX_EXPIRIES)
_sync_env_value("SUPABASE_HISTORY_READ_LIMIT", SUPABASE_HISTORY_READ_LIMIT)
_sync_env_value("PROBABILITY_ENGINE_ENABLED", str(PROBABILITY_ENGINE_ENABLED).lower())
_sync_env_value("PROBABILITY_RETENTION_ENABLED", str(PROBABILITY_RETENTION_ENABLED).lower())
_sync_env_value("RICH_DATA_COLLECTION_ENABLED", str(RICH_DATA_COLLECTION_ENABLED).lower())
_sync_env_value("RICH_ORDERFLOW_REST_ENABLED", str(RICH_ORDERFLOW_REST_ENABLED).lower())
_sync_env_value("RICH_ORDERFLOW_WS_ENABLED", str(RICH_ORDERFLOW_WS_ENABLED).lower())
_sync_env_value("RICH_OPTIONS_SURFACE_ENABLED", str(RICH_OPTIONS_SURFACE_ENABLED).lower())
_sync_env_value("RICH_DERIVATIVES_INTERVAL_SECONDS", RICH_DERIVATIVES_INTERVAL_SECONDS)
_sync_env_value("RICH_ORDERFLOW_INTERVAL_SECONDS", RICH_ORDERFLOW_INTERVAL_SECONDS)
_sync_env_value("RICH_ORDERBOOK_INTERVAL_SECONDS", RICH_ORDERBOOK_INTERVAL_SECONDS)
_sync_env_value("RICH_OPTIONS_SURFACE_INTERVAL_SECONDS", RICH_OPTIONS_SURFACE_INTERVAL_SECONDS)
_sync_env_value("RICH_ORDERFLOW_WS_URL", RICH_ORDERFLOW_WS_URL)
_sync_env_value("RICH_ORDERFLOW_WS_CHANNEL", RICH_ORDERFLOW_WS_CHANNEL)
_sync_env_value("RICH_ORDERFLOW_WS_SYMBOL", RICH_ORDERFLOW_WS_SYMBOL)


def delta_status():
    private_api_configured = bool(DELTA_API_KEY and DELTA_API_SECRET)
    configured_accounts = delta_account_credentials()

    return {
        "public_api_mode": True,
        "api_key_configured": bool(DELTA_API_KEY),
        "api_secret_configured": bool(DELTA_API_SECRET),
        "private_api_configured": private_api_configured,
        "private_trading_enabled": False,
        "configured_account_count": len(configured_accounts),
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
    logger.info("Market refresh interval seconds: %s", MARKET_REFRESH_INTERVAL_SECONDS)
    logger.info("Option chain refresh interval seconds: %s", OPTION_CHAIN_REFRESH_INTERVAL_SECONDS)
    logger.info("SMC refresh interval seconds: %s", SMC_REFRESH_INTERVAL_SECONDS)
    logger.info("Volume profile refresh interval seconds: %s", VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS)
    logger.info("Probability Engine enabled: %s", PROBABILITY_ENGINE_ENABLED)
    logger.info("Probability Engine destructive retention enabled: %s", PROBABILITY_RETENTION_ENABLED)
    logger.info("Rich data collection enabled: %s", RICH_DATA_COLLECTION_ENABLED)
    logger.info("Rich orderflow REST enabled: %s", RICH_ORDERFLOW_REST_ENABLED)
    logger.info("Rich orderflow websocket enabled: %s", RICH_ORDERFLOW_WS_ENABLED)
    logger.info("Rich options surface enabled: %s", RICH_OPTIONS_SURFACE_ENABLED)
