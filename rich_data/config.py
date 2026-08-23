import os


RICH_DERIVATIVES_VERSION = "rich_data_v1_derivatives"
RICH_ORDERFLOW_VERSION = "rich_data_v1_orderflow"
RICH_ORDERFLOW_WS_VERSION = "rich_data_v2_orderflow_ws"
RICH_ORDERBOOK_VERSION = "rich_data_v1_orderbook"
RICH_OPTIONS_SURFACE_VERSION = "rich_data_v1_options_surface"


def _bool_env(key, default=False):
    value = os.getenv(key)
    if value in [None, ""]:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(key, default):
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


RICH_DATA_COLLECTION_ENABLED = _bool_env("RICH_DATA_COLLECTION_ENABLED", False)
RICH_ORDERFLOW_REST_ENABLED = _bool_env("RICH_ORDERFLOW_REST_ENABLED", True)
RICH_ORDERFLOW_WS_ENABLED = _bool_env("RICH_ORDERFLOW_WS_ENABLED", False)
RICH_OPTIONS_SURFACE_ENABLED = _bool_env("RICH_OPTIONS_SURFACE_ENABLED", False)
RICH_DERIVATIVES_INTERVAL_SECONDS = _int_env("RICH_DERIVATIVES_INTERVAL_SECONDS", 300)
RICH_ORDERFLOW_INTERVAL_SECONDS = _int_env("RICH_ORDERFLOW_INTERVAL_SECONDS", 60)
RICH_ORDERBOOK_INTERVAL_SECONDS = _int_env("RICH_ORDERBOOK_INTERVAL_SECONDS", 60)
RICH_OPTIONS_SURFACE_INTERVAL_SECONDS = _int_env("RICH_OPTIONS_SURFACE_INTERVAL_SECONDS", 600)
RICH_ORDERFLOW_WS_URL = os.getenv("RICH_ORDERFLOW_WS_URL", "wss://public-socket.india.delta.exchange")
RICH_ORDERFLOW_WS_CHANNEL = os.getenv("RICH_ORDERFLOW_WS_CHANNEL", "trades")
RICH_ORDERFLOW_WS_SYMBOL = os.getenv("RICH_ORDERFLOW_WS_SYMBOL", "ETHUSD")
