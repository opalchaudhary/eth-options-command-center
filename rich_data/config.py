import os


RICH_DERIVATIVES_VERSION = "rich_data_v1_derivatives"
RICH_ORDERFLOW_VERSION = "rich_data_v1_orderflow"
RICH_ORDERBOOK_VERSION = "rich_data_v1_orderbook"


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
RICH_DERIVATIVES_INTERVAL_SECONDS = _int_env("RICH_DERIVATIVES_INTERVAL_SECONDS", 300)
RICH_ORDERFLOW_INTERVAL_SECONDS = _int_env("RICH_ORDERFLOW_INTERVAL_SECONDS", 60)
RICH_ORDERBOOK_INTERVAL_SECONDS = _int_env("RICH_ORDERBOOK_INTERVAL_SECONDS", 60)

