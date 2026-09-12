from dataclasses import dataclass
import os
from pathlib import Path
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is optional outside the app runtime.
    load_dotenv = None

if load_dotenv:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

GRIDBOT_VERSION = "0.1"
STRATEGY_VERSION = "grid-strategy-v0.1"
RISK_MODULE_VERSION = "grid-risk-v0.1"
ACCOUNTING_VERSION = "grid-accounting-v0.1"
MAX_ACTIVE_GRID_BOTS = 1

REST_URL = "https://cdn-ind.testnet.deltaex.org"
PRIVATE_WS_URL = "wss://socket-ind.testnet.deltaex.org"
PUBLIC_WS_URL = "wss://socket-ind-pub.testnet.deltaex.org"

TESTNET_REST_URL = REST_URL
TESTNET_PRIVATE_WS_URL = PRIVATE_WS_URL
TESTNET_PUBLIC_WS_URL = PUBLIC_WS_URL

LIVE_REST_URL = "https://api.india.delta.exchange"
LIVE_PRIVATE_WS_URL = "wss://socket.india.delta.exchange"
LIVE_PUBLIC_WS_URL = "wss://public-socket.india.delta.exchange"

APPROVED_HOSTS = {
    "testnet": {
        "rest": "cdn-ind.testnet.deltaex.org",
        "private_ws": "socket-ind.testnet.deltaex.org",
        "public_ws": "socket-ind-pub.testnet.deltaex.org",
    },
    "live": {
        "rest": "api.india.delta.exchange",
        "private_ws": "socket.india.delta.exchange",
        "public_ws": "public-socket.india.delta.exchange",
    },
}

ENVIRONMENT_ENDPOINTS = {
    "testnet": (TESTNET_REST_URL, TESTNET_PRIVATE_WS_URL, TESTNET_PUBLIC_WS_URL),
    "live": (LIVE_REST_URL, LIVE_PRIVATE_WS_URL, LIVE_PUBLIC_WS_URL),
}


@dataclass(frozen=True)
class DeltaEndpointConfig:
    __test__ = False

    rest_url: str = REST_URL
    private_ws_url: str = PRIVATE_WS_URL
    public_ws_url: str = PUBLIC_WS_URL
    environment: str = "testnet"


TestnetEndpointConfig = DeltaEndpointConfig


def _host(url: str) -> str:
    return urlparse(url).hostname or ""


def normalize_gridbot_environment(value: str | None = None) -> str:
    environment = (value if value is not None else os.getenv("GRIDBOT_ENV", "")).strip().lower()
    if environment not in ENVIRONMENT_ENDPOINTS:
        raise ValueError("DeltaGridBot execution refused: GRIDBOT_ENV must be explicitly set to testnet or live.")
    return environment


def endpoint_config_for_environment(environment: str | None = None) -> DeltaEndpointConfig:
    selected = normalize_gridbot_environment(environment)
    rest_url, private_ws_url, public_ws_url = ENVIRONMENT_ENDPOINTS[selected]
    return DeltaEndpointConfig(rest_url=rest_url, private_ws_url=private_ws_url, public_ws_url=public_ws_url, environment=selected)


def validate_delta_endpoints(config: DeltaEndpointConfig) -> None:
    environment = normalize_gridbot_environment(config.environment)
    approved = APPROVED_HOSTS[environment]
    if _host(config.rest_url) != approved["rest"]:
        raise ValueError(f"DeltaGridBot execution refused: REST endpoint is not approved Delta India {environment}.")
    if _host(config.private_ws_url) != approved["private_ws"]:
        raise ValueError(f"DeltaGridBot execution refused: private WebSocket endpoint is not approved Delta India {environment}.")
    if _host(config.public_ws_url) != approved["public_ws"]:
        raise ValueError(f"DeltaGridBot execution refused: public WebSocket endpoint is not approved Delta India {environment}.")


def validate_testnet_endpoints(config: TestnetEndpointConfig) -> None:
    validate_delta_endpoints(DeltaEndpointConfig(config.rest_url, config.private_ws_url, config.public_ws_url, "testnet"))


def gridbot_credentials(environment: str | None = None) -> tuple[str, str]:
    selected = normalize_gridbot_environment(environment)
    if selected == "testnet":
        return os.getenv("GRIDBOT_TESTNET_API_KEY", ""), os.getenv("GRIDBOT_TESTNET_API_SECRET", "")
    return os.getenv("GRIDBOT_LIVE_API_KEY", ""), os.getenv("GRIDBOT_LIVE_API_SECRET", "")


DEFAULT_RISK_THRESHOLDS = {
    "inventory_warning_utilisation": 0.6,
    "inventory_orange_utilisation": 0.8,
    "inventory_red_utilisation": 0.95,
    "margin_yellow_utilisation": 0.5,
    "margin_orange_utilisation": 0.65,
    "margin_red_utilisation": 0.8,
    "margin_critical_utilisation": 0.9,
    "grr_yellow": 0.5,
    "grr_orange": 0.75,
    "grr_red": 1.0,
    "grr_critical": 1.25,
    "drawdown_yellow_pct": 0.05,
    "drawdown_orange_pct": 0.1,
    "drawdown_red_pct": 0.15,
    "drawdown_critical_pct": 0.2,
    "max_open_orders": 100,
}
