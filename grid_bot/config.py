from dataclasses import dataclass
import os
from urllib.parse import urlparse


GRIDBOT_VERSION = "0.1"
STRATEGY_VERSION = "grid-strategy-v0.1"
RISK_MODULE_VERSION = "grid-risk-v0.1"
ACCOUNTING_VERSION = "grid-accounting-v0.1"
MAX_ACTIVE_GRID_BOTS = 1

TESTNET_API_KEY = os.getenv("GRIDBOT_TESTNET_API_KEY") or os.getenv("DELTA_API_KEY", "")
TESTNET_API_SECRET = os.getenv("GRIDBOT_TESTNET_API_SECRET") or os.getenv("DELTA_API_SECRET", "")

REST_URL = "https://cdn-ind.testnet.deltaex.org"
PRIVATE_WS_URL = "wss://socket-ind.testnet.deltaex.org"
PUBLIC_WS_URL = "wss://socket-ind-pub.testnet.deltaex.org"

APPROVED_REST_HOST = "cdn-ind.testnet.deltaex.org"
APPROVED_PRIVATE_WS_HOST = "socket-ind.testnet.deltaex.org"
APPROVED_PUBLIC_WS_HOST = "socket-ind-pub.testnet.deltaex.org"


@dataclass(frozen=True)
class TestnetEndpointConfig:
    __test__ = False

    rest_url: str = REST_URL
    private_ws_url: str = PRIVATE_WS_URL
    public_ws_url: str = PUBLIC_WS_URL


def _host(url: str) -> str:
    return urlparse(url).hostname or ""


def validate_testnet_endpoints(config: TestnetEndpointConfig) -> None:
    if _host(config.rest_url) != APPROVED_REST_HOST:
        raise ValueError("DeltaGridBot execution refused: REST endpoint is not India Testnet.")
    if _host(config.private_ws_url) != APPROVED_PRIVATE_WS_HOST:
        raise ValueError("DeltaGridBot execution refused: private WebSocket endpoint is not India Testnet.")
    if _host(config.public_ws_url) != APPROVED_PUBLIC_WS_HOST:
        raise ValueError("DeltaGridBot execution refused: public WebSocket endpoint is not India Testnet.")


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
