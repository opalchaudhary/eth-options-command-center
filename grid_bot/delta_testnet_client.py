import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode, urlparse

import requests

from .config import (
    PRIVATE_WS_URL,
    PUBLIC_WS_URL,
    REST_URL,
    TESTNET_API_KEY,
    TESTNET_API_SECRET,
    TestnetEndpointConfig,
    validate_testnet_endpoints,
)
from .models import ProductSpec

USER_AGENT = "deltaforge-gridbot-v0.1-testnet"


def _safe_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


class DeltaTestnetClient:
    def __init__(
        self,
        api_key: str = TESTNET_API_KEY,
        api_secret: str = TESTNET_API_SECRET,
        endpoints: TestnetEndpointConfig | None = None,
        session: requests.Session | None = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoints = endpoints or TestnetEndpointConfig()
        validate_testnet_endpoints(self.endpoints)
        self.rest_url = self.endpoints.rest_url.rstrip("/")
        self.session = session or requests.Session()

    def _validate_rest_host(self) -> None:
        validate_testnet_endpoints(self.endpoints)
        if (urlparse(self.rest_url).hostname or "") != "cdn-ind.testnet.deltaex.org":
            raise ValueError("DeltaGridBot execution refused: REST host is not approved Testnet.")

    def _query(self, params: dict | None) -> str:
        cleaned = {k: v for k, v in (params or {}).items() if v not in [None, ""]}
        return "" if not cleaned else "?" + urlencode(cleaned)

    def _headers(self, method: str, path: str, params: dict | None = None, body: dict | None = None) -> dict:
        timestamp = str(int(time.time()))
        payload = json.dumps(body, separators=(",", ":")) if body else ""
        api_path = path if path.startswith("/v2/") else f"/v2{path if path.startswith('/') else '/' + path}"
        signature_data = method.upper() + timestamp + api_path + self._query(params) + payload
        signature = hmac.new(self.api_secret.encode("utf-8"), signature_data.encode("utf-8"), hashlib.sha256).hexdigest()
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "api-key": self.api_key,
            "signature": signature,
            "timestamp": timestamp,
        }

    def public_get(self, path: str, params: dict | None = None, timeout: int = 10) -> dict:
        self._validate_rest_host()
        response = self.session.get(f"{self.rest_url}/v2{path}", params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def private_get(self, path: str, params: dict | None = None, timeout: int = 15) -> dict:
        self._validate_rest_host()
        response = self.session.get(
            f"{self.rest_url}/v2{path}",
            params=params,
            headers=self._headers("GET", path, params=params),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def private_post(self, path: str, payload: dict, timeout: int = 15) -> dict:
        self._validate_rest_host()
        body = json.dumps(payload, separators=(",", ":"))
        response = self.session.post(
            f"{self.rest_url}/v2{path}",
            headers=self._headers("POST", path, body=payload),
            data=body,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def private_delete(self, path: str, payload: dict | None = None, timeout: int = 15) -> dict:
        self._validate_rest_host()
        payload = payload or {}
        body = json.dumps(payload, separators=(",", ":")) if payload else ""
        response = self.session.delete(
            f"{self.rest_url}/v2{path}",
            headers=self._headers("DELETE", path, body=payload),
            data=body,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def websocket_auth_payload(self) -> dict:
        validate_testnet_endpoints(TestnetEndpointConfig(REST_URL, PRIVATE_WS_URL, PUBLIC_WS_URL))
        timestamp = str(int(time.time()))
        signature_data = "GET" + timestamp + "/live"
        signature = hmac.new(self.api_secret.encode("utf-8"), signature_data.encode("utf-8"), hashlib.sha256).hexdigest()
        return {"type": "key-auth", "payload": {"api-key": self.api_key, "signature": signature, "timestamp": timestamp}}

    def get_products(self) -> list[dict]:
        return self.public_get("/products").get("result") or []

    def get_tickers(self) -> list[dict]:
        return self.public_get("/tickers").get("result") or []

    def product_spec(self, symbol: str) -> ProductSpec:
        products = self.get_products()
        tickers = {item.get("symbol"): item for item in self.get_tickers()}
        product = next((item for item in products if item.get("symbol") == symbol), None)
        if not product:
            raise ValueError(f"Delta Testnet product not found: {symbol}")
        ticker = tickers.get(symbol) or {}
        quotes = ticker.get("quotes") or {}
        return ProductSpec(
            product_id=int(product.get("id") or product.get("product_id")),
            symbol=symbol,
            contract_type=str(product.get("contract_type") or ""),
            contract_multiplier=_safe_decimal(product.get("contract_value") or product.get("contract_multiplier") or 1, "1"),
            lot_size=_safe_decimal(product.get("contract_unit") or product.get("lot_size") or product.get("min_order_size") or 1, "1"),
            min_quantity=_safe_decimal(product.get("min_order_size") or product.get("minimum_quantity") or 1, "1"),
            tick_size=_safe_decimal(product.get("tick_size") or product.get("price_increment") or "0.1", "0.1"),
            price_precision=int(product.get("price_precision") or product.get("tick_size_precision") or 1),
            quantity_precision=int(product.get("quantity_precision") or 0),
            mark_price=_safe_decimal(ticker.get("mark_price") or quotes.get("mark_price")),
            last_price=_safe_decimal(ticker.get("close") or ticker.get("last_price")) if ticker else None,
            best_bid=_safe_decimal(quotes.get("best_bid") or ticker.get("best_bid")) if ticker else None,
            best_ask=_safe_decimal(quotes.get("best_ask") or ticker.get("best_ask")) if ticker else None,
        )

    def wallet(self) -> dict:
        return self.private_get("/wallet/balances")

    def positions(self, underlying_asset_symbol: str = "ETH") -> dict:
        return self.private_get("/positions", params={"underlying_asset_symbol": underlying_asset_symbol})

    def account_margin(self) -> dict:
        try:
            return self.private_get("/profile")
        except Exception:
            return self.private_get("/wallet/balances")

    def place_order(self, payload: dict) -> dict:
        return self.private_post("/orders", payload)

    def cancel_order(self, product_id: int, order_id: str) -> dict:
        return self.private_delete("/orders", {"product_id": product_id, "id": order_id})

    def open_orders(self, product_id: int | None = None) -> dict:
        params = {"product_id": product_id} if product_id else None
        return self.private_get("/orders", params=params)

    def get_order(self, order_id: str | int) -> dict:
        return self.private_get(f"/orders/{order_id}")

    def order_history(
        self,
        product_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        after: str | None = None,
        page_size: int = 50,
    ) -> dict:
        params = {
            "product_ids": str(product_id) if product_id else None,
            "start_time": start_time,
            "end_time": end_time,
            "after": after,
            "page_size": min(page_size, 50),
        }
        return self.private_get("/orders/history", params=params)

    def fills(
        self,
        product_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        after: str | None = None,
        page_size: int = 50,
    ) -> dict:
        params = {
            "product_ids": str(product_id) if product_id else None,
            "start_time": start_time,
            "end_time": end_time,
            "after": after,
            "page_size": min(page_size, 50),
        }
        return self.private_get("/fills", params=params)

    def ticker(self, symbol: str) -> dict:
        return self.public_get(f"/tickers/{symbol}")
