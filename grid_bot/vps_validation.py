import argparse
import asyncio
import base64
import json
import os
import socket
import ssl
import time
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlparse

import websocket
import requests
import websockets

from .config import PRIVATE_WS_URL, PUBLIC_WS_URL
from .delta_testnet_client import DeltaTestnetClient
from .durable_lifecycle import DurableGridBotLifecycle, first_real_grid_validation
from .rest_fallback import RestFallbackPoller, RestFallbackState


def _summarize_response(value):
    if isinstance(value, list):
        return {"type": "list", "count": len(value)}
    if isinstance(value, dict):
        result = value.get("result")
        if isinstance(result, list):
            return {"type": "dict", "keys": list(value.keys()), "result_count": len(result)}
        if isinstance(result, dict):
            return {"type": "dict", "keys": list(value.keys()), "result_keys": list(result.keys())[:20]}
        return {"type": "dict", "keys": list(value.keys())[:20]}
    return {"type": type(value).__name__}


def _capture(name, callback):
    try:
        return name, {"ok": True, **_summarize_response(callback())}
    except Exception as exc:
        response = getattr(exc, "response", None)
        return name, {
            "ok": False,
            "status": getattr(response, "status_code", None),
            "error": (getattr(response, "text", None) or str(exc))[:500],
        }


def readonly_connectivity():
    client = DeltaTestnetClient()
    checks = {}
    for name, callback in [
        ("public_rest_products", client.get_products),
        ("public_rest_tickers", client.get_tickers),
        ("product_spec_ETHUSD", lambda: client.product_spec("ETHUSD").__dict__),
        ("authenticated_wallet", client.wallet),
        ("authenticated_positions", client.positions),
        ("portfolio_margin_state", client.account_margin),
        ("open_orders_ETHUSD", lambda: client.open_orders(client.product_spec("ETHUSD").product_id)),
    ]:
        key, value = _capture(name, callback)
        checks[key] = value
    return checks


def product_spec():
    client = DeltaTestnetClient()
    spec = client.product_spec("ETHUSD")
    return {
        "product_id": spec.product_id,
        "symbol": spec.symbol,
        "contract_type": spec.contract_type,
        "contract_multiplier": str(spec.contract_multiplier),
        "lot_size": str(spec.lot_size),
        "min_quantity": str(spec.min_quantity),
        "tick_size": str(spec.tick_size),
        "price_precision": spec.price_precision,
        "quantity_precision": spec.quantity_precision,
        "mark_price": str(spec.mark_price),
        "last_price": str(spec.last_price) if spec.last_price is not None else None,
        "best_bid": str(spec.best_bid) if spec.best_bid is not None else None,
        "best_ask": str(spec.best_ask) if spec.best_ask is not None else None,
    }


def _ws_probe(url, messages, timeout=8):
    ws = websocket.create_connection(
        url,
        timeout=timeout,
        origin="https://www.delta.exchange",
        header=["User-Agent: deltaforge-gridbot-v0.1-testnet"],
    )
    received = []
    try:
        ws.settimeout(2)
        for message in messages:
            ws.send(json.dumps(message))
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    raw = ws.recv()
                except Exception:
                    break
                try:
                    payload = json.loads(raw)
                except Exception:
                    payload = {"raw": raw[:300]}
                received.append(payload)
                if payload.get("type") in {"subscriptions", "success", "error"} or payload.get("success") is not None:
                    break
        return {"ok": True, "message_count": len(received), "sample_types": [item.get("type") for item in received[:10]], "samples": received[:5]}
    finally:
        ws.close()


def websocket_connectivity():
    client = DeltaTestnetClient()
    public_subscribes = [
        {"type": "subscribe", "payload": {"channels": [{"name": "ticker", "symbols": ["ETHUSD"]}]}},
        {"type": "subscribe", "payload": {"channels": [{"name": "l1_orderbook", "symbols": ["ETHUSD"]}]}},
        {"type": "subscribe", "payload": {"channels": [{"name": "all_trades", "symbols": ["ETHUSD"]}]}},
    ]
    private_subscribe = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {"name": "orders", "symbols": ["ETHUSD"]},
                {"name": "positions", "symbols": ["ETHUSD"]},
                {"name": "user_trades", "symbols": ["ETHUSD"]},
                {"name": "portfolio_margins", "symbols": ["ETHUSD"]},
            ]
        },
    }
    checks = {}
    for name, url, messages in [
        ("public_websocket", PUBLIC_WS_URL, public_subscribes),
        ("private_websocket", PRIVATE_WS_URL, [client.websocket_auth_payload(), private_subscribe]),
    ]:
        try:
            checks[name] = _ws_probe(url, messages)
        except Exception as exc:
            checks[name] = {"ok": False, "error": str(exc)[:500]}
    return checks


def api_smoke():
    payload = {
        "bot_name": "VPS Smoke Preview",
        "product_symbol": "ETHUSD",
        "grid_type": "neutral",
        "lower_price": "2400",
        "upper_price": "2550",
        "grid_count": 4,
        "spacing_type": "arithmetic",
        "lot_size": "1",
        "max_inventory_lots": "4",
        "allocated_capital": "100",
        "risk_capital": "50",
    }
    created = requests.post("http://127.0.0.1:8000/api/grid/bots", json=payload, timeout=10)
    created.raise_for_status()
    bot = created.json()["bot"]
    preview = requests.post(
        f"http://127.0.0.1:8000/api/grid/bots/{bot['bot_id']}/preview",
        json={"reference_price": "2475", "tick_size": "0.05"},
        timeout=10,
    )
    preview.raise_for_status()
    preview_payload = preview.json()["preview"]
    return {
        "ok": True,
        "bot_id": bot["bot_id"],
        "level_count": len(preview_payload["levels"]),
        "buy_count": len(preview_payload["buy_levels"]),
        "sell_count": len(preview_payload["sell_levels"]),
        "started": False,
    }


def _decimal(value, default="0"):
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _round_to_tick(price, tick_size):
    ticks = (Decimal(str(price)) / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
    return ticks * tick_size


def _position_size(client, product_id):
    positions = client.positions("ETH").get("result") or []
    for row in positions:
        if str(row.get("product_id")) == str(product_id) or row.get("product_symbol") == "ETHUSD" or row.get("symbol") == "ETHUSD":
            return _decimal(row.get("size"))
    return Decimal("0")


def _gridbot_open_orders(client, product_id):
    rows = client.open_orders(product_id).get("result") or []
    return [row for row in rows if str(row.get("client_order_id", "")).startswith(("DGB", "DGFV01"))]


def _recent_fills_for_order(client, product_id, order_id, start_time_us):
    fills = client.fills(product_id, start_time=start_time_us, page_size=50).get("result") or []
    return [fill for fill in fills if str(fill.get("order_id")) == str(order_id)]


def _weighted_average(fills):
    total_size = sum((_decimal(fill.get("size")) for fill in fills), Decimal("0"))
    if total_size == 0:
        return None
    notional = sum((_decimal(fill.get("size")) * _decimal(fill.get("price")) for fill in fills), Decimal("0"))
    return notional / total_size


def _fee_total(fills):
    return sum((_decimal(fill.get("commission")) for fill in fills), Decimal("0"))


def _roles(fills):
    return sorted({str(fill.get("role") or "unknown") for fill in fills})


def _wait_for_order_fills(client, product_id, order, start_time_us, timeout_seconds=20):
    order_id = order.get("id")
    submitted_at = time.time()
    deadline = time.monotonic() + timeout_seconds
    poller = RestFallbackPoller(client)
    state = RestFallbackState(f"validation_{order_id}", product_id, "ETHUSD")
    seen = {}
    polls = 0
    first_detected_at = None
    while time.monotonic() < deadline:
        polls += 1
        poll_result = poller.poll_once(state)
        fills = _recent_fills_for_order(client, product_id, order_id, start_time_us)
        for fill in fills:
            seen[str(fill.get("id"))] = fill
        if seen and first_detected_at is None:
            first_detected_at = time.time()
        if seen:
            filled = sum((_decimal(fill.get("size")) for fill in seen.values()), Decimal("0"))
            requested = _decimal(order.get("size"))
            unfilled = _decimal(order.get("unfilled_size"))
            if filled >= requested or unfilled == 0 or order.get("state") == "closed":
                break
        time.sleep(2)
    fills = list(seen.values())
    detection_latency = None if first_detected_at is None else first_detected_at - submitted_at
    return {
        "polls": polls,
        "poller_state": poller.serialise_state(state),
        "fills": fills,
        "filled_quantity": str(sum((_decimal(fill.get("size")) for fill in fills), Decimal("0"))),
        "average_price": str(_weighted_average(fills)) if fills else None,
        "fee": str(_fee_total(fills)),
        "roles": _roles(fills),
        "fill_ids": [str(fill.get("id")) for fill in fills],
        "detection_latency_seconds": detection_latency,
    }


def execution_gate():
    client = DeltaTestnetClient()
    spec = client.product_spec("ETHUSD")
    rest = readonly_connectivity()
    ws = websocket_connectivity()
    open_orders = _gridbot_open_orders(client, spec.product_id)
    start_us = int((time.time() - 120) * 1_000_000)
    fills = client.fills(spec.product_id, start_time=start_us, page_size=50)
    positions = client.positions("ETH")
    margin = client.account_margin()
    return {
        "authenticated_rest": rest["authenticated_wallet"]["ok"] and rest["authenticated_positions"]["ok"],
        "public_ws": ws["public_websocket"]["ok"],
        "private_ws_status": "BLOCKED_403" if not ws["private_websocket"]["ok"] else "AVAILABLE",
        "execution_event_mode": "REST_FALLBACK",
        "operational_state": "DEGRADED",
        "rest_open_order_reconciliation": len(open_orders) == 0,
        "rest_fill_reconciliation": bool(fills.get("success")),
        "rest_position_reconciliation": bool(positions.get("success")),
        "rest_margin_account_fetch": bool(margin.get("success")),
        "ethusd_position": str(_position_size(client, spec.product_id)),
        "gridbot_open_orders": len(open_orders),
        "polling_interval_seconds": 2,
    }


def roundtrip_10_lot():
    client = DeltaTestnetClient()
    spec = client.product_spec("ETHUSD")
    starting_position = _position_size(client, spec.product_id)
    starting_orders = _gridbot_open_orders(client, spec.product_id)
    if starting_position != 0:
        return {"ok": False, "error": "Starting ETHUSD position is not zero.", "starting_position": str(starting_position)}
    if starting_orders:
        return {"ok": False, "error": "GridBot-owned ETHUSD open orders exist.", "gridbot_open_orders": len(starting_orders)}

    quantity = Decimal("10")
    buy_reference = max(spec.best_ask or spec.mark_price, spec.mark_price)
    sell_reference = min(spec.best_bid or spec.mark_price, spec.mark_price)
    buy_price = _round_to_tick(buy_reference + Decimal("20"), spec.tick_size)
    sell_price_hint = _round_to_tick(sell_reference - Decimal("20"), spec.tick_size)
    started_us = int((time.time() - 5) * 1_000_000)
    buy_payload = {
        "product_id": spec.product_id,
        "limit_price": str(buy_price),
        "size": int(quantity),
        "side": "buy",
        "order_type": "limit_order",
        "time_in_force": "ioc",
        "mmp": "disabled",
        "post_only": False,
        "reduce_only": False,
        "client_order_id": f"DGFV01BUY{int(time.time())}"[:32],
        "cancel_orders_accepted": False,
    }
    buy_submitted_at = time.time()
    try:
        buy_order = client.place_order(buy_payload).get("result") or {}
    except Exception as exc:
        response = getattr(exc, "response", None)
        return {
            "ok": False,
            "stage": "buy_order_submission",
            "status": getattr(response, "status_code", None),
            "exchange_error": (getattr(response, "text", None) or str(exc))[:500],
            "final_position": str(_position_size(client, spec.product_id)),
            "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
        }
    buy_fills = _wait_for_order_fills(client, spec.product_id, buy_order, started_us)
    filled_qty = _decimal(buy_fills["filled_quantity"])
    position_after_buy = _position_size(client, spec.product_id)
    if filled_qty == 0:
        return {"ok": False, "error": "BUY did not fill.", "buy_order": buy_order, "buy": buy_fills, "position_after_buy": str(position_after_buy)}

    exit_started_us = int((time.time() - 5) * 1_000_000)
    ticker = client.product_spec("ETHUSD")
    sell_reference = min(ticker.best_bid or ticker.mark_price, ticker.mark_price)
    sell_price = _round_to_tick(sell_reference - Decimal("20"), ticker.tick_size)
    sell_payload = {
        "product_id": spec.product_id,
        "limit_price": str(sell_price),
        "size": int(filled_qty),
        "side": "sell",
        "order_type": "limit_order",
        "time_in_force": "ioc",
        "mmp": "disabled",
        "post_only": False,
        "reduce_only": True,
        "client_order_id": f"DGFV01SELL{int(time.time())}"[:32],
        "cancel_orders_accepted": False,
    }
    sell_submitted_at = time.time()
    try:
        sell_order = client.place_order(sell_payload).get("result") or {}
    except Exception as exc:
        response = getattr(exc, "response", None)
        return {
            "ok": False,
            "stage": "sell_order_submission",
            "status": getattr(response, "status_code", None),
            "exchange_error": (getattr(response, "text", None) or str(exc))[:500],
            "position_after_buy": str(position_after_buy),
            "final_position": str(_position_size(client, spec.product_id)),
            "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
        }
    sell_fills = _wait_for_order_fills(client, spec.product_id, sell_order, exit_started_us)
    final_position = _position_size(client, spec.product_id)
    entry_price = _decimal(buy_fills["average_price"])
    exit_price = _decimal(sell_fills["average_price"])
    gross_pnl = (exit_price - entry_price) * filled_qty * spec.contract_multiplier if entry_price and exit_price else Decimal("0")
    fees = _decimal(buy_fills["fee"]) + _decimal(sell_fills["fee"])
    return {
        "ok": final_position == 0 and _decimal(sell_fills["filled_quantity"]) == filled_qty,
        "product": spec.symbol,
        "product_id": spec.product_id,
        "starting_position": str(starting_position),
        "buy_requested": str(quantity),
        "buy_filled": buy_fills["filled_quantity"],
        "buy_average_price": buy_fills["average_price"],
        "buy_roles": buy_fills["roles"],
        "buy_fee": buy_fills["fee"],
        "buy_fill_ids": buy_fills["fill_ids"],
        "buy_submitted_at": buy_submitted_at,
        "buy_detection_latency_seconds": buy_fills["detection_latency_seconds"],
        "position_after_buy": str(position_after_buy),
        "sell_requested": str(filled_qty),
        "sell_filled": sell_fills["filled_quantity"],
        "sell_average_price": sell_fills["average_price"],
        "sell_roles": sell_fills["roles"],
        "sell_fee": sell_fills["fee"],
        "sell_fill_ids": sell_fills["fill_ids"],
        "sell_submitted_at": sell_submitted_at,
        "sell_detection_latency_seconds": sell_fills["detection_latency_seconds"],
        "reduce_only_confirmation": sell_payload["reduce_only"],
        "final_position": str(final_position),
        "gross_pnl": str(gross_pnl),
        "exchange_fees": str(fees),
        "funding": "0",
        "other_delta_costs_credits": "0",
        "net_trading_pnl_before_income_tax": str(gross_pnl - fees),
    }


def resting_order_test():
    client = DeltaTestnetClient()
    spec = client.product_spec("ETHUSD")
    if _position_size(client, spec.product_id) != 0:
        return {"ok": False, "error": "Refusing resting-order test because ETHUSD position is not flat."}
    price = _round_to_tick((spec.best_bid or spec.mark_price) * Decimal("0.97"), spec.tick_size)
    client_order_id = f"DGFV01POST{int(time.time())}"[:32]
    payload = {
        "product_id": spec.product_id,
        "limit_price": str(price),
        "size": 1,
        "side": "buy",
        "order_type": "limit_order",
        "time_in_force": "gtc",
        "mmp": "disabled",
        "post_only": True,
        "reduce_only": False,
        "client_order_id": client_order_id,
        "cancel_orders_accepted": False,
    }
    try:
        order = client.place_order(payload).get("result") or {}
    except Exception as exc:
        response = getattr(exc, "response", None)
        return {
            "ok": False,
            "stage": "resting_order_submission",
            "status": getattr(response, "status_code", None),
            "exchange_error": (getattr(response, "text", None) or str(exc))[:500],
            "final_position": str(_position_size(client, spec.product_id)),
            "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
        }
    time.sleep(2)
    open_orders = client.open_orders(spec.product_id).get("result") or []
    matching = [row for row in open_orders if row.get("client_order_id") == client_order_id]
    cancelled = client.cancel_order(spec.product_id, str(order.get("id"))).get("result") or {}
    time.sleep(2)
    remaining = [row for row in client.open_orders(spec.product_id).get("result") or [] if row.get("client_order_id") == client_order_id]
    stray = _gridbot_open_orders(client, spec.product_id)
    return {
        "ok": bool(matching) and not remaining,
        "client_order_id": client_order_id,
        "price": str(price),
        "created_order_id": order.get("id"),
        "rest_verification": bool(matching),
        "cancelled_order_state": cancelled.get("state"),
        "cancellation_verification": not remaining,
        "stray_gridbot_orders": len(stray),
    }


def order_auth_probe():
    client = DeltaTestnetClient()
    spec = client.product_spec("ETHUSD")
    if _position_size(client, spec.product_id) != 0 or _gridbot_open_orders(client, spec.product_id):
        return {"ok": False, "error": "Refusing probe because position/orders are not clean."}
    payload = {
        "product_id": spec.product_id,
        "limit_price": str(_round_to_tick((spec.best_ask or spec.mark_price) + spec.tick_size * 10, spec.tick_size)),
        "size": 1,
        "side": "buy",
        "order_type": "limit_order",
        "time_in_force": "ioc",
        "mmp": "disabled",
        "post_only": False,
        "reduce_only": False,
        "client_order_id": f"DGFV01AUTH{int(time.time())}"[:32],
        "cancel_orders_accepted": False,
    }
    try:
        order = client.place_order(payload).get("result") or {}
        return {
            "ok": True,
            "order_id": order.get("id"),
            "state": order.get("state"),
            "final_position": str(_position_size(client, spec.product_id)),
            "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
        }
    except Exception as exc:
        response = getattr(exc, "response", None)
        return {
            "ok": False,
            "status": getattr(response, "status_code", None),
            "exchange_error": (getattr(response, "text", None) or str(exc))[:500],
            "final_position": str(_position_size(client, spec.product_id)),
            "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
        }


def cleanup_validation_position():
    client = DeltaTestnetClient()
    spec = client.product_spec("ETHUSD")
    position = _position_size(client, spec.product_id)
    if position == 0:
        return {"ok": True, "action": "none", "final_position": "0", "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id))}
    side = "sell" if position > 0 else "buy"
    quantity = abs(position)
    price = (
        _round_to_tick((spec.best_bid or spec.mark_price) - spec.tick_size * 10, spec.tick_size)
        if side == "sell"
        else _round_to_tick((spec.best_ask or spec.mark_price) + spec.tick_size * 10, spec.tick_size)
    )
    payload = {
        "product_id": spec.product_id,
        "limit_price": str(price),
        "size": int(quantity),
        "side": side,
        "order_type": "limit_order",
        "time_in_force": "ioc",
        "mmp": "disabled",
        "post_only": False,
        "reduce_only": True,
        "client_order_id": f"DGFV01CLEAN{int(time.time())}"[:32],
        "cancel_orders_accepted": False,
    }
    try:
        order = client.place_order(payload).get("result") or {}
    except Exception as exc:
        response = getattr(exc, "response", None)
        return {
            "ok": False,
            "stage": "cleanup_order_submission",
            "status": getattr(response, "status_code", None),
            "exchange_error": (getattr(response, "text", None) or str(exc))[:500],
            "starting_position": str(position),
            "final_position": str(_position_size(client, spec.product_id)),
            "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
        }
    time.sleep(3)
    return {
        "ok": _position_size(client, spec.product_id) == 0,
        "action": "reduce_only_close",
        "side": side,
        "quantity": str(quantity),
        "order_id": order.get("id"),
        "order_state": order.get("state"),
        "starting_position": str(position),
        "final_position": str(_position_size(client, spec.product_id)),
        "stray_gridbot_orders": len(_gridbot_open_orders(client, spec.product_id)),
    }


def durable_grid_status():
    return DurableGridBotLifecycle().status()


def durable_first_grid_run():
    return first_real_grid_validation()


def _response_header_subset(headers):
    wanted = {
        "server",
        "date",
        "content-type",
        "content-length",
        "connection",
        "x-cache",
        "via",
        "x-amz-cf-pop",
        "x-amz-cf-id",
    }
    return {key: value for key, value in headers.items() if key.lower() in wanted}


def _parse_http_response(raw):
    text = raw.decode("iso-8859-1", errors="replace")
    header_text = text.split("\r\n\r\n", 1)[0]
    lines = header_text.split("\r\n")
    status_line = lines[0] if lines else ""
    headers = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return status_line, headers


def _raw_handshake(url, user_agent=None, origin=None, force_ipv4=True):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 443
    path = parsed.path or "/"
    family = socket.AF_INET if force_ipv4 else 0
    infos = socket.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)
    address = infos[0][4]
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request_headers = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if user_agent:
        request_headers.append(f"User-Agent: {user_agent}")
    if origin:
        request_headers.append(f"Origin: {origin}")
    request = "\r\n".join(request_headers) + "\r\n\r\n"
    ctx = ssl.create_default_context()
    with socket.create_connection(address, timeout=8) as sock:
        peer = sock.getpeername()
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            tls_version = ssock.version()
            ssock.sendall(request.encode("ascii"))
            response = ssock.recv(4096)
    status_line, response_headers = _parse_http_response(response)
    return {
        "request_url": url,
        "request_headers": [line for line in request_headers if not line.startswith("Sec-WebSocket-Key")],
        "response_status": status_line,
        "response_headers": _response_header_subset(response_headers),
        "peer": f"{peer[0]}:{peer[1]}",
        "ip_family": "IPv4" if "." in peer[0] else "IPv6",
        "tls_version": tls_version,
        "sni": host,
    }


def _websocket_client_handshake(url, user_agent, origin=None, suppress_origin=False):
    kwargs = {
        "timeout": 8,
        "header": [f"User-Agent: {user_agent}"],
        "http_proxy_host": None,
        "http_proxy_port": None,
    }
    if suppress_origin:
        kwargs["suppress_origin"] = True
    elif origin:
        kwargs["origin"] = origin
    ws = websocket.create_connection(url, **kwargs)
    try:
        return {"ok": True, "connected": True, "peer": str(getattr(ws, "sock", None))}
    finally:
        ws.close()


async def _websockets_handshake(url, user_agent, origin=None):
    kwargs = {
        "open_timeout": 8,
        "user_agent_header": user_agent,
        "extra_headers": {},
    }
    if origin:
        kwargs["origin"] = origin
    async with websockets.connect(url, **kwargs) as ws:
        return {"ok": True, "connected": True, "subprotocol": ws.subprotocol}


def _capture_exception(callback):
    try:
        return callback()
    except Exception as exc:
        headers = getattr(exc, "headers", None)
        status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
        return {
            "ok": False,
            "exception": type(exc).__name__,
            "status": status,
            "error": str(exc)[:700],
            "response_headers": _response_header_subset(dict(headers)) if headers else None,
        }


def handshake_diagnostics():
    user_agent = "deltaforge-gridbot-v0.1-testnet/python-websocket-client"
    delta_origin = "https://www.delta.exchange"
    parsed = urlparse(PRIVATE_WS_URL)
    live_url = PRIVATE_WS_URL.rstrip("/") + "/live"
    proxy_keys = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"]
    result = {
        "endpoint": PRIVATE_WS_URL,
        "host": parsed.hostname,
        "outbound_ipv4": requests.get("https://api.ipify.org", timeout=8).text.strip(),
        "proxy_env": {key: os.environ.get(key) for key in proxy_keys if os.environ.get(key)},
        "dns": [item[4][0] for item in socket.getaddrinfo(parsed.hostname, 443, family=socket.AF_INET, type=socket.SOCK_STREAM)],
        "libraries": {
            "websocket_client": getattr(websocket, "__version__", "unknown"),
            "websockets": getattr(websockets, "__version__", "unknown"),
        },
        "tests": {},
    }
    variants = [
        ("public_raw_no_origin_with_ua", lambda: _raw_handshake(PUBLIC_WS_URL, user_agent=user_agent, origin=None)),
        ("raw_no_origin_with_ua", lambda: _raw_handshake(PRIVATE_WS_URL, user_agent=user_agent, origin=None)),
        ("raw_delta_origin_with_ua", lambda: _raw_handshake(PRIVATE_WS_URL, user_agent=user_agent, origin=delta_origin)),
        ("raw_no_user_agent", lambda: _raw_handshake(PRIVATE_WS_URL, user_agent=None, origin=None)),
        ("raw_live_path_no_origin_with_ua", lambda: _raw_handshake(live_url, user_agent=user_agent, origin=None)),
        ("websocket_client_no_origin_with_ua", lambda: _websocket_client_handshake(PRIVATE_WS_URL, user_agent, suppress_origin=True)),
        ("websocket_client_delta_origin_with_ua", lambda: _websocket_client_handshake(PRIVATE_WS_URL, user_agent, origin=delta_origin)),
        ("websocket_client_live_path_no_origin_with_ua", lambda: _websocket_client_handshake(live_url, user_agent, suppress_origin=True)),
        ("websockets_no_origin_with_ua", lambda: asyncio.run(_websockets_handshake(PRIVATE_WS_URL, user_agent))),
        ("websockets_delta_origin_with_ua", lambda: asyncio.run(_websockets_handshake(PRIVATE_WS_URL, user_agent, origin=delta_origin))),
        ("websockets_live_path_no_origin_with_ua", lambda: asyncio.run(_websockets_handshake(live_url, user_agent))),
    ]
    for name, callback in variants:
        result["tests"][name] = _capture_exception(callback)
    return result


def main():
    parser = argparse.ArgumentParser(description="DeltaGridBot V0.1 VPS-only validation helpers.")
    parser.add_argument(
        "mode",
        choices=[
            "readonly",
            "product-spec",
            "websocket",
            "api-smoke",
            "handshake-diagnostics",
            "execution-gate",
            "roundtrip-10-lot",
            "resting-order-test",
            "order-auth-probe",
            "cleanup-validation-position",
            "durable-grid-status",
            "first-gridbot-run",
        ],
    )
    args = parser.parse_args()

    if args.mode == "readonly":
        payload = readonly_connectivity()
    elif args.mode == "product-spec":
        payload = product_spec()
    elif args.mode == "websocket":
        payload = websocket_connectivity()
    elif args.mode == "api-smoke":
        payload = api_smoke()
    elif args.mode == "execution-gate":
        payload = execution_gate()
    elif args.mode == "roundtrip-10-lot":
        payload = roundtrip_10_lot()
    elif args.mode == "resting-order-test":
        payload = resting_order_test()
    elif args.mode == "order-auth-probe":
        payload = order_auth_probe()
    elif args.mode == "cleanup-validation-position":
        payload = cleanup_validation_position()
    elif args.mode == "durable-grid-status":
        payload = durable_grid_status()
    elif args.mode == "first-gridbot-run":
        payload = durable_first_grid_run()
    else:
        payload = handshake_diagnostics()

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
