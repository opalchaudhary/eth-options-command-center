from collections import defaultdict

import requests

from backend import config
from delta_api import get_margined_positions, get_tickers, get_wallet_balances, safe_float

from .json_utils import to_jsonable


OPTION_CONTRACT_TYPES = {"call_options", "put_options"}
POSITION_CONTRACT_TYPES = "perpetual_futures,call_options,put_options"
GREEK_KEYS = ("delta", "gamma", "theta", "vega")
BALANCE_KEYS = ("balance", "available_balance", "blocked_margin", "order_margin", "position_margin")


def _ticker_map():
    tickers = get_tickers()
    return {
        ticker.get("symbol"): ticker
        for ticker in tickers
        if ticker.get("symbol")
    }


def _position_symbol(position):
    return (
        position.get("product_symbol")
        or position.get("symbol")
        or (position.get("product") or {}).get("symbol")
    )


def _position_contract_type(position):
    return (
        position.get("contract_type")
        or (position.get("product") or {}).get("contract_type")
    )


def _position_size(position):
    return safe_float(position.get("size")) or 0.0


def _position_greeks(position, tickers_by_symbol):
    symbol = _position_symbol(position)
    ticker = tickers_by_symbol.get(symbol) or {}
    greeks = ticker.get("greeks") or {}
    size = _position_size(position)
    contract_type = _position_contract_type(position)

    values = {}

    for key in GREEK_KEYS:
        greek = safe_float(greeks.get(key))
        values[key] = size * greek if greek is not None else 0.0

    if contract_type not in OPTION_CONTRACT_TYPES:
        values["gamma"] = 0.0
        values["theta"] = 0.0
        values["vega"] = 0.0

    return values


def _summarize_positions(positions, tickers_by_symbol):
    rows = []
    totals = {key: 0.0 for key in GREEK_KEYS}

    for position in positions:
        greeks = _position_greeks(position, tickers_by_symbol)

        for key in GREEK_KEYS:
            totals[key] += greeks[key]

        row = dict(position)
        row["symbol"] = _position_symbol(position)
        row["contract_type"] = _position_contract_type(position)
        row["computed_delta"] = greeks["delta"]
        row["computed_gamma"] = greeks["gamma"]
        row["computed_theta"] = greeks["theta"]
        row["computed_vega"] = greeks["vega"]
        rows.append(row)

    return rows, totals


def _wallet_rows_and_totals(wallet_payload):
    rows = wallet_payload.get("result") or []
    meta = wallet_payload.get("meta") or {}
    totals_by_asset = defaultdict(lambda: {key: 0.0 for key in BALANCE_KEYS})

    for row in rows:
        asset = row.get("asset_symbol") or "UNKNOWN"

        for key in BALANCE_KEYS:
            totals_by_asset[asset][key] += safe_float(row.get(key)) or 0.0

    return rows, {
        "net_equity": safe_float(meta.get("net_equity")),
        "robo_trading_equity": safe_float(meta.get("robo_trading_equity")),
        "by_asset": {
            asset: values
            for asset, values in sorted(totals_by_asset.items())
        },
    }


def _empty_totals():
    return {
        "greeks": {key: 0.0 for key in GREEK_KEYS},
        "balances_by_asset": {},
        "net_equity": 0.0,
        "available_balance": 0.0,
        "balance": 0.0,
        "blocked_margin": 0.0,
        "order_margin": 0.0,
        "position_margin": 0.0,
    }


def _add_account_to_aggregate(aggregate, account):
    for key in GREEK_KEYS:
        aggregate["greeks"][key] += account["greeks"][key]

    balance_summary = account["balance_summary"]
    aggregate["net_equity"] += balance_summary.get("net_equity") or 0.0

    for asset, values in (balance_summary.get("by_asset") or {}).items():
        aggregate_asset = aggregate["balances_by_asset"].setdefault(
            asset,
            {key: 0.0 for key in BALANCE_KEYS},
        )

        for key in BALANCE_KEYS:
            aggregate_asset[key] += values.get(key) or 0.0
            aggregate[key] += values.get(key) or 0.0


def get_accounts_snapshot():
    credentials = config.delta_account_credentials()

    if not credentials:
        return {
            "ok": False,
            "error": "No Delta account API credentials are configured.",
            "accounts": [],
            "aggregate": _empty_totals(),
            "delta_credentials": config.delta_status(),
        }

    tickers_by_symbol = _ticker_map()
    accounts = []
    aggregate = _empty_totals()

    for account_config in credentials:
        account = {
            "id": account_config["id"],
            "label": account_config["label"],
            "kind": account_config["kind"],
            "ok": False,
            "positions": [],
            "position_count": 0,
            "wallets": [],
            "greeks": {key: 0.0 for key in GREEK_KEYS},
            "balance_summary": {
                "net_equity": None,
                "robo_trading_equity": None,
                "by_asset": {},
            },
        }

        try:
            positions = get_margined_positions(
                account_config["api_key"],
                account_config["api_secret"],
                contract_types=POSITION_CONTRACT_TYPES,
            )
            wallet_payload = get_wallet_balances(
                account_config["api_key"],
                account_config["api_secret"],
            )

            position_rows, greeks = _summarize_positions(positions, tickers_by_symbol)
            wallet_rows, balance_summary = _wallet_rows_and_totals(wallet_payload)

            account.update({
                "ok": True,
                "positions": position_rows,
                "position_count": len(position_rows),
                "wallets": wallet_rows,
                "greeks": greeks,
                "balance_summary": balance_summary,
            })
            _add_account_to_aggregate(aggregate, account)
        except requests.HTTPError as exc:
            response = exc.response
            account["error"] = response.text if response is not None else str(exc)
        except Exception as exc:
            account["error"] = str(exc)

        accounts.append(account)

    return {
        "ok": any(account.get("ok") for account in accounts),
        "accounts": to_jsonable(accounts),
        "aggregate": to_jsonable(aggregate),
        "delta_credentials": config.delta_status(),
    }
