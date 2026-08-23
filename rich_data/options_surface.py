import logging
import math
from datetime import datetime, timedelta, timezone

import pandas as pd

from delta_api import get_products, get_tickers, iv_to_percent, safe_float
from market_data import fetch_ohlcv
from rich_data.config import RICH_OPTIONS_SURFACE_VERSION
from rich_data.repositories import OptionsSurfaceSnapshotRepository
from rich_data.time_utils import floor_time, utc_now


logger = logging.getLogger(__name__)

SURFACE_METHOD_VERSION = "options_surface_summary_v1"
TARGET_BUCKETS = ["D0", "D1", "D2", "D3", "W1", "W2", "W3", "M1"]
DAILY_BUCKETS = ["D0", "D1", "D2", "D3"]
WEEKLY_BUCKETS = ["W1", "W2", "W3"]
MONTHLY_BUCKETS = ["M1"]
CADENCE_MINUTES = {
    "D0": 10,
    "D1": 10,
    "D2": 10,
    "D3": 10,
    "W1": 30,
    "W2": 30,
    "W3": 30,
    "M1": 60,
}


def normalize_eth_options_surface_chain(products=None, tickers=None):
    products = products if products is not None else get_products()
    tickers = tickers if tickers is not None else get_tickers()
    ticker_map = {ticker.get("symbol"): ticker for ticker in tickers or []}
    rows = []

    for product in products or []:
        symbol = product.get("symbol") or ""
        contract_type = product.get("contract_type")
        if "ETH" not in symbol or contract_type not in ["call_options", "put_options"]:
            continue

        ticker = ticker_map.get(symbol, {})
        quotes = ticker.get("quotes") or {}
        greeks = ticker.get("greeks") or {}
        mark_iv = quotes.get("mark_iv") or ticker.get("mark_iv") or ticker.get("mark_vol")
        spot = safe_float(greeks.get("spot")) or safe_float(ticker.get("spot_price"))

        rows.append(
            {
                "contract_symbol": symbol,
                "strike": safe_float(product.get("strike_price") or ticker.get("strike_price")),
                "type": contract_type,
                "expiry": product.get("settlement_time"),
                "spot_price": safe_float(ticker.get("spot_price")) or spot,
                "index_price": spot,
                "mark_price": safe_float(ticker.get("mark_price")),
                "bid": safe_float(quotes.get("best_bid")),
                "ask": safe_float(quotes.get("best_ask")),
                "bid_size": safe_float(quotes.get("bid_size")),
                "ask_size": safe_float(quotes.get("ask_size")),
                "oi": safe_float(ticker.get("oi")),
                "volume": safe_float(ticker.get("volume")),
                "mark_iv": iv_to_percent(mark_iv),
                "bid_iv": iv_to_percent(quotes.get("bid_iv")),
                "ask_iv": iv_to_percent(quotes.get("ask_iv")),
                "delta": safe_float(greeks.get("delta")),
                "gamma": safe_float(greeks.get("gamma")),
                "theta": safe_float(greeks.get("theta")),
                "vega": safe_float(greeks.get("vega")),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["expiry"] = pd.to_datetime(df["expiry"], utc=True, errors="coerce")
    numeric_columns = [
        "strike",
        "spot_price",
        "index_price",
        "mark_price",
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "oi",
        "volume",
        "mark_iv",
        "bid_iv",
        "ask_iv",
        "delta",
        "gamma",
        "theta",
        "vega",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.dropna(subset=["expiry", "strike", "type"]).reset_index(drop=True)


def is_due_bucket(bucket, snapshot_timestamp):
    cadence = CADENCE_MINUTES[bucket]
    return snapshot_timestamp.minute % cadence == 0


def due_buckets(snapshot_timestamp):
    return [bucket for bucket in TARGET_BUCKETS if is_due_bucket(bucket, snapshot_timestamp)]


def map_target_expiries(expiries, now=None):
    now = now or utc_now()
    normalized = sorted({pd.Timestamp(expiry).to_pydatetime().astimezone(timezone.utc) for expiry in expiries if pd.notna(expiry)})
    by_date = {}
    for expiry in normalized:
        by_date.setdefault(expiry.date(), expiry)

    mapping = {}
    today = now.date()
    for offset, bucket in enumerate(DAILY_BUCKETS):
        target_date = today + timedelta(days=offset)
        if target_date in by_date:
            mapping[bucket] = by_date[target_date]

    daily_cutoff = today + timedelta(days=3)
    weekly_candidates = [
        expiry
        for expiry in normalized
        if expiry.date() > daily_cutoff and not _is_monthly_expiry(expiry, now)
    ]
    for bucket, expiry in zip(WEEKLY_BUCKETS, weekly_candidates):
        mapping[bucket] = expiry

    monthly_candidates = [
        expiry
        for expiry in normalized
        if expiry.date() > daily_cutoff and _is_monthly_expiry(expiry, now)
    ]
    if monthly_candidates:
        mapping["M1"] = monthly_candidates[0]

    return mapping


def _is_monthly_expiry(expiry, now):
    if expiry.weekday() != 4:
        return False
    if (expiry - now).total_seconds() < 21 * 86400:
        return False
    return (expiry + timedelta(days=7)).month != expiry.month


def build_options_surface_rows(option_df, now=None, buckets=None, realized_volatility_reference=None):
    now = floor_time(now or utc_now(), 600)
    buckets = buckets or due_buckets(now)
    if option_df is None or option_df.empty:
        return []

    df = option_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["expiry"]):
        df["expiry"] = pd.to_datetime(df["expiry"], utc=True, errors="coerce")
    mapping = map_target_expiries(df["expiry"].dropna().unique(), now=now)
    rows = []

    for bucket in buckets:
        expiry = mapping.get(bucket)
        if not expiry:
            continue
        expiry_df = df[df["expiry"] == pd.Timestamp(expiry)].copy()
        try:
            row = build_expiry_surface_row(
                expiry_df,
                logical_expiry_bucket=bucket,
                actual_expiry=expiry,
                snapshot_timestamp=now,
                realized_volatility_reference=realized_volatility_reference,
            )
            if row:
                rows.append(row)
        except Exception as exc:
            logger.exception("options_surface.bucket_failed bucket=%s expiry=%s", bucket, expiry)
            rows.append(_error_row(bucket, expiry, now, str(exc)))
    return rows


def build_expiry_surface_row(expiry_df, logical_expiry_bucket, actual_expiry, snapshot_timestamp, realized_volatility_reference=None):
    snapshot_timestamp = snapshot_timestamp.astimezone(timezone.utc)
    actual_expiry = pd.Timestamp(actual_expiry).to_pydatetime().astimezone(timezone.utc)
    dte_hours = max(0.0, (actual_expiry - snapshot_timestamp).total_seconds() / 3600)
    spot_price = _first_valid(expiry_df.get("spot_price")) or _first_valid(expiry_df.get("index_price"))
    index_price = _first_valid(expiry_df.get("index_price"))
    calls = expiry_df[expiry_df["type"] == "call_options"].copy()
    puts = expiry_df[expiry_df["type"] == "put_options"].copy()
    contracts_seen = int(len(expiry_df))

    if expiry_df.empty or calls.empty or puts.empty:
        return _error_row(logical_expiry_bucket, actual_expiry, snapshot_timestamp, "missing_call_or_put_side")

    atm_strike = _atm_strike(expiry_df, spot_price)
    atm_call = _row_at_strike(calls, atm_strike)
    atm_put = _row_at_strike(puts, atm_strike)

    atm_call_mark = _value(atm_call, "mark_price")
    atm_put_mark = _value(atm_put, "mark_price")
    atm_call_iv = _value(atm_call, "mark_iv")
    atm_put_iv = _value(atm_put, "mark_iv")
    atm_iv = _average_valid([atm_call_iv, atm_put_iv])
    atm_bid_iv = _average_valid([_value(atm_call, "bid_iv"), _value(atm_put, "bid_iv")])
    atm_ask_iv = _average_valid([_value(atm_call, "ask_iv"), _value(atm_put, "ask_iv")])
    atm_straddle_mark = _sum_valid([atm_call_mark, atm_put_mark])
    atm_straddle_bid = _sum_valid([_value(atm_call, "bid"), _value(atm_put, "bid")])
    atm_straddle_ask = _sum_valid([_value(atm_call, "ask"), _value(atm_put, "ask")])
    atm_straddle_mid = _mid(atm_straddle_bid, atm_straddle_ask)

    call_25 = _select_delta_contract(calls, target_abs_delta=0.25, option_type="call_options")
    put_25 = _select_delta_contract(puts, target_abs_delta=0.25, option_type="put_options")
    call_25_iv = _value(call_25, "mark_iv")
    put_25_iv = _value(put_25, "mark_iv")
    risk_reversal = call_25_iv - put_25_iv if call_25_iv is not None and put_25_iv is not None else None
    put_skew_vs_atm = put_25_iv - atm_iv if put_25_iv is not None and atm_iv is not None else None
    call_skew_vs_atm = call_25_iv - atm_iv if call_25_iv is not None and atm_iv is not None else None
    butterfly = ((call_25_iv + put_25_iv) / 2 - atm_iv) if call_25_iv is not None and put_25_iv is not None and atm_iv is not None else None

    total_call_oi = _sum_series(calls["oi"])
    total_put_oi = _sum_series(puts["oi"])
    total_call_volume = _sum_series(calls["volume"])
    total_put_volume = _sum_series(puts["volume"])
    call_wall = _max_metric_row(calls, "oi")
    put_wall = _max_metric_row(puts, "oi")
    call_volume_wall = _max_metric_row(calls, "volume")
    put_volume_wall = _max_metric_row(puts, "volume")
    atm_zone = _atm_zone(expiry_df, atm_strike)

    valid_quote_contracts = int(((expiry_df["bid"].notna()) & (expiry_df["ask"].notna()) & (expiry_df["ask"] >= expiry_df["bid"])).sum())
    valid_iv_contracts = int(expiry_df["mark_iv"].notna().sum())
    valid_oi_contracts = int(expiry_df["oi"].notna().sum())
    valid_call_quotes = int(((calls["bid"].notna()) & (calls["ask"].notna()) & (calls["ask"] >= calls["bid"])).sum())
    valid_put_quotes = int(((puts["bid"].notna()) & (puts["ask"].notna()) & (puts["ask"] >= puts["bid"])).sum())

    atm_call_spread = _spread(atm_call)
    atm_put_spread = _spread(atm_put)
    atm_call_spread_pct = _spread_pct(atm_call)
    atm_put_spread_pct = _spread_pct(atm_put)
    surface_liquidity_score = _liquidity_score(valid_quote_contracts, contracts_seen, atm_call_spread_pct, atm_put_spread_pct)

    implied_move_pct = (atm_straddle_mark / spot_price * 100) if atm_straddle_mark is not None and spot_price else None
    iv_rv_spread = atm_iv - realized_volatility_reference if atm_iv is not None and realized_volatility_reference is not None else None
    iv_rv_ratio = atm_iv / realized_volatility_reference if atm_iv is not None and realized_volatility_reference else None

    completeness = _completeness(
        [
            spot_price,
            atm_strike,
            atm_call_mark,
            atm_put_mark,
            atm_iv,
            total_call_oi,
            total_put_oi,
            valid_quote_contracts if valid_quote_contracts else None,
        ]
    )
    status = "COMPLETE" if completeness >= 0.85 else ("PARTIAL" if completeness > 0 else "UNAVAILABLE")

    row = {
        "snapshot_timestamp": snapshot_timestamp.isoformat(),
        "symbol": "ETHUSD",
        "version": RICH_OPTIONS_SURFACE_VERSION,
        "surface_method_version": SURFACE_METHOD_VERSION,
        "logical_expiry_bucket": logical_expiry_bucket,
        "actual_expiry": actual_expiry.isoformat(),
        "dte_hours": dte_hours,
        "dte_days": dte_hours / 24,
        "spot_price": spot_price,
        "index_price": index_price,
        "atm_strike": atm_strike,
        "atm_call_mark": atm_call_mark,
        "atm_put_mark": atm_put_mark,
        "atm_call_bid": _value(atm_call, "bid"),
        "atm_call_ask": _value(atm_call, "ask"),
        "atm_put_bid": _value(atm_put, "bid"),
        "atm_put_ask": _value(atm_put, "ask"),
        "atm_call_iv": atm_call_iv,
        "atm_put_iv": atm_put_iv,
        "atm_iv": atm_iv,
        "atm_bid_iv": atm_bid_iv,
        "atm_ask_iv": atm_ask_iv,
        "atm_straddle_mark": atm_straddle_mark,
        "atm_straddle_bid": atm_straddle_bid,
        "atm_straddle_ask": atm_straddle_ask,
        "atm_straddle_mid": atm_straddle_mid,
        "implied_move_abs": atm_straddle_mark,
        "implied_move_pct": implied_move_pct,
        "realized_volatility_reference": realized_volatility_reference,
        "realized_vol_window": "24h_5m_close_to_close_annualized_pct",
        "iv_rv_spread": iv_rv_spread,
        "iv_rv_ratio": iv_rv_ratio,
        "put_25d_iv": put_25_iv,
        "call_25d_iv": call_25_iv,
        "risk_reversal_25d": risk_reversal,
        "put_skew_vs_atm": put_skew_vs_atm,
        "call_skew_vs_atm": call_skew_vs_atm,
        "skew_slope": risk_reversal,
        "butterfly_25d": butterfly,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "put_call_oi_ratio": _ratio(total_put_oi, total_call_oi),
        "largest_call_oi_strike": _value(call_wall, "strike"),
        "largest_call_oi": _value(call_wall, "oi"),
        "largest_put_oi_strike": _value(put_wall, "strike"),
        "largest_put_oi": _value(put_wall, "oi"),
        "distance_to_call_oi_wall_abs": _distance(spot_price, _value(call_wall, "strike")),
        "distance_to_call_oi_wall_pct": _distance_pct(spot_price, _value(call_wall, "strike")),
        "distance_to_put_oi_wall_abs": _distance(spot_price, _value(put_wall, "strike")),
        "distance_to_put_oi_wall_pct": _distance_pct(spot_price, _value(put_wall, "strike")),
        "atm_zone_call_oi": _sum_series(atm_zone[atm_zone["type"] == "call_options"]["oi"]),
        "atm_zone_put_oi": _sum_series(atm_zone[atm_zone["type"] == "put_options"]["oi"]),
        "oi_concentration": _oi_concentration(expiry_df),
        "total_call_volume": total_call_volume,
        "total_put_volume": total_put_volume,
        "put_call_volume_ratio": _ratio(total_put_volume, total_call_volume),
        "largest_call_volume_strike": _value(call_volume_wall, "strike"),
        "largest_put_volume_strike": _value(put_volume_wall, "strike"),
        "atm_call_spread": atm_call_spread,
        "atm_put_spread": atm_put_spread,
        "atm_call_spread_pct": atm_call_spread_pct,
        "atm_put_spread_pct": atm_put_spread_pct,
        "atm_combined_liquidity_score": _liquidity_score(2 if atm_call_spread_pct is not None and atm_put_spread_pct is not None else 0, 2, atm_call_spread_pct, atm_put_spread_pct),
        "valid_quoted_calls": valid_call_quotes,
        "valid_quoted_puts": valid_put_quotes,
        "usable_quote_pct": valid_quote_contracts / contracts_seen if contracts_seen else None,
        "surface_liquidity_score": surface_liquidity_score,
        "atm_call_delta": _value(atm_call, "delta"),
        "atm_put_delta": _value(atm_put, "delta"),
        "atm_gamma": _sum_valid([_value(atm_call, "gamma"), _value(atm_put, "gamma")]),
        "atm_theta": _sum_valid([_value(atm_call, "theta"), _value(atm_put, "theta")]),
        "atm_vega": _sum_valid([_value(atm_call, "vega"), _value(atm_put, "vega")]),
        "contracts_seen": contracts_seen,
        "valid_iv_contracts": valid_iv_contracts,
        "valid_oi_contracts": valid_oi_contracts,
        "valid_quote_contracts": valid_quote_contracts,
        "atm_quality": _quality(atm_call_mark is not None and atm_put_mark is not None and atm_iv is not None),
        "skew_quality": _quality(call_25_iv is not None and put_25_iv is not None),
        "oi_quality": _quality(valid_oi_contracts > 0),
        "liquidity_quality": _quality(valid_quote_contracts > 0),
        "source_status": status,
        "completeness": completeness,
        "staleness_seconds": max(0, (snapshot_timestamp - actual_expiry).total_seconds()) if snapshot_timestamp > actual_expiry else 0,
        "error_reason": None if status != "UNAVAILABLE" else "surface_unavailable",
        "metadata_json": {
            "sources": {
                "products": "delta.public.products",
                "tickers": "delta.public.tickers",
                "quotes": "delta.ticker.quotes",
                "realized_volatility": "delta.eth_ohlcv.5m.live_fetch",
            },
            "field_null_behavior": "Missing Delta fields are persisted as null, never fabricated as zero.",
            "expiry_mapping": "D0-D3 exact UTC calendar dates; W1-W3 next non-monthly expiries after D3; M1 first last-Friday monthly expiry after D3 with >=21 DTE.",
            "implied_move_definition": "ATM call mark + ATM put mark; pct divides by spot_price.",
            "atm_definition": "strike nearest spot/index reference with same-strike call and put when available.",
            "skew_definition": "nearest absolute 0.25 delta contract per side; no interpolation in V1.",
            "butterfly_definition": "((25d call IV + 25d put IV) / 2) - ATM IV.",
            "wall_definition": "largest open-interest strike by option side.",
            "atm_zone_definition": "strikes within one observed strike step of ATM.",
            "cadence_minutes": CADENCE_MINUTES.get(logical_expiry_bucket),
        },
    }
    return {key: _json_safe(value) for key, value in row.items()}


def realized_volatility_24h(symbol="ETHUSD", ohlcv_provider=None):
    provider = ohlcv_provider or fetch_ohlcv
    df = provider(symbol=symbol, resolution="5m", minutes_back=24 * 60)
    if df is None or df.empty or "close" not in df:
        return None
    closes = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(closes) < 3:
        return None
    returns = (closes / closes.shift(1)).apply(lambda value: math.log(value) if value and value > 0 else None).dropna()
    if len(returns) < 2:
        return None
    periods_per_year = 365 * 24 * 12
    return float(returns.std(ddof=1) * math.sqrt(periods_per_year) * 100)


class OptionsSurfaceCollector:
    def __init__(self, repository=None, chain_provider=None, realized_vol_provider=None):
        self.repository = repository or OptionsSurfaceSnapshotRepository()
        self.chain_provider = chain_provider or normalize_eth_options_surface_chain
        self.realized_vol_provider = realized_vol_provider or realized_volatility_24h

    def collect(self, symbol="ETHUSD", now=None):
        snapshot_timestamp = floor_time(now or utc_now(), 600)
        buckets = due_buckets(snapshot_timestamp)
        if not buckets:
            return {"ok": True, "row_count": 0, "source_status": "NOT_DUE"}

        option_df = self.chain_provider()
        rv = self.realized_vol_provider(symbol=symbol)
        rows = build_options_surface_rows(option_df, now=snapshot_timestamp, buckets=buckets, realized_volatility_reference=rv)
        ok = self.repository.upsert_many(rows)
        return {
            "ok": bool(ok),
            "row_count": len(rows) if ok else 0,
            "attempted_bucket_count": len(buckets),
            "persisted_buckets": [row.get("logical_expiry_bucket") for row in rows] if ok else [],
            "source_status": "HEALTHY" if rows and ok else ("NO_TARGET_EXPIRIES" if ok else "PERSIST_FAILED"),
        }


def _error_row(bucket, expiry, now, reason):
    return {
        "snapshot_timestamp": now.isoformat(),
        "symbol": "ETHUSD",
        "version": RICH_OPTIONS_SURFACE_VERSION,
        "surface_method_version": SURFACE_METHOD_VERSION,
        "logical_expiry_bucket": bucket,
        "actual_expiry": pd.Timestamp(expiry).to_pydatetime().astimezone(timezone.utc).isoformat() if expiry else None,
        "source_status": "UNAVAILABLE",
        "completeness": 0.0,
        "error_reason": reason,
        "metadata_json": {"collector": "options_surface_v1", "error": reason},
    }


def _first_valid(series):
    if series is None:
        return None
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.iloc[0]) if not clean.empty else None


def _atm_strike(df, spot_price):
    reference = spot_price if spot_price is not None else float(pd.to_numeric(df["strike"], errors="coerce").median())
    ranked = df.assign(distance=(pd.to_numeric(df["strike"], errors="coerce") - reference).abs()).sort_values(["distance", "strike"])
    return float(ranked.iloc[0]["strike"]) if not ranked.empty else None


def _row_at_strike(df, strike):
    if strike is None:
        return None
    rows = df[df["strike"] == strike]
    return rows.iloc[0].to_dict() if not rows.empty else None


def _select_delta_contract(df, target_abs_delta, option_type):
    clean = df.dropna(subset=["delta", "mark_iv"]).copy()
    if clean.empty:
        return None
    if option_type == "call_options":
        clean = clean[clean["delta"] > 0]
    else:
        clean = clean[clean["delta"] < 0]
    if clean.empty:
        return None
    clean["delta_distance"] = (clean["delta"].abs() - target_abs_delta).abs()
    return clean.sort_values(["delta_distance", "strike"]).iloc[0].to_dict()


def _value(row, key):
    if not row:
        return None
    return safe_float(row.get(key))


def _average_valid(values):
    clean = [safe_float(value) for value in values if safe_float(value) is not None]
    return sum(clean) / len(clean) if clean else None


def _sum_valid(values):
    clean = [safe_float(value) for value in values if safe_float(value) is not None]
    return sum(clean) if clean else None


def _sum_series(series):
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.sum()) if not clean.empty else None


def _max_metric_row(df, metric):
    clean = df.dropna(subset=[metric]).copy()
    if clean.empty:
        return None
    return clean.sort_values([metric, "strike"], ascending=[False, True]).iloc[0].to_dict()


def _ratio(numerator, denominator):
    return numerator / denominator if numerator is not None and denominator else None


def _distance(spot, strike):
    return abs(strike - spot) if spot is not None and strike is not None else None


def _distance_pct(spot, strike):
    return abs(strike - spot) / spot * 100 if spot and strike is not None else None


def _atm_zone(df, atm_strike):
    strikes = sorted(pd.to_numeric(df["strike"], errors="coerce").dropna().unique())
    if atm_strike is None or not strikes:
        return df.iloc[0:0]
    steps = sorted({round(abs(b - a), 10) for a, b in zip(strikes, strikes[1:]) if b > a})
    step = steps[0] if steps else 0
    return df[(df["strike"] >= atm_strike - step) & (df["strike"] <= atm_strike + step)]


def _oi_concentration(df):
    total = _sum_series(df["oi"])
    if not total:
        return None
    top = pd.to_numeric(df["oi"], errors="coerce").dropna().sort_values(ascending=False).head(3).sum()
    return float(top / total)


def _spread(row):
    bid = _value(row, "bid")
    ask = _value(row, "ask")
    return ask - bid if bid is not None and ask is not None and ask >= bid else None


def _spread_pct(row):
    spread = _spread(row)
    bid = _value(row, "bid")
    ask = _value(row, "ask")
    mid = _mid(bid, ask)
    return spread / mid * 100 if spread is not None and mid else None


def _mid(bid, ask):
    return (bid + ask) / 2 if bid is not None and ask is not None and ask >= bid else None


def _liquidity_score(valid_quotes, total_contracts, call_spread_pct, put_spread_pct):
    quote_score = (valid_quotes / total_contracts * 70) if total_contracts else 0
    spreads = [value for value in [call_spread_pct, put_spread_pct] if value is not None]
    spread_score = 30 if not spreads else max(0, 30 - min(30, sum(spreads) / len(spreads)))
    return round(min(100, quote_score + spread_score), 2)


def _completeness(values):
    return sum(1 for value in values if value is not None) / len(values) if values else 0.0


def _quality(ok):
    return "GREEN" if ok else "RED"


def _json_safe(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value
