import requests
import pandas as pd

from market_data import fetch_ohlcv


DELTA_BASE_URL = "https://api.india.delta.exchange/v2"
ALT_FUTURES_SYMBOLS = [
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "ADAUSDT",
    "BNBUSDT",
    "SUIUSDT",
    "ARBUSDT",
    "OPUSDT",
    "NEARUSDT",
    "APTUSDT",
    "INJUSDT",
    "SEIUSDT",
]


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _round(value, digits=4):
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _percent_change(current, previous):
    current = safe_float(current)
    previous = safe_float(previous)

    if current is None or previous in [None, 0]:
        return None

    return ((current - previous) / abs(previous)) * 100


def fetch_ticker_map():
    try:
        response = requests.get(f"{DELTA_BASE_URL}/tickers", timeout=10)
        response.raise_for_status()
        data = response.json()
        tickers = data.get("result", []) if data.get("success", True) else []
        return {item.get("symbol"): item for item in tickers if item.get("symbol")}
    except Exception as exc:
        print("Alt futures ticker fetch failed:", exc)
        return {}


def fetch_orderbook(symbol, depth=20):
    try:
        response = requests.get(
            f"{DELTA_BASE_URL}/l2orderbook/{symbol}",
            params={"depth": depth},
            headers={"Accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        if not data.get("success", True):
            return {"status": "error", "message": str(data)}

        result = data.get("result", {})
        bids = pd.DataFrame(result.get("buy", []))
        asks = pd.DataFrame(result.get("sell", []))

        for frame in [bids, asks]:
            if frame.empty:
                continue
            for col in ["price", "size", "depth"]:
                if col in frame:
                    frame[col] = pd.to_numeric(frame[col], errors="coerce")

        return {"status": "ok", "symbol": result.get("symbol", symbol), "bids": bids, "asks": asks}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "symbol": symbol}


def analyze_orderbook(orderbook):
    bids = orderbook.get("bids", pd.DataFrame())
    asks = orderbook.get("asks", pd.DataFrame())

    if orderbook.get("status") != "ok" or bids.empty or asks.empty:
        return {
            "status": "missing",
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "spread_pct": None,
            "bid_depth": None,
            "ask_depth": None,
            "imbalance_ratio": None,
            "liquidity_usdt": None,
            "bias": "Neutral",
            "trap_risk": "Unknown",
        }

    best_bid = bids["price"].max()
    best_ask = asks["price"].min()
    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
    spread = best_ask - best_bid if mid_price else None
    spread_pct = (spread / mid_price) * 100 if mid_price else None
    bid_depth = bids["size"].sum()
    ask_depth = asks["size"].sum()
    imbalance_ratio = bid_depth / ask_depth if ask_depth else None
    liquidity_units = bid_depth + ask_depth
    liquidity_usdt = liquidity_units * mid_price if mid_price else None

    if imbalance_ratio and imbalance_ratio > 1.25:
        bias = "Mild Bullish"
    elif imbalance_ratio and imbalance_ratio < 0.8:
        bias = "Mild Bearish"
    else:
        bias = "Neutral"

    max_bid = bids["size"].max() if "size" in bids else 0
    max_ask = asks["size"].max() if "size" in asks else 0
    max_wall = max(max_bid, max_ask)
    trap_risk = "Medium" if liquidity_units and max_wall > liquidity_units * 0.35 else "Low"

    return {
        "status": "ok",
        "best_bid": _round(best_bid, 8),
        "best_ask": _round(best_ask, 8),
        "mid_price": _round(mid_price, 8),
        "spread": _round(spread, 8),
        "spread_pct": _round(spread_pct, 4),
        "bid_depth": _round(bid_depth, 4),
        "ask_depth": _round(ask_depth, 4),
        "imbalance_ratio": _round(imbalance_ratio, 4),
        "liquidity_usdt": _round(liquidity_usdt, 2),
        "bias": bias,
        "trap_risk": trap_risk,
    }


def _indicators(candles):
    if candles is None or candles.empty or len(candles) < 30:
        return {}

    df = candles.copy()
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    prev_close = close.shift(1)
    true_range = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = true_range.rolling(14).mean().iloc[-1]
    last_close = close.iloc[-1]
    ema_20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    ema_50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
    typical = (high + low + close) / 3
    vwap = (typical * volume).sum() / volume.sum() if volume.sum() else None
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    volume_now = volume.tail(6).mean()
    volume_prev = volume.iloc[-30:-6].mean() if len(volume) >= 36 else volume.head(max(len(volume) - 6, 1)).mean()
    volatility_pct = close.pct_change().tail(30).std() * (30**0.5) * 100

    return {
        "atr": _round(atr, 8),
        "atr_pct": _round((atr / last_close) * 100 if last_close else None, 4),
        "volatility_pct": _round(volatility_pct, 4),
        "ema_20": _round(ema_20, 8),
        "ema_50": _round(ema_50, 8),
        "vwap": _round(vwap, 8),
        "rsi": _round(rsi.iloc[-1], 2),
        "last_close": _round(last_close, 8),
        "volume_now": _round(volume_now, 4),
        "volume_prev": _round(volume_prev, 4),
        "volume_change_pct": _round(_percent_change(volume_now, volume_prev), 2),
        "trend": "Bullish" if last_close > ema_20 > ema_50 else "Bearish" if last_close < ema_20 < ema_50 else "Mixed",
        "above_vwap": bool(vwap and last_close > vwap),
    }


def _score_candidate(ticker, indicators, orderbook):
    price = safe_float(ticker.get("mark_price") or ticker.get("close") or ticker.get("spot_price") or indicators.get("last_close"))
    volume = safe_float(ticker.get("volume") or ticker.get("turnover") or indicators.get("volume_now"))
    open_interest = safe_float(ticker.get("oi") or ticker.get("open_interest"))
    funding_rate = safe_float(ticker.get("funding_rate") or ticker.get("funding_rate_8h"))
    spread_pct = safe_float(orderbook.get("spread_pct"))
    liquidity_usdt = safe_float(orderbook.get("liquidity_usdt"))
    volatility_pct = safe_float(indicators.get("volatility_pct"), 0)
    atr_pct = safe_float(indicators.get("atr_pct"), 0)
    volume_change_pct = safe_float(indicators.get("volume_change_pct"), 0)
    rsi = safe_float(indicators.get("rsi"), 50)
    trend = indicators.get("trend")
    ob_bias = orderbook.get("bias")

    liquidity_score = min(18, (liquidity_usdt or 0) / 25000 * 18) if liquidity_usdt is not None else 8
    volume_score = min(14, max(0, volume_change_pct) / 80 * 14) if volume_change_pct is not None else 6
    oi_score = 7 if open_interest else 4
    funding_score = 9
    if funding_rate is None:
        funding_score = 6
    elif abs(funding_rate) > 0.03:
        funding_score = 3
    elif abs(funding_rate) > 0.015:
        funding_score = 6

    trend_score = 0
    long_bias = 0
    short_bias = 0

    if trend == "Bullish":
        trend_score += 14
        long_bias += 18
    elif trend == "Bearish":
        trend_score += 14
        short_bias += 18
    else:
        trend_score += 6

    if rsi >= 55 and rsi <= 72:
        long_bias += 10
        trend_score += 5
    elif rsi <= 45 and rsi >= 28:
        short_bias += 10
        trend_score += 5
    elif rsi > 78 or rsi < 22:
        trend_score -= 4

    if ob_bias == "Mild Bullish":
        long_bias += 6
    elif ob_bias == "Mild Bearish":
        short_bias += 6

    smc_score = 8
    spread_score = 12
    if spread_pct is None:
        spread_score = 7
    elif spread_pct <= 0.05:
        spread_score = 12
    elif spread_pct <= 0.12:
        spread_score = 8
    else:
        spread_score = 2

    if 0.6 <= atr_pct <= 3.8 and 0.8 <= volatility_pct <= 5:
        volatility_score = 12
    elif atr_pct <= 5.5 and volatility_pct <= 7:
        volatility_score = 8
    else:
        volatility_score = 3

    score = liquidity_score + volume_score + oi_score + funding_score + trend_score + smc_score + spread_score + volatility_score
    score = max(0, min(100, score))

    if long_bias - short_bias >= 18 and score >= 82:
        classification = "STRONG_LONG"
        direction = "LONG"
    elif long_bias > short_bias and score >= 68:
        classification = "LONG"
        direction = "LONG"
    elif short_bias - long_bias >= 18 and score >= 82:
        classification = "STRONG_SHORT"
        direction = "SHORT"
    elif short_bias > long_bias and score >= 68:
        classification = "SHORT"
        direction = "SHORT"
    elif score >= 55:
        classification = "WATCHLIST"
        direction = "NO_TRADE"
    elif spread_pct is not None and spread_pct > 0.2:
        classification = "AVOID"
        direction = "NO_TRADE"
    else:
        classification = "NO_TRADE"
        direction = "NO_TRADE"

    reason = (
        f"{classification}: score {score:.0f}, trend {trend}, RSI {rsi:.1f}, "
        f"volume change {volume_change_pct:.1f}%, spread {spread_pct if spread_pct is not None else 'NA'}%."
    )

    return {
        "price": price,
        "volume": volume,
        "open_interest": open_interest,
        "funding_rate": funding_rate,
        "spread": orderbook.get("spread"),
        "spread_pct": spread_pct,
        "volume_change_pct": volume_change_pct,
        "oi_change_pct": None,
        "score": round(score, 2),
        "classification": classification,
        "direction": direction,
        "reason": reason,
        "scores": {
            "liquidity": round(liquidity_score, 2),
            "volume_expansion": round(volume_score, 2),
            "oi_expansion": round(oi_score, 2),
            "funding_trap": round(funding_score, 2),
            "trend": round(trend_score, 2),
            "smc": round(smc_score, 2),
            "spread_safety": round(spread_score, 2),
            "volatility": round(volatility_score, 2),
        },
    }


def scan_symbol(symbol, ticker_map=None, resolution="5m", minutes_back=720):
    ticker_map = ticker_map if ticker_map is not None else fetch_ticker_map()
    ticker = ticker_map.get(symbol, {})
    candles = fetch_ohlcv(symbol=symbol, resolution=resolution, minutes_back=minutes_back)
    indicators = _indicators(candles)
    orderbook = analyze_orderbook(fetch_orderbook(symbol))
    scored = _score_candidate(ticker, indicators, orderbook)
    market_regime = "Trend" if indicators.get("trend") in ["Bullish", "Bearish"] else "Mixed"

    if not scored.get("price") and indicators.get("last_close"):
        scored["price"] = indicators.get("last_close")

    return {
        "symbol": symbol,
        "market_regime": market_regime,
        "indicators": indicators,
        "orderbook": orderbook,
        **scored,
    }


def scan_alt_futures(symbols=None):
    symbols = symbols or ALT_FUTURES_SYMBOLS
    ticker_map = fetch_ticker_map()
    candidates = []

    for symbol in symbols:
        try:
            candidates.append(scan_symbol(symbol, ticker_map=ticker_map))
        except Exception as exc:
            candidates.append(
                {
                    "symbol": symbol,
                    "price": None,
                    "score": 0,
                    "classification": "NO_TRADE",
                    "direction": "NO_TRADE",
                    "reason": f"Scanner error: {exc}",
                    "indicators": {},
                    "orderbook": {},
                    "scores": {},
                }
            )

    return sorted(candidates, key=lambda item: item.get("score") or 0, reverse=True)
