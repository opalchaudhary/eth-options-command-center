from statistics import mean, pstdev

from delta_api import safe_float
from rich_data import delta_public
from rich_data.config import RICH_DERIVATIVES_VERSION
from rich_data.repositories import DerivativesMetricRepository
from rich_data.time_utils import floor_time, utc_now


def percentile_rank(value, samples):
    clean = sorted(float(item) for item in samples if item is not None)
    if value is None or len(clean) < 20:
        return None
    below_or_equal = sum(1 for item in clean if item <= float(value))
    return below_or_equal / len(clean)


def zscore(value, samples):
    clean = [float(item) for item in samples if item is not None]
    if value is None or len(clean) < 20:
        return None
    sigma = pstdev(clean)
    if not sigma:
        return 0.0
    return (float(value) - mean(clean)) / sigma


def build_derivatives_snapshot(ticker, previous=None, funding_samples=None, collected_at=None, version=RICH_DERIVATIVES_VERSION):
    ticker = ticker or {}
    timestamp = floor_time(collected_at or utc_now(), 300)
    spot_price = safe_float(ticker.get("spot_price"))
    mark_price = safe_float(ticker.get("mark_price"))
    open_interest = safe_float(ticker.get("oi"))
    funding_rate = safe_float(ticker.get("funding_rate"))
    premium = (mark_price - spot_price) if mark_price is not None and spot_price is not None else None
    premium_pct = (premium / spot_price) if premium is not None and spot_price else None

    previous_oi = safe_float((previous or {}).get("open_interest"))
    oi_delta_5m = (
        open_interest - previous_oi
        if open_interest is not None and previous_oi is not None
        else None
    )
    oi_delta_pct_5m = (
        oi_delta_5m / previous_oi
        if oi_delta_5m is not None and previous_oi not in [None, 0]
        else None
    )
    funding_samples = funding_samples or []

    return {
        "timestamp": timestamp.isoformat(),
        "symbol": ticker.get("symbol") or "ETHUSD",
        "version": version,
        "spot_price": spot_price,
        "reference_price": spot_price,
        "mark_price": mark_price,
        "mark_premium": premium,
        "mark_premium_pct": premium_pct,
        "open_interest": open_interest,
        "oi_delta_5m": oi_delta_5m,
        "oi_delta_pct_5m": oi_delta_pct_5m,
        "funding_rate": funding_rate,
        "funding_zscore": zscore(funding_rate, funding_samples),
        "funding_percentile": percentile_rank(funding_rate, funding_samples),
        "source_status": "HEALTHY" if spot_price is not None and mark_price is not None else "PARTIAL",
        "completeness": _completeness([spot_price, mark_price, open_interest, funding_rate]),
        "staleness_seconds": 0,
        "error_reason": None,
        "metadata_json": {
            "sources": {
                "spot_price": "delta.ticker.spot_price",
                "reference_price": "delta.ticker.spot_price",
                "mark_price": "delta.ticker.mark_price",
                "mark_premium": "derived.mark_price_minus_spot_price",
                "open_interest": "delta.ticker.oi",
                "funding_rate": "delta.ticker.funding_rate",
                "oi_delta_5m": "derived.previous_domain_snapshot",
                "funding_zscore": "derived.local_domain_history",
                "funding_percentile": "derived.local_domain_history",
            }
        },
    }


def _completeness(values):
    return sum(1 for value in values if value is not None) / len(values)


class DerivativesCollector:
    def __init__(self, repository=None, ticker_provider=None, version=RICH_DERIVATIVES_VERSION):
        self.repository = repository or DerivativesMetricRepository()
        self.ticker_provider = ticker_provider or delta_public.get_ticker
        self.version = version

    def collect(self, symbol="ETHUSD"):
        collected_at = utc_now()
        bucket = floor_time(collected_at, 300)
        previous = self.repository.latest_before(symbol=symbol, before_iso=bucket.isoformat())
        funding_samples = self.repository.recent_funding(symbol=symbol)
        ticker = self.ticker_provider(symbol)
        row = build_derivatives_snapshot(
            ticker=ticker,
            previous=previous,
            funding_samples=funding_samples,
            collected_at=collected_at,
            version=self.version,
        )
        ok = self.repository.upsert_one(row)
        return {"ok": ok, "row_count": 1 if ok else 0, "timestamp": row["timestamp"], "source_status": row["source_status"]}

