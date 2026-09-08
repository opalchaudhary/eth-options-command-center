from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports"
RECOMMENDATIONS_TABLE = "grid_parameter_recommendations"
OUTCOMES_TABLE = "grid_parameter_recommendation_outcomes"
OHLCV_TABLE = "eth_ohlcv"
SYMBOL = "ETHUSD"
HORIZON = "12H"
HORIZON_MINUTES = 12 * 60
RESOLUTION = "5m"
RESOLUTION_MINUTES = 5
MIN_SEGMENT_N = 5


@dataclass(frozen=True)
class PolicyRange:
    policy: str
    lower: float | None
    upper: float | None
    no_grid: bool = False
    reason: str | None = None

    @property
    def width(self) -> float | None:
        if self.lower is None or self.upper is None:
            return None
        return self.upper - self.lower


def parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def as_float(value: Any) -> float | None:
    if value in [None, ""]:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def midpoint(lower: float, upper: float) -> float:
    return (lower + upper) / 2


def widen_around(center: float, width: float) -> tuple[float, float]:
    return center - width / 2, center + width / 2


def range_utilization(actual_low: float, actual_high: float, lower: float, upper: float) -> float:
    width = upper - lower
    if width <= 0:
        return 0.0
    covered = max(0.0, min(actual_high, upper) - max(actual_low, lower))
    return covered / width


def directional_state(row: dict[str, Any]) -> str:
    up = as_float(row.get("upside_probability"))
    down = as_float(row.get("downside_probability"))
    if up is None or down is None:
        return "UNKNOWN"
    edge = up - down
    if edge >= 0.18 and up >= 0.58:
        return "BULLISH"
    if -edge >= 0.18 and down >= 0.58:
        return "BEARISH"
    return "NEUTRAL"


def confidence_band(value: Any) -> str:
    confidence = as_float(value)
    if confidence is None:
        return "UNKNOWN"
    if confidence >= 0.70:
        return "HIGH"
    if confidence >= 0.45:
        return "MEDIUM"
    return "LOW"


def quantile_band(value: Any, low: float, high: float, label: str) -> str:
    number = as_float(value)
    if number is None:
        return f"{label}_UNKNOWN"
    if number <= low:
        return f"{label}_LOW"
    if number >= high:
        return f"{label}_HIGH"
    return f"{label}_MID"


def champion_policy(row: dict[str, Any], trailing: dict[str, float] | None = None) -> PolicyRange:
    return PolicyRange("champion_v0_1", as_float(row.get("recommended_lower_price")), as_float(row.get("recommended_upper_price")))


def expansion_widen_policy(row: dict[str, Any], trailing: dict[str, float] | None = None) -> PolicyRange:
    champion = champion_policy(row)
    if champion.lower is None or champion.upper is None or not champion.width or champion.width <= 0:
        return PolicyRange("expansion_widen", None, None, True, "missing champion range")
    expansion = as_float(row.get("realized_over_range_width_ge_1")) or 0.0
    if expansion >= 0.86:
        factor = 1.60
    elif expansion >= 0.72:
        factor = 1.35
    elif expansion >= 0.55:
        factor = 1.20
    else:
        factor = 1.10
    lower, upper = widen_around(midpoint(champion.lower, champion.upper), champion.width * factor)
    return PolicyRange("expansion_widen", lower, upper)


def v2_range_floor_policy(row: dict[str, Any], trailing: dict[str, float] | None = None) -> PolicyRange:
    champion = champion_policy(row)
    ref_lower = as_float(row.get("range_70_lower"))
    ref_upper = as_float(row.get("range_70_upper"))
    if champion.lower is None or champion.upper is None:
        return PolicyRange("v2_range70_floor", None, None, True, "missing champion range")
    if ref_lower is None or ref_upper is None or ref_upper <= ref_lower:
        return PolicyRange("v2_range70_floor", None, None, True, "range_70 unavailable")
    return PolicyRange("v2_range70_floor", min(champion.lower, ref_lower), max(champion.upper, ref_upper))


def v2_volatility_buffer_policy(row: dict[str, Any], trailing: dict[str, float] | None = None) -> PolicyRange:
    champion = champion_policy(row)
    ref_lower = as_float(row.get("range_70_lower"))
    ref_upper = as_float(row.get("range_70_upper"))
    if champion.lower is None or champion.upper is None or not champion.width or champion.width <= 0:
        return PolicyRange("v2_range70_trailing_buffer", None, None, True, "missing champion range")
    base_width = champion.width
    if ref_lower is not None and ref_upper is not None and ref_upper > ref_lower:
        base_width = max(base_width, ref_upper - ref_lower)
    trailing_width = (trailing or {}).get("trailing_12h_path_width")
    if trailing_width is None or trailing_width <= 0:
        return PolicyRange("v2_range70_trailing_buffer", None, None, True, "trailing OHLCV unavailable")
    expansion = as_float(row.get("realized_over_range_width_ge_1")) or 0.0
    buffer_width = 0.35 * trailing_width * (0.50 + expansion)
    lower, upper = widen_around(midpoint(champion.lower, champion.upper), base_width + buffer_width)
    return PolicyRange("v2_range70_trailing_buffer", lower, upper)


def stress_no_grid_policy(row: dict[str, Any], trailing: dict[str, float] | None = None) -> PolicyRange:
    containment = as_float(row.get("path_inside_70"))
    expansion = as_float(row.get("realized_over_range_width_ge_1"))
    if containment is not None and expansion is not None and containment <= 0.38 and expansion >= 0.72:
        return PolicyRange("stress_filter_no_grid", None, None, True, "containment <= 0.38 and expansion >= 0.72")
    champion = champion_policy(row)
    return PolicyRange("stress_filter_no_grid", champion.lower, champion.upper)


POLICIES = [
    champion_policy,
    expansion_widen_policy,
    v2_range_floor_policy,
    v2_volatility_buffer_policy,
    stress_no_grid_policy,
]


def first_breach(candles: list[dict[str, Any]], lower: float, upper: float, start: datetime) -> tuple[str | None, datetime | None, float | None]:
    for candle in candles:
        ts = parse_ts(candle.get("candle_time"))
        if ts is None:
            continue
        open_price = as_float(candle.get("open"))
        high = as_float(candle.get("high"))
        low = as_float(candle.get("low"))
        close = as_float(candle.get("close"))
        if None in {open_price, high, low, close}:
            continue
        if open_price > upper:
            return "upper", ts, (ts - start).total_seconds() / 60
        if open_price < lower:
            return "lower", ts, (ts - start).total_seconds() / 60
        path = [open_price, low, high, close] if close >= open_price else [open_price, high, low, close]
        for a, b in zip(path, path[1:]):
            if b > a and b > upper and a <= upper:
                return "upper", ts, (ts - start).total_seconds() / 60
            if b < a and b < lower and a >= lower:
                return "lower", ts, (ts - start).total_seconds() / 60
        if high > upper and low < lower:
            return "both_same_candle", ts, (ts - start).total_seconds() / 60
        if high > upper:
            return "upper", ts, (ts - start).total_seconds() / 60
        if low < lower:
            return "lower", ts, (ts - start).total_seconds() / 60
    return None, None, None


def evaluate_policy(row: dict[str, Any], outcome: dict[str, Any], policy_range: PolicyRange, candles: list[dict[str, Any]]) -> dict[str, Any]:
    champion_lower = as_float(row.get("recommended_lower_price"))
    champion_upper = as_float(row.get("recommended_upper_price"))
    champion_width = champion_upper - champion_lower if champion_lower is not None and champion_upper is not None else None
    actual_low = as_float(outcome.get("actual_forward_low"))
    actual_high = as_float(outcome.get("actual_forward_high"))
    start = parse_ts(outcome.get("evaluation_start")) or parse_ts(row.get("requested_at"))
    if policy_range.no_grid:
        champion_would_breach = (
            actual_low is not None
            and actual_high is not None
            and champion_lower is not None
            and champion_upper is not None
            and (actual_low < champion_lower or actual_high > champion_upper)
        )
        return {
            "policy": policy_range.policy,
            "recommendation_id": row.get("recommendation_id"),
            "usable": True,
            "no_grid": True,
            "no_grid_reason": policy_range.reason,
            "champion_would_breach": champion_would_breach,
            "recommended_width": None,
            "width_vs_champion": None,
            "stayed_inside": None,
            "breached": None,
            "minutes_to_first_breach": None,
            "actual_recommended_width_ratio": None,
            "range_utilization": None,
        }
    if policy_range.lower is None or policy_range.upper is None or policy_range.upper <= policy_range.lower or actual_low is None or actual_high is None or start is None:
        return {"policy": policy_range.policy, "recommendation_id": row.get("recommendation_id"), "usable": False, "no_grid": False}
    side, breach_time, minutes = first_breach(candles, policy_range.lower, policy_range.upper, start)
    width = policy_range.upper - policy_range.lower
    actual_width = actual_high - actual_low
    return {
        "policy": policy_range.policy,
        "recommendation_id": row.get("recommendation_id"),
        "usable": True,
        "no_grid": False,
        "recommended_lower": policy_range.lower,
        "recommended_upper": policy_range.upper,
        "recommended_width": width,
        "width_vs_champion": width / champion_width if champion_width and champion_width > 0 else None,
        "stayed_inside": actual_low >= policy_range.lower and actual_high <= policy_range.upper,
        "breached": actual_low < policy_range.lower or actual_high > policy_range.upper,
        "first_breach_side": side,
        "first_breach_time": breach_time.isoformat() if breach_time else None,
        "minutes_to_first_breach": minutes,
        "actual_recommended_width_ratio": actual_width / width if width > 0 else None,
        "range_utilization": range_utilization(actual_low, actual_high, policy_range.lower, policy_range.upper),
        "path_inside_70": as_float(row.get("path_inside_70")),
        "expansion_probability": as_float(row.get("realized_over_range_width_ge_1")),
        "directional_state": directional_state(row),
        "confidence": as_float(row.get("recommender_confidence")),
        "confidence_band": confidence_band(row.get("recommender_confidence")),
    }


class SupabaseReader:
    def __init__(self):
        load_dotenv(ROOT / ".env")
        self.url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
        self.key = os.getenv("SUPABASE_KEY")
        if not self.url or not self.key:
            raise RuntimeError("Supabase credentials are not configured.")
        self.headers = {"apikey": self.key, "Authorization": f"Bearer {self.key}"}

    def select(self, table: str, params: dict[str, Any] | list[tuple[str, Any]]) -> list[dict[str, Any]]:
        response = requests.get(f"{self.url}/rest/v1/{table}", headers=self.headers, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"Supabase read failed for {table}: {response.status_code} {response.text[:300]}")
        return response.json()

    def ohlcv_window(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        params = [
            ("select", "candle_time,open,high,low,close,volume"),
            ("symbol", f"eq.{SYMBOL}"),
            ("resolution", f"eq.{RESOLUTION}"),
            ("candle_time", f"gt.{start.isoformat()}"),
            ("candle_time", f"lte.{end.isoformat()}"),
            ("order", "candle_time.asc"),
            ("limit", 2000),
        ]
        return self.select(OHLCV_TABLE, params)


def trailing_features(candles: list[dict[str, Any]]) -> dict[str, float]:
    highs = [as_float(row.get("high")) for row in candles]
    lows = [as_float(row.get("low")) for row in candles]
    closes = [as_float(row.get("close")) for row in candles]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    closes = [value for value in closes if value is not None]
    result: dict[str, float] = {}
    if highs and lows:
        result["trailing_12h_path_width"] = max(highs) - min(lows)
    if len(closes) > 1:
        returns = pd.Series(closes).pct_change().dropna()
        result["trailing_12h_close_vol"] = float(returns.std()) if not returns.empty else 0.0
    return result


def aggregate(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for policy, group in pd.DataFrame(results).groupby("policy", sort=False):
        usable = group[group["usable"] == True]  # noqa: E712
        grids = usable[usable["no_grid"] == False]  # noqa: E712
        no_grids = usable[usable["no_grid"] == True]  # noqa: E712
        breached = grids[grids["breached"] == True]  # noqa: E712
        rows.append(
            {
                "policy": policy,
                "usable_snapshots": len(usable),
                "grid_recommendations": len(grids),
                "no_grid": int((usable["no_grid"] == True).sum()),  # noqa: E712
                "grid_participation_rate": len(grids) / len(usable) if len(usable) else None,
                "abstention_rate": len(no_grids) / len(usable) if len(usable) else None,
                "no_grid_avoided_champion_breach_rate": float((no_grids["champion_would_breach"] == True).mean()) if len(no_grids) and "champion_would_breach" in no_grids else None,  # noqa: E712
                "containment_rate": float((grids["stayed_inside"] == True).mean()) if len(grids) else None,  # noqa: E712
                "breach_rate": float((grids["breached"] == True).mean()) if len(grids) else None,  # noqa: E712
                "median_time_to_first_breach": float(breached["minutes_to_first_breach"].dropna().median()) if len(breached) else None,
                "median_actual_recommended_width_ratio": float(grids["actual_recommended_width_ratio"].dropna().median()) if len(grids) else None,
                "median_range_utilization": float(grids["range_utilization"].dropna().median()) if len(grids) else None,
                "median_recommended_width": float(grids["recommended_width"].dropna().median()) if len(grids) else None,
                "median_width_vs_champion": float(grids["width_vs_champion"].dropna().median()) if len(grids) else None,
                "wide_range_penalty": float((grids["width_vs_champion"].dropna() - 1).clip(lower=0).median()) if len(grids) else None,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["balance_score"] = df.apply(
        lambda row: None
        if pd.isna(row["containment_rate"])
        else row["containment_rate"] * (row["grid_participation_rate"] or 0)
        - 0.20 * (row["wide_range_penalty"] or 0)
        - 0.10 * max(0.0, 0.35 - (row["median_range_utilization"] or 0)),
        axis=1,
    )
    return df.sort_values(["balance_score", "containment_rate"], ascending=False)


def segment_summary(results: list[dict[str, Any]], recommendations: list[dict[str, Any]]) -> pd.DataFrame:
    base = pd.DataFrame(recommendations)
    inside_values = pd.to_numeric(base["path_inside_70"], errors="coerce")
    expansion_values = pd.to_numeric(base["realized_over_range_width_ge_1"], errors="coerce")
    inside_lo, inside_hi = inside_values.quantile([1 / 3, 2 / 3]).tolist()
    exp_lo, exp_hi = expansion_values.quantile([1 / 3, 2 / 3]).tolist()
    regimes = {}
    for row in recommendations:
        rec_id = row["recommendation_id"]
        regimes[rec_id] = {
            "inside_band": quantile_band(row.get("path_inside_70"), inside_lo, inside_hi, "inside"),
            "expansion_band": quantile_band(row.get("realized_over_range_width_ge_1"), exp_lo, exp_hi, "expansion"),
            "directional_state": directional_state(row),
            "confidence_band": confidence_band(row.get("recommender_confidence")),
        }
    enriched = []
    for result in results:
        if not result.get("usable") or result.get("no_grid"):
            continue
        enriched.append({**result, **regimes.get(result.get("recommendation_id"), {})})
    frame = pd.DataFrame(enriched)
    rows = []
    for segment_col in ["inside_band", "expansion_band", "directional_state", "confidence_band"]:
        for (segment, policy), group in frame.groupby([segment_col, "policy"], sort=False):
            if len(group) < MIN_SEGMENT_N:
                continue
            rows.append(
                {
                    "segment": segment_col,
                    "bucket": segment,
                    "policy": policy,
                    "n": len(group),
                    "containment_rate": float((group["stayed_inside"] == True).mean()),  # noqa: E712
                    "median_width_vs_champion": float(group["width_vs_champion"].dropna().median()),
                    "median_actual_recommended_width_ratio": float(group["actual_recommended_width_ratio"].dropna().median()),
                    "median_range_utilization": float(group["range_utilization"].dropna().median()),
                }
            )
    return pd.DataFrame(rows)


def markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "_No rows._"
    view = df[columns].copy()
    for col in view.columns:
        if pd.api.types.is_float_dtype(view[col]):
            view[col] = view[col].map(lambda x: "" if pd.isna(x) else f"{x:,.4f}")
    lines = ["| " + " | ".join(view.columns) + " |", "| " + " | ".join(["---"] * len(view.columns)) + " |"]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in view.columns) + " |")
    return "\n".join(lines)


def load_research_data(reader: SupabaseReader) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    recs = reader.select(
        RECOMMENDATIONS_TABLE,
        {
            "select": "*",
            "symbol": f"eq.{SYMBOL}",
            "selected_operating_horizon": f"eq.{HORIZON}",
            "recommended_lower_price": "not.is.null",
            "recommended_upper_price": "not.is.null",
            "order": "requested_at.asc",
            "limit": 1000,
        },
    )
    outcomes = reader.select(
        OUTCOMES_TABLE,
        {
            "select": "*",
            "horizon": f"eq.{HORIZON}",
            "order": "evaluation_start.asc",
            "limit": 1000,
        },
    )
    return recs, {str(row["recommendation_id"]): row for row in outcomes}


def run_research() -> dict[str, Any]:
    reader = SupabaseReader()
    recommendations, outcomes_by_id = load_research_data(reader)
    results = []
    missing_outcome = 0
    ohlcv_windows = 0
    for row in recommendations:
        outcome = outcomes_by_id.get(str(row.get("recommendation_id")))
        if not outcome:
            missing_outcome += 1
            continue
        start = parse_ts(row.get("requested_at"))
        if start is None:
            continue
        trailing = trailing_features(reader.ohlcv_window(start - timedelta(minutes=HORIZON_MINUTES), start))
        forward = reader.ohlcv_window(start, start + timedelta(minutes=HORIZON_MINUTES))
        ohlcv_windows += 2
        for policy in POLICIES:
            results.append(evaluate_policy(row, outcome, policy(row, trailing), forward))

    aggregate_df = aggregate(results)
    segment_df = segment_summary(results, recommendations) if results else pd.DataFrame()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    aggregate_path = REPORT_DIR / "grid_intelligence_policy_challenger_v01_12h_results.csv"
    segment_path = REPORT_DIR / "grid_intelligence_policy_challenger_v01_segments.csv"
    report_path = REPORT_DIR / "grid_intelligence_policy_challenger_v01_report.md"
    aggregate_df.to_csv(aggregate_path, index=False)
    segment_df.to_csv(segment_path, index=False)
    report_path.write_text(build_report(recommendations, outcomes_by_id, results, aggregate_df, segment_df, missing_outcome, ohlcv_windows), encoding="utf-8")
    return {
        "recommendations": recommendations,
        "outcomes": outcomes_by_id,
        "results": results,
        "aggregate": aggregate_df,
        "segments": segment_df,
        "missing_outcome": missing_outcome,
        "ohlcv_windows": ohlcv_windows,
        "report_path": report_path,
        "aggregate_path": aggregate_path,
        "segment_path": segment_path,
    }


def build_report(
    recommendations: list[dict[str, Any]],
    outcomes_by_id: dict[str, dict[str, Any]],
    results: list[dict[str, Any]],
    aggregate_df: pd.DataFrame,
    segment_df: pd.DataFrame,
    missing_outcome: int,
    ohlcv_windows: int,
) -> str:
    available_range90 = False
    usable_by_policy = pd.DataFrame(results).groupby("policy")["usable"].sum().to_dict() if results else {}
    no_grid_by_policy = pd.DataFrame(results).groupby("policy")["no_grid"].sum().to_dict() if results else {}
    leader = aggregate_df.iloc[0].to_dict() if not aggregate_df.empty else {}
    champion = aggregate_df[aggregate_df["policy"] == "champion_v0_1"].iloc[0].to_dict() if not aggregate_df.empty and (aggregate_df["policy"] == "champion_v0_1").any() else {}
    report = f"""# Grid Intelligence V1 - Policy Challenger Research V0.1

## Scope

- Research only. No live Grid Parameter Engine, Probability V2, GridBot, order, UI, deployment, or production policy code was changed.
- Focus horizon: `{HORIZON}`.
- Champion: stored current Grid Parameter Recommender V0.1 ranges from `grid_parameter_recommendations`.
- Outcome source: `grid_parameter_recommendation_outcomes` plus persisted `{OHLCV_TABLE}` `{RESOLUTION}` candles for challenger breach timing and trailing pre-recommendation buffers.
- Usable snapshots with recommended 12H ranges: {len(recommendations)}.
- Matched 12H outcomes: {len(outcomes_by_id)} available; missing for usable snapshot set: {missing_outcome}.
- OHLCV windows read: {ohlcv_windows} read-only windows.

## Challenger Definitions

1. `champion_v0_1`: use stored `recommended_lower_price` and `recommended_upper_price` exactly.
2. `expansion_widen`: keep champion midpoint; set width to champion width x 1.10 if expansion < 0.55, x 1.20 if 0.55 <= expansion < 0.72, x 1.35 if 0.72 <= expansion < 0.86, and x 1.60 if expansion >= 0.86.
3. `v2_range70_floor`: use `min(champion_lower, range_70_lower)` and `max(champion_upper, range_70_upper)`. Wider V2 ranges such as range_90 were not present in the allowed point-in-time V2 snapshot inputs, so this challenger only uses available range_70.
4. `v2_range70_trailing_buffer`: keep champion midpoint; base width is `max(champion_width, range_70_width)` where range_70 exists; add `0.35 * trailing_12h_path_width * (0.50 + expansion_probability)` using only persisted OHLCV before the recommendation timestamp.
5. `stress_filter_no_grid`: emit `NO_GRID` when `path_inside_70 <= 0.38 and expansion_probability >= 0.72`; otherwise use champion range.

Balance score is research-only: `containment_rate * grid_participation_rate - 0.20 * median_extra_width_vs_champion - 0.10 * max(0, 0.35 - median_range_utilization)`.

## 12H Results

{markdown_table(aggregate_df, ['policy', 'usable_snapshots', 'grid_recommendations', 'no_grid', 'grid_participation_rate', 'abstention_rate', 'no_grid_avoided_champion_breach_rate', 'containment_rate', 'breach_rate', 'median_time_to_first_breach', 'median_actual_recommended_width_ratio', 'median_range_utilization', 'median_recommended_width', 'median_width_vs_champion', 'wide_range_penalty', 'balance_score'])}

## Regime Findings

{markdown_table(segment_df.sort_values(['segment', 'bucket', 'policy']) if not segment_df.empty else segment_df, ['segment', 'bucket', 'policy', 'n', 'containment_rate', 'median_width_vs_champion', 'median_actual_recommended_width_ratio', 'median_range_utilization'])}

## Interpretation

- Best research balance by the defined score: `{leader.get('policy')}`.
- Champion 12H containment: {champion.get('containment_rate'):.4f} with median width ratio {champion.get('median_actual_recommended_width_ratio'):.4f} if champion data is available.
- Wider V2 range use is currently constrained because the allowed point-in-time recommendation snapshots preserve `range_70` only; no `range_90` or broader V2 interval was available for this replay.
- The stress filter is useful as a risk gate signal, but it abstained on many snapshots; its high conditional containment should not be compared to full-participation policies without the participation penalty.
- The trailing-buffer challenger tests whether recent realized path width can cover expansion regimes without blindly making every grid huge.

## V1.1 Readiness

Evidence is directional but not strong enough by itself to ship Parameter Engine V1.1. The sample is only {len(recommendations)} actionable 12H snapshots, all from one recommender version and a short calendar period. A candidate V1.1 can be proposed for paper trading/shadow replay if it improves containment materially without excessive width penalty, but production policy should wait for more matured snapshots and a non-overlapping validation period.

## Limitations

- Sample size is small; segment buckets use a minimum n={MIN_SEGMENT_N}.
- Challenger policies use stored snapshots and pre-recommendation OHLCV only; no future path information is used to choose ranges.
- 5-minute OHLCV cannot reconstruct true tick order inside a candle.
- Wider V2 ranges beyond `range_70` were unavailable in the allowed point-in-time inputs.
- This is research evidence, not a live recommendation policy change.
"""
    if available_range90:
        report += "\n"
    return report


if __name__ == "__main__":
    result = run_research()
    print(f"report={result['report_path']}")
    print(f"aggregate={result['aggregate_path']}")
    print(f"segments={result['segment_path']}")
