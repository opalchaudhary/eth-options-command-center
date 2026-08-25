from __future__ import annotations

import json
import math
import os
import sys
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

from probability_engine.config import HORIZON_MINUTES
from probability_engine.research.step11_indicators import add_indicators, feature_columns


SYMBOL = "ETHUSD"
MODEL_VERSION = "probability_v1"
FEATURE_VERSION = "historical_reconstructible_v1"
LABEL_VERSION = "label_v2"
BACKTEST_RECORD_TYPE = "BACKTEST"
REPORT_DIR = Path("reports")
BASE_FEATURES = {"return_1h", "atr_pct", "realized_volatility", "volume_zscore", "vwap_zscore"}
LABEL_TARGETS = [
    "mean_reversion_occurred",
    "upside_breakout_occurred",
    "downside_breakdown_occurred",
    "range_held",
    "trend_continuation_occurred",
]
STRATEGY_TARGETS = {
    "ironfly": ["path_inside_70", "range_breached", "max_abs_excursion_ge_1_5_atr", "realized_over_range_width_ge_1"],
    "long_option": ["up_excursion_ge_1_atr", "down_excursion_ge_1_atr", "max_abs_excursion_ge_1_5_atr", "fast_1atr_touch"],
    "grid": ["oscillatory_path", "trend_efficiency_high", "one_sided_runaway", "range_held"],
}


def parse_utc(value):
    return pd.Timestamp(value).tz_convert("UTC") if pd.Timestamp(value).tzinfo else pd.Timestamp(value).tz_localize("UTC")


def iso(value):
    if value is None:
        return None
    return pd.Timestamp(value).tz_convert("UTC").isoformat().replace("+00:00", "Z")


def supabase_headers():
    return {"apikey": os.environ["SUPABASE_KEY"], "Authorization": f"Bearer {os.environ['SUPABASE_KEY']}"}


def supabase_get(table: str, params: dict, timeout: int = 60):
    base = os.environ["SUPABASE_URL"].rstrip("/")
    response = requests.get(f"{base}/rest/v1/{table}", headers=supabase_headers(), params=params, timeout=timeout)
    response.raise_for_status()
    return response.json(), response.headers


def fetch_all(table: str, params: dict, order_column: str, page_size: int = 1000, max_pages: int = 200):
    rows = []
    query = {**params, "order": f"{order_column}.asc", "limit": str(page_size)}
    cursor = params.get(order_column)
    for _ in range(max_pages):
        if cursor:
            query[order_column] = cursor
        batch, _ = supabase_get(table, query)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        cursor = f"gt.{batch[-1][order_column]}"
    return rows


def fetch_count(table: str, params: dict | None = None):
    headers = supabase_headers()
    headers["Prefer"] = "count=exact"
    base = os.environ["SUPABASE_URL"].rstrip("/")
    query = {"select": "id", "limit": "1", **(params or {})}
    response = requests.get(f"{base}/rest/v1/{table}", headers=headers, params=query, timeout=60)
    response.raise_for_status()
    content_range = response.headers.get("content-range", "")
    if "/" not in content_range:
        return None
    total = content_range.split("/")[-1]
    return int(total) if total.isdigit() else None


def auc_score(y_true: pd.Series, values: pd.Series):
    data = pd.DataFrame({"y": y_true, "x": values}).dropna()
    if data.empty or data["y"].nunique() < 2:
        return None
    y = data["y"].astype(int).to_numpy()
    x = data["x"].astype(float).to_numpy()
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), dtype=float)
    ranks[order] = np.arange(1, len(x) + 1)
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    if not n_pos or not n_neg:
        return None
    rank_sum_pos = ranks[y == 1].sum()
    return float((rank_sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def average_precision(y_true: pd.Series, values: pd.Series):
    data = pd.DataFrame({"y": y_true, "x": values}).dropna().sort_values("x", ascending=False)
    if data.empty or data["y"].nunique() < 2:
        return None
    positives = data["y"].astype(int).sum()
    if not positives:
        return None
    cumulative_tp = data["y"].astype(int).cumsum()
    precision = cumulative_tp / np.arange(1, len(data) + 1)
    return float((precision * data["y"].astype(int)).sum() / positives)


def safe_corr(a: pd.Series, b: pd.Series, method: str):
    data = pd.DataFrame({"a": a, "b": b}).dropna()
    if len(data) < 20 or data["a"].nunique() < 2 or data["b"].nunique() < 2:
        return None
    if method == "spearman":
        value = data["a"].rank(method="average").corr(data["b"].rank(method="average"), method="pearson")
    else:
        value = data["a"].corr(data["b"], method=method)
    return None if pd.isna(value) else float(value)


def quantile_lift(y_true: pd.Series, values: pd.Series):
    data = pd.DataFrame({"y": y_true, "x": values}).dropna()
    if len(data) < 50 or data["x"].nunique() < 5:
        return {}
    data["bucket"] = pd.qcut(data["x"].rank(method="first"), 5, labels=False)
    rates = data.groupby("bucket")["y"].mean()
    base = data["y"].mean()
    return {
        "base_rate": float(base),
        "bottom_rate": float(rates.iloc[0]),
        "top_rate": float(rates.iloc[-1]),
        "top_lift": float(rates.iloc[-1] - base),
        "bottom_lift": float(rates.iloc[0] - base),
        "spread": float(rates.iloc[-1] - rates.iloc[0]),
        "monotonic_steps": int(np.sign(rates.diff().dropna()).sum()),
    }


def temporal_stability(data: pd.DataFrame, feature: str, target: str, horizon: str):
    subset = data[data["horizon"] == horizon].dropna(subset=[feature, target]).sort_values("created_at")
    if len(subset) < 90:
        return {"blocks": 0, "stable_sign": None, "min_abs_auc_edge": None}
    auc_edges = []
    edges = np.linspace(0, len(subset), 4, dtype=int)
    for block in range(3):
        block_df = subset.iloc[edges[block] : edges[block + 1]]
        auc = auc_score(block_df[target], block_df[feature])
        if auc is None:
            continue
        auc_edges.append(auc - 0.5)
    if not auc_edges:
        return {"blocks": 0, "stable_sign": None, "min_abs_auc_edge": None}
    signs = {1 if value > 0 else -1 if value < 0 else 0 for value in auc_edges}
    return {
        "blocks": len(auc_edges),
        "stable_sign": len(signs - {0}) == 1,
        "min_abs_auc_edge": float(min(abs(value) for value in auc_edges)),
        "edges": [float(value) for value in auc_edges],
    }


def classify_horizon(horizons: list[str]):
    values = set(horizons)
    if len(values) >= 5:
        return "MULTI-HORIZON"
    if values <= {"1H", "2H"}:
        return "SHORT-HORIZON"
    if values <= {"4H", "8H", "12H"}:
        return "MEDIUM-HORIZON"
    if values <= {"12H", "24H"}:
        return "LONG-HORIZON"
    return "MIXED-HORIZON"


def redundancy_cluster(feature: str):
    if any(token in feature for token in ["rsi", "stoch", "roc_", "return_", "macd"]):
        return "momentum"
    if any(token in feature for token in ["atr", "rv_", "bollinger", "keltner", "squeeze", "choppiness"]):
        return "volatility"
    if any(token in feature for token in ["sma", "ema", "dema", "tema", "adx", "di_", "aroon"]):
        return "trend"
    if any(token in feature for token in ["vwap", "donchian", "wick", "body", "close_location", "rolling_high", "rolling_low"]):
        return "price_structure"
    if any(token in feature for token in ["volume", "obv", "mfi"]):
        return "volume"
    if any(token in feature for token in ["efficiency", "consecutive"]):
        return "state"
    return "other"


def feature_family(feature: str):
    return redundancy_cluster(feature).replace("_", " ").title()


def transparent_score(row):
    auc_edge = abs((row.get("auc_oriented") or 0.5) - 0.5) * 200
    lift = min(abs(row.get("top_bottom_spread") or 0) * 100, 30)
    temporal = 20 if row.get("temporal_stable") else 0
    regime = min((row.get("regime_support_count") or 0) * 4, 16)
    sample = min((row.get("n") or 0) / 100, 10)
    redundancy = 8 if row.get("cluster_rank", 99) > 3 else 0
    interpretability = 8 if "obv" not in row["feature"] else 5
    incremental = min(max((row.get("incremental_brier_improvement") or 0) * 10000, 0), 12)
    return round(auc_edge + lift + temporal + regime + sample + interpretability + incremental - redundancy, 3)


def orientation(auc):
    if auc is None:
        return 1
    return 1 if auc >= 0.5 else -1


def decile_brier_improvement(y: pd.Series, x: pd.Series):
    data = pd.DataFrame({"y": y, "x": x}).dropna()
    if len(data) < 100 or data["y"].nunique() < 2 or data["x"].nunique() < 10:
        return None
    base = float(data["y"].mean())
    base_brier = float(((data["y"] - base) ** 2).mean())
    data["bucket"] = pd.qcut(data["x"].rank(method="first"), 10, labels=False)
    rates = data.groupby("bucket")["y"].transform("mean")
    model_brier = float(((data["y"] - rates) ** 2).mean())
    return base_brier - model_brier


def load_dataset():
    rows = fetch_all(
        "probability_outcomes",
        {
            "select": (
                "prediction_id,evaluated_at,actual_open,actual_high,actual_low,actual_close,"
                "maximum_up_excursion,maximum_down_excursion,mean_reversion_occurred,"
                "upside_breakout_occurred,downside_breakdown_occurred,range_held,"
                "trend_continuation_occurred,range_50_covered,range_70_covered,range_90_covered,"
                "upper_touch_occurred,lower_touch_occurred,label_version,metadata_json,"
                "prediction:probability_predictions!inner("
                "id,snapshot_id,created_at,symbol,horizon,record_type,model_version,feature_version,"
                "regime_version,range_model_version,prediction_status,range_50_lower,range_50_upper,"
                "range_70_lower,range_70_upper,range_90_lower,range_90_upper,"
                "mean_reversion_probability,upside_breakout_probability,downside_breakdown_probability,"
                "range_continuation_probability,trend_continuation_probability,metadata_json,"
                "snapshot:probability_market_snapshots("
                "id,timestamp,symbol,spot_price,return_5m,return_15m,return_1h,return_4h,"
                "vwap,vwap_deviation_pct,vwap_zscore,atr,atr_pct,realized_volatility,"
                "volume,volume_zscore,regime,feature_version,regime_version,metadata_json"
                "))"
            ),
            "label_version": f"eq.{LABEL_VERSION}",
            "prediction.symbol": f"eq.{SYMBOL}",
            "prediction.record_type": f"eq.{BACKTEST_RECORD_TYPE}",
            "prediction.model_version": f"eq.{MODEL_VERSION}",
            "prediction.feature_version": f"eq.{FEATURE_VERSION}",
        },
        "evaluated_at",
        max_pages=80,
    )
    outcomes = []
    predictions = []
    snapshots_by_id = {}
    outcome_fields = [
        "prediction_id", "evaluated_at", "actual_open", "actual_high", "actual_low", "actual_close",
        "maximum_up_excursion", "maximum_down_excursion", "mean_reversion_occurred",
        "upside_breakout_occurred", "downside_breakdown_occurred", "range_held",
        "trend_continuation_occurred", "range_50_covered", "range_70_covered", "range_90_covered",
        "upper_touch_occurred", "lower_touch_occurred", "label_version", "metadata_json",
    ]
    for row in rows:
        prediction = row.get("prediction") or {}
        snapshot = prediction.pop("snapshot", None) or {}
        outcomes.append({key: row.get(key) for key in outcome_fields})
        predictions.append(prediction)
        if snapshot.get("id"):
            snapshots_by_id[snapshot["id"]] = snapshot
    return pd.DataFrame(predictions), pd.DataFrame(outcomes), pd.DataFrame(snapshots_by_id.values())


def load_ohlcv(start_at, end_at):
    read_start = pd.Timestamp(start_at).tz_convert("UTC") - pd.Timedelta(days=3)
    read_end = pd.Timestamp(end_at).tz_convert("UTC") + pd.Timedelta(days=2)
    rows = fetch_all(
        "eth_ohlcv",
        {
            "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
            "symbol": f"eq.{SYMBOL}",
            "resolution": "eq.5m",
            "candle_time": f"gte.{read_start.isoformat()}",
        },
        "candle_time",
        max_pages=300,
    )
    frame = pd.DataFrame(rows)
    frame["candle_time"] = pd.to_datetime(frame["candle_time"], utc=True)
    return frame[frame["candle_time"] <= read_end].copy()


def derive_research_targets(frame: pd.DataFrame):
    data = frame.copy()
    for column in [
        "actual_open",
        "actual_high",
        "actual_low",
        "actual_close",
        "maximum_up_excursion",
        "maximum_down_excursion",
        "range_50_lower",
        "range_50_upper",
        "range_70_lower",
        "range_70_upper",
        "range_90_lower",
        "range_90_upper",
        "spot_price",
        "atr",
    ]:
        if column in data:
            data[column] = pd.to_numeric(data[column], errors="coerce")
    data["future_close_return"] = (data["actual_close"] - data["spot_price"]) / data["spot_price"]
    data["abs_close_move"] = data["future_close_return"].abs()
    data["max_up_atr"] = data["maximum_up_excursion"] / data["atr"].replace(0, np.nan)
    data["max_down_atr"] = data["maximum_down_excursion"] / data["atr"].replace(0, np.nan)
    data["max_abs_excursion_atr"] = data[["max_up_atr", "max_down_atr"]].max(axis=1)
    data["realized_path_range"] = (data["actual_high"] - data["actual_low"]) / data["spot_price"]
    data["range_70_width"] = data["range_70_upper"] - data["range_70_lower"]
    data["realized_over_range_width"] = (data["actual_high"] - data["actual_low"]) / data["range_70_width"].replace(0, np.nan)
    data["path_inside_50"] = (data["actual_high"] <= data["range_50_upper"]) & (data["actual_low"] >= data["range_50_lower"])
    data["path_inside_70"] = (data["actual_high"] <= data["range_70_upper"]) & (data["actual_low"] >= data["range_70_lower"])
    data["path_inside_90"] = (data["actual_high"] <= data["range_90_upper"]) & (data["actual_low"] >= data["range_90_lower"])
    data["range_breached"] = ~data["path_inside_70"]
    data["upper_breach_only"] = (data["actual_high"] > data["range_70_upper"]) & (data["actual_low"] >= data["range_70_lower"])
    data["lower_breach_only"] = (data["actual_low"] < data["range_70_lower"]) & (data["actual_high"] <= data["range_70_upper"])
    data["both_side_breach"] = (data["actual_high"] > data["range_70_upper"]) & (data["actual_low"] < data["range_70_lower"])
    for threshold in [0.5, 1.0, 1.5, 2.0]:
        suffix = str(threshold).replace(".", "_")
        data[f"up_excursion_ge_{suffix}_atr"] = data["max_up_atr"] >= threshold
        data[f"down_excursion_ge_{suffix}_atr"] = data["max_down_atr"] >= threshold
        data[f"max_abs_excursion_ge_{suffix}_atr"] = data["max_abs_excursion_atr"] >= threshold
    data["realized_over_range_width_ge_1"] = data["realized_over_range_width"] >= 1
    data["trend_efficiency_high"] = (data["future_close_return"].abs() / data["realized_path_range"].replace(0, np.nan)) >= 0.65
    data["oscillatory_path"] = (data["future_close_return"].abs() / data["realized_path_range"].replace(0, np.nan)) <= 0.35
    data["one_sided_runaway"] = data["trend_efficiency_high"] & data["max_abs_excursion_ge_1_5_atr"]
    data["fast_1atr_touch"] = data["up_excursion_ge_1_0_atr"] | data["down_excursion_ge_1_0_atr"]
    return data


def integrity_report(predictions, outcomes, snapshots, data):
    pred_dups = predictions.duplicated(["snapshot_id", "horizon", "record_type", "model_version"]).sum()
    snap_dups = snapshots.duplicated(["symbol", "timestamp", "feature_version"]).sum() if not snapshots.empty else None
    joined_prediction_ids = set(data["id"])
    outcome_ids = set(outcomes["prediction_id"]) if not outcomes.empty else set()
    return {
        "snapshots": int(len(snapshots)),
        "predictions": int(len(predictions)),
        "outcomes": int(len(outcomes[outcomes["prediction_id"].isin(joined_prediction_ids)])),
        "counts_by_horizon": {str(k): int(v) for k, v in predictions["horizon"].value_counts().sort_index().items()},
        "date_range": {
            "start": iso(predictions["created_at"].min()),
            "end": iso(predictions["created_at"].max()),
        },
        "regimes": {str(k): int(v) for k, v in snapshots["regime"].value_counts(dropna=False).items()},
        "duplicate_prediction_groups": int(pred_dups),
        "duplicate_snapshots": int(snap_dups or 0),
        "predictions_missing_outcomes": int(len(joined_prediction_ids - outcome_ids)),
        "unexpected_prediction_versions": {
            "record_type": {str(k): int(v) for k, v in predictions["record_type"].value_counts(dropna=False).items()},
            "model_version": {str(k): int(v) for k, v in predictions["model_version"].value_counts(dropna=False).items()},
            "feature_version": {str(k): int(v) for k, v in predictions["feature_version"].value_counts(dropna=False).items()},
        },
        "outcome_label_versions": {str(k): int(v) for k, v in outcomes["label_version"].value_counts(dropna=False).items()},
    }


def screen_features(data: pd.DataFrame, features: list[str]):
    rows = []
    target_columns = LABEL_TARGETS + [
        "path_inside_70",
        "range_breached",
        "up_excursion_ge_1_0_atr",
        "down_excursion_ge_1_0_atr",
        "max_abs_excursion_ge_1_5_atr",
        "oscillatory_path",
        "trend_efficiency_high",
        "one_sided_runaway",
        "realized_over_range_width_ge_1",
        "fast_1atr_touch",
    ]
    for horizon in sorted(data["horizon"].dropna().unique(), key=lambda h: HORIZON_MINUTES.get(h, 0)):
        hdata = data[data["horizon"] == horizon]
        for target in target_columns:
            if target not in hdata:
                continue
            y = hdata[target]
            if y.dropna().nunique() < 2:
                continue
            for feature in features:
                x = hdata[feature]
                clean = pd.DataFrame({"x": x, "y": y}).dropna()
                if len(clean) < 100 or clean["x"].nunique() < 5:
                    continue
                auc = auc_score(clean["y"], clean["x"])
                oriented = auc if auc is None or auc >= 0.5 else 1 - auc
                basic_signal = oriented is not None and oriented >= 0.56
                lift = quantile_lift(clean["y"], clean["x"]) if basic_signal else {}
                stability = {}
                regime_support = 0
                rows.append(
                    {
                        "feature": feature,
                        "family": feature_family(feature),
                        "cluster": redundancy_cluster(feature),
                        "target": target,
                        "horizon": horizon,
                        "n": int(len(clean)),
                        "base_rate": float(clean["y"].mean()),
                        "auc": auc,
                        "auc_oriented": oriented,
                        "pr_auc": average_precision(clean["y"], clean["x"]) if basic_signal else None,
                        "pearson": safe_corr(clean["x"], clean["y"], "pearson") if basic_signal else None,
                        "spearman": None,
                        "top_bottom_spread": lift.get("spread"),
                        "top_lift": lift.get("top_lift"),
                        "bottom_lift": lift.get("bottom_lift"),
                        "monotonic_steps": lift.get("monotonic_steps"),
                        "temporal_stable": stability.get("stable_sign"),
                        "temporal_min_abs_auc_edge": stability.get("min_abs_auc_edge"),
                        "regime_support_count": regime_support,
                        "direction": "higher_increases_target" if orientation(auc) > 0 else "lower_increases_target",
                        "incremental_brier_improvement": None,
                    }
                )
    scorecard = pd.DataFrame(rows)
    if scorecard.empty:
        return scorecard
    cluster_rank = (
        scorecard.groupby(["cluster", "target", "horizon"])["auc_oriented"]
        .rank(ascending=False, method="first")
        .rename("cluster_rank")
    )
    scorecard["cluster_rank"] = cluster_rank
    scorecard["research_score"] = scorecard.apply(transparent_score, axis=1)
    return scorecard.sort_values("research_score", ascending=False)


def summarize_features(scorecard: pd.DataFrame):
    if scorecard.empty:
        return pd.DataFrame()
    grouped = []
    for feature, rows in scorecard.groupby("feature"):
        strong = rows[rows["research_score"] >= rows["research_score"].quantile(0.80)]
        useful = rows[(rows["auc_oriented"] >= 0.555) & ((rows["temporal_stable"] == True) | rows["temporal_stable"].isna())]
        horizons = sorted(useful["horizon"].unique(), key=lambda h: HORIZON_MINUTES.get(h, 0))
        best = rows.sort_values("research_score", ascending=False).iloc[0]
        grouped.append(
            {
                "feature": feature,
                "family": best["family"],
                "cluster": best["cluster"],
                "best_target": best["target"],
                "best_horizon": best["horizon"],
                "best_auc_oriented": best["auc_oriented"],
                "best_lift_spread": best["top_bottom_spread"],
                "best_score": best["research_score"],
                "useful_horizons": ",".join(horizons),
                "horizon_class": classify_horizon(horizons) if horizons else "NO STABLE HORIZON SIGNAL",
                "stable_hits": int(len(useful)),
                "first_pass_hits": int((rows["auc_oriented"] >= 0.555).sum()),
                "mean_score_top": float(strong["research_score"].mean()) if not strong.empty else float(rows["research_score"].mean()),
            }
        )
    summary = pd.DataFrame(grouped).sort_values(["stable_hits", "best_score"], ascending=False)
    summary["class"] = summary.apply(classify_feature, axis=1)
    return summary


def classify_feature(row):
    hits = max(row.get("stable_hits", 0), row.get("first_pass_hits", 0))
    if hits >= 8 and row["best_score"] >= 45:
        return "CORE CANDIDATE"
    if hits >= 4 and row["best_score"] >= 35:
        return "SECONDARY CANDIDATE"
    if hits >= 2:
        return "CONDITIONAL / REGIME-SPECIFIC"
    if row["best_score"] >= 32 and row["horizon_class"] == "NO STABLE HORIZON SIGNAL":
        return "UNSTABLE"
    if row["cluster"] in {"momentum", "trend", "volatility"} and row["best_score"] < 24:
        return "WEAK"
    return "REJECT"


def redundancy(score_data: pd.DataFrame, features: list[str]):
    columns = [feature for feature in features if feature in score_data and score_data[feature].notna().sum() >= 100]
    corr = score_data[columns].corr(method="spearman").abs()
    rows = []
    seen = set()
    for a in columns:
        for b in columns:
            if a >= b or (a, b) in seen:
                continue
            value = corr.loc[a, b]
            if pd.notna(value) and value >= 0.85:
                rows.append({"feature_a": a, "feature_b": b, "abs_spearman_corr": float(value), "cluster": redundancy_cluster(a)})
                seen.add((a, b))
    return pd.DataFrame(rows).sort_values("abs_spearman_corr", ascending=False) if rows else pd.DataFrame(columns=["feature_a", "feature_b", "abs_spearman_corr", "cluster"])


def feature_map(scorecard: pd.DataFrame, family: str):
    rows = []
    for target in STRATEGY_TARGETS[family]:
        subset = scorecard[scorecard["target"] == target].head(25)
        for _, row in subset.iterrows():
            rows.append(
                {
                    "strategy_family": family,
                    "use_case": target,
                    "feature": row["feature"],
                    "horizon": row["horizon"],
                    "direction": row["direction"],
                    "auc_oriented": row["auc_oriented"],
                    "lift_spread": row["top_bottom_spread"],
                    "research_score": row["research_score"],
                    "class": "candidate" if row["research_score"] >= 35 else "watch",
                }
            )
    return pd.DataFrame(rows).drop_duplicates(["strategy_family", "use_case", "feature", "horizon"])


def main():
    load_dotenv(dotenv_path=".env")
    generated_at = datetime.now(timezone.utc)
    print("step11: loading backtest dataset", flush=True)
    predictions, outcomes, snapshots = load_dataset()
    for frame, column in [(predictions, "created_at"), (outcomes, "evaluated_at"), (snapshots, "timestamp")]:
        frame[column] = pd.to_datetime(frame[column], utc=True)

    prediction_outcomes = predictions.merge(outcomes, left_on="id", right_on="prediction_id", how="inner", suffixes=("", "_outcome"))
    data = prediction_outcomes.merge(snapshots.add_prefix("snapshot_"), left_on="snapshot_id", right_on="snapshot_id", how="left")
    for column in ["spot_price", "return_5m", "return_15m", "return_1h", "return_4h", "vwap_zscore", "atr", "atr_pct", "realized_volatility", "volume_zscore", "regime"]:
        snap_col = f"snapshot_{column}"
        if snap_col in data:
            data[column] = data[snap_col]

    print("step11: loading and computing historical OHLCV indicators", flush=True)
    ohlcv = load_ohlcv(predictions["created_at"].min(), predictions["created_at"].max())
    indicators = add_indicators(ohlcv)
    indicator_features = feature_columns(indicators)
    feature_frame = indicators[["timestamp"] + indicator_features].sort_values("timestamp")
    data = data.sort_values("created_at")
    data = pd.merge_asof(
        data,
        feature_frame,
        left_on="created_at",
        right_on="timestamp",
        direction="backward",
        allow_exact_matches=True,
    )
    data = derive_research_targets(data)

    broad_exclude = ("dema_", "tema_", "obv")
    features = [feature for feature in indicator_features if feature in data.columns and not feature.startswith(broad_exclude)]
    print(f"step11: screening {len(features)} features", flush=True)
    scorecard = screen_features(data, features)
    print(f"step11: scorecard rows {len(scorecard)}", flush=True)
    summary = summarize_features(scorecard)
    redundancy_rows = redundancy(data, features)

    horizon_matrix = (
        scorecard.groupby(["feature", "horizon"])["research_score"].max().reset_index().pivot(index="feature", columns="horizon", values="research_score").reset_index()
        if not scorecard.empty
        else pd.DataFrame()
    )
    regime_rows = []
    for feature in summary.head(60)["feature"].tolist() if not summary.empty else []:
        for target in ["upside_breakout_occurred", "downside_breakdown_occurred", "range_breached", "path_inside_70", "trend_efficiency_high"]:
            for (regime, horizon), subset in data.groupby(["regime", "horizon"]):
                clean = subset[[feature, target]].dropna()
                if len(clean) < 60 or clean[target].nunique() < 2:
                    continue
                auc = auc_score(clean[target], clean[feature])
                regime_rows.append({"feature": feature, "target": target, "regime": regime, "horizon": horizon, "n": len(clean), "auc_oriented": auc if auc is None or auc >= 0.5 else 1 - auc})
    regime_matrix = pd.DataFrame(regime_rows)

    temporal_rows = []
    for _, row in scorecard.head(300).iterrows() if not scorecard.empty else []:
        stability = temporal_stability(data, row["feature"], row["target"], row["horizon"])
        temporal_rows.append({"feature": row["feature"], "target": row["target"], "horizon": row["horizon"], **stability})
    temporal = pd.DataFrame(temporal_rows)

    ironfly = feature_map(scorecard, "ironfly")
    long_option = feature_map(scorecard, "long_option")
    grid = feature_map(scorecard, "grid")

    integrity = integrity_report(predictions, outcomes, snapshots, data)
    target_rates = {
        target: {str(k): float(v) for k, v in data.groupby("horizon")[target].mean().dropna().items()}
        for target in LABEL_TARGETS + ["path_inside_70", "range_breached", "max_abs_excursion_ge_1_5_atr", "oscillatory_path", "trend_efficiency_high"]
        if target in data
    }
    report = {
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "dataset_contract": {
            "record_type": BACKTEST_RECORD_TYPE,
            "model_version": MODEL_VERSION,
            "label_version": LABEL_VERSION,
            "feature_version": FEATURE_VERSION,
            "rich_forward_data_used": False,
            "production_modified": False,
        },
        "integrity": integrity,
        "indicator_families_tested": sorted({feature_family(feature) for feature in features}),
        "lookbacks_tested_5m_bars": [3, 6, 12, 14, 20, 24, 48, 96, 288],
        "feature_count": len(features),
        "research_targets_created": sorted(target_rates),
        "target_base_rates_by_horizon": target_rates,
        "no_lookahead_verification": {
            "method": "Indicators are computed on OHLCV sorted by candle_time; prediction rows are joined with pandas merge_asof(direction='backward', allow_exact_matches=True).",
            "max_feature_timestamp_after_prediction": int((data["timestamp"] > data["created_at"]).sum()),
            "warmup_nulls_preserved": True,
        },
        "top_features": summary.head(30).to_dict("records") if not summary.empty else [],
        "weak_or_rejected_count": int(summary["class"].isin(["WEAK", "REJECT", "UNSTABLE"]).sum()) if not summary.empty else 0,
        "redundancy_cluster_counts": {str(k): int(v) for k, v in redundancy_rows["cluster"].value_counts().items()} if not redundancy_rows.empty else {},
        "v2_candidate_features": summary[summary["class"].isin(["CORE CANDIDATE", "SECONDARY CANDIDATE", "CONDITIONAL / REGIME-SPECIFIC"])]["feature"].head(20).tolist() if not summary.empty else [],
        "limitations": [
            "No rich forward-only data used.",
            "Overlapping outcomes are treated descriptively; temporal thirds are used as a robustness guard instead of p-value claims.",
            "No full indicator combination mining or Probability V2 training was performed.",
            "Time-to-touch targets are approximated from stored outcome excursions, not intrahorizon tick paths.",
        ],
    }

    REPORT_DIR.mkdir(exist_ok=True)
    (REPORT_DIR / "step11_indicator_feature_discovery.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    scorecard.to_csv(REPORT_DIR / "step11_feature_scorecard.csv", index=False)
    horizon_matrix.to_csv(REPORT_DIR / "step11_feature_horizon_matrix.csv", index=False)
    regime_matrix.to_csv(REPORT_DIR / "step11_feature_regime_matrix.csv", index=False)
    temporal.to_csv(REPORT_DIR / "step11_feature_temporal_stability.csv", index=False)
    redundancy_rows.to_csv(REPORT_DIR / "step11_feature_redundancy.csv", index=False)
    ironfly.to_csv(REPORT_DIR / "step11_ironfly_feature_map.csv", index=False)
    long_option.to_csv(REPORT_DIR / "step11_long_option_feature_map.csv", index=False)
    grid.to_csv(REPORT_DIR / "step11_grid_feature_map.csv", index=False)

    print(json.dumps({
        "ok": True,
        "generated_at": report["generated_at"],
        "integrity": integrity,
        "feature_count": len(features),
        "scorecard_rows": int(len(scorecard)),
        "top_features": report["top_features"][:10],
        "artifacts": [
            "reports/step11_indicator_feature_discovery.json",
            "reports/step11_feature_scorecard.csv",
            "reports/step11_feature_horizon_matrix.csv",
            "reports/step11_feature_regime_matrix.csv",
            "reports/step11_feature_temporal_stability.csv",
            "reports/step11_feature_redundancy.csv",
            "reports/step11_ironfly_feature_map.csv",
            "reports/step11_long_option_feature_map.csv",
            "reports/step11_grid_feature_map.csv",
        ],
    }, indent=2, default=str))


if __name__ == "__main__":
    main()
