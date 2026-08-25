from __future__ import annotations

import importlib.util
import json
import os
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

from probability_engine.research.step11_indicators import add_indicators, feature_columns
from probability_engine.research.step12_interactions import (
    HORIZON_ORDER,
    auc_score,
    average_precision,
    binary_state,
    chronological_split,
    classify_horizon,
    fit_logistic_metrics,
    phi_corr,
    point_biserial_auc_edge,
    quantile_threshold,
    state_from_quantiles,
    state_row,
)


SYMBOL = "ETHUSD"
MODEL_VERSION = "probability_v1"
FEATURE_VERSION = "historical_reconstructible_v1"
LABEL_VERSION = "label_v2"
BACKTEST_RECORD_TYPE = "BACKTEST"
REPORT_DIR = ROOT / "reports"

TARGETS = [
    "path_inside_70",
    "range_breached",
    "realized_over_range_width_ge_1",
    "oscillatory_path",
    "trend_efficiency_high",
    "one_sided_runaway",
    "range_held",
    "upside_breakout_occurred",
    "downside_breakdown_occurred",
    "fast_1atr_touch",
    "max_abs_excursion_ge_1_5_atr",
]

SHORTLIST = [
    "atr_slope_96b",
    "rv_slope_96b",
    "atr_pct_12b",
    "bollinger_bandwidth_20b",
    "atr_12b",
    "keltner_width_20b",
    "rv_12b",
    "atr_pct_24b",
    "atr_24b",
    "rv_24b",
    "price_efficiency_96b",
    "price_efficiency_48b",
    "rolling_vwap_z_96b",
    "donchian_pos_96b",
    "bb_keltner_width_ratio_48b",
    "volume_z_96b",
    "volume_rel_96b",
    "adx_48b",
    "ema_spread_atr_12_48b",
]


def load_step11_module():
    path = ROOT / "reports" / "step11_indicator_feature_discovery.py"
    spec = importlib.util.spec_from_file_location("step11_indicator_feature_discovery", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def load_research_data() -> tuple[pd.DataFrame, dict]:
    step11 = load_step11_module()
    print("step12: loading BACKTEST probability_v1/label_v2 dataset", flush=True)
    predictions, outcomes, snapshots = step11.load_dataset()
    for frame, column in [(predictions, "created_at"), (outcomes, "evaluated_at"), (snapshots, "timestamp")]:
        frame[column] = pd.to_datetime(frame[column], utc=True)

    prediction_outcomes = predictions.merge(outcomes, left_on="id", right_on="prediction_id", how="inner", suffixes=("", "_outcome"))
    data = prediction_outcomes.merge(snapshots.add_prefix("snapshot_"), left_on="snapshot_id", right_on="snapshot_id", how="left")
    for column in [
        "spot_price",
        "return_5m",
        "return_15m",
        "return_1h",
        "return_4h",
        "vwap_zscore",
        "atr",
        "atr_pct",
        "realized_volatility",
        "volume_zscore",
        "regime",
    ]:
        snap_col = f"snapshot_{column}"
        if snap_col in data:
            data[column] = data[snap_col]

    print("step12: loading bounded 5m ETH OHLCV window", flush=True)
    read_start = predictions["created_at"].min() - pd.Timedelta(days=3)
    read_end = predictions["created_at"].max() + pd.Timedelta(days=2)
    ohlcv_rows = step11.fetch_all(
        "eth_ohlcv",
        {
            "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
            "symbol": f"eq.{SYMBOL}",
            "resolution": "eq.5m",
            "candle_time": f"gte.{read_start.isoformat()}",
        },
        "candle_time",
        max_pages=120,
    )
    ohlcv = pd.DataFrame(ohlcv_rows)
    ohlcv["candle_time"] = pd.to_datetime(ohlcv["candle_time"], utc=True)
    ohlcv = ohlcv[ohlcv["candle_time"] <= read_end].copy()
    print(f"step12: OHLCV rows after date cap {len(ohlcv)}", flush=True)
    indicators = add_indicators(ohlcv)
    indicator_features = feature_columns(indicators)
    feature_frame = indicators[["timestamp"] + indicator_features].sort_values("timestamp")
    data = pd.merge_asof(
        data.sort_values("created_at"),
        feature_frame,
        left_on="created_at",
        right_on="timestamp",
        direction="backward",
        allow_exact_matches=True,
    )
    data = step11.derive_research_targets(data)
    data["chronological_third"] = chronological_split(data)
    integrity = step11.integrity_report(predictions, outcomes, snapshots, data)
    integrity["canonical_timestamps"] = int(data["created_at"].nunique())
    integrity["max_feature_timestamp_after_prediction"] = int((data["timestamp"] > data["created_at"]).sum())
    integrity["horizon_counts_joined"] = {str(k): int(v) for k, v in data["horizon"].value_counts().sort_index().items()}
    return data, integrity


def add_feature_states(data: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, dict]:
    data = data.copy()
    thresholds = {}
    for feature in features:
        if feature not in data:
            continue
        low = quantile_threshold(data[feature], 0.30)
        high = quantile_threshold(data[feature], 0.70)
        median = quantile_threshold(data[feature], 0.50)
        thresholds[feature] = {"q30": low, "q50": median, "q70": high}
        data[f"{feature}__qstate"] = state_from_quantiles(data[feature])
        data[f"{feature}__low"] = binary_state(data[feature], "<=", low)
        data[f"{feature}__high"] = binary_state(data[feature], ">=", high)
        data[f"{feature}__rising"] = binary_state(data[feature], ">", median)
        data[f"{feature}__falling"] = binary_state(data[feature], "<=", median)
    return data, thresholds


def interaction_specs() -> list[dict]:
    return [
        {"name": "low_atr_pct_and_rising_atr_slope", "family": "VOL_LEVEL_X_VOL_SLOPE", "features": ["atr_pct_12b", "atr_slope_96b"], "states": [("atr_pct_12b", "low"), ("atr_slope_96b", "rising")], "why": "low realized range with rising ATR slope tests pre-expansion risk"},
        {"name": "low_atr_pct_and_falling_atr_slope", "family": "VOL_LEVEL_X_VOL_SLOPE", "features": ["atr_pct_12b", "atr_slope_96b"], "states": [("atr_pct_12b", "low"), ("atr_slope_96b", "falling")], "why": "low volatility plus falling ATR tests stable compression"},
        {"name": "high_atr_pct_and_falling_atr_slope", "family": "VOL_LEVEL_X_VOL_SLOPE", "features": ["atr_pct_12b", "atr_slope_96b"], "states": [("atr_pct_12b", "high"), ("atr_slope_96b", "falling")], "why": "high but cooling volatility may favor post-expansion containment"},
        {"name": "high_atr_pct_and_rising_atr_slope", "family": "VOL_LEVEL_X_VOL_SLOPE", "features": ["atr_pct_12b", "atr_slope_96b"], "states": [("atr_pct_12b", "high"), ("atr_slope_96b", "rising")], "why": "high and accelerating volatility is a risk state"},
        {"name": "low_rv_and_rising_rv_slope", "family": "VOL_LEVEL_X_VOL_SLOPE", "features": ["rv_12b", "rv_slope_96b"], "states": [("rv_12b", "low"), ("rv_slope_96b", "rising")], "why": "low realized variance with rising variance slope tests early expansion"},
        {"name": "low_rv_and_falling_rv_slope", "family": "VOL_LEVEL_X_VOL_SLOPE", "features": ["rv_12b", "rv_slope_96b"], "states": [("rv_12b", "low"), ("rv_slope_96b", "falling")], "why": "quiet and decelerating realized variance tests stable range"},
        {"name": "bb_compressed_and_rising_atr_slope", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["bollinger_bandwidth_20b", "atr_slope_96b"], "states": [("bollinger_bandwidth_20b", "low"), ("atr_slope_96b", "rising")], "why": "Bollinger compression with ATR acceleration tests breakout transition"},
        {"name": "bb_compressed_and_falling_atr_slope", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["bollinger_bandwidth_20b", "atr_slope_96b"], "states": [("bollinger_bandwidth_20b", "low"), ("atr_slope_96b", "falling")], "why": "Bollinger compression with falling ATR tests range persistence"},
        {"name": "keltner_compressed_and_rising_atr_slope", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["keltner_width_20b", "atr_slope_96b"], "states": [("keltner_width_20b", "low"), ("atr_slope_96b", "rising")], "why": "channel compression with rising ATR tests expansion risk"},
        {"name": "keltner_compressed_and_falling_atr_slope", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["keltner_width_20b", "atr_slope_96b"], "states": [("keltner_width_20b", "low"), ("atr_slope_96b", "falling")], "why": "channel compression with falling ATR tests quiet containment"},
        {"name": "low_atr_and_low_efficiency", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["atr_pct_12b", "price_efficiency_96b"], "states": [("atr_pct_12b", "low"), ("price_efficiency_96b", "low")], "why": "quiet and inefficient paths are candidate grid harvest states"},
        {"name": "low_atr_and_high_efficiency", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["atr_pct_12b", "price_efficiency_96b"], "states": [("atr_pct_12b", "low"), ("price_efficiency_96b", "high")], "why": "quiet but directional paths test pre-runaway drift"},
        {"name": "rising_atr_and_high_efficiency", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["atr_slope_96b", "price_efficiency_96b"], "states": [("atr_slope_96b", "rising"), ("price_efficiency_96b", "high")], "why": "volatility acceleration with efficient path tests trend onset"},
        {"name": "falling_atr_and_low_efficiency", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["atr_slope_96b", "price_efficiency_96b"], "states": [("atr_slope_96b", "falling"), ("price_efficiency_96b", "low")], "why": "cooling volatility with inefficient path tests oscillation"},
        {"name": "bb_compressed_and_low_efficiency", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["bollinger_bandwidth_20b", "price_efficiency_96b"], "states": [("bollinger_bandwidth_20b", "low"), ("price_efficiency_96b", "low")], "why": "compression plus low efficiency tests stable range harvesting"},
        {"name": "bb_compressed_and_high_efficiency", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["bollinger_bandwidth_20b", "price_efficiency_96b"], "states": [("bollinger_bandwidth_20b", "low"), ("price_efficiency_96b", "high")], "why": "compression plus high efficiency tests breakout from quiet state"},
        {"name": "bb_compressed_and_high_volume", "family": "VOLATILITY_X_VOLUME", "features": ["bollinger_bandwidth_20b", "volume_z_96b"], "states": [("bollinger_bandwidth_20b", "low"), ("volume_z_96b", "high")], "why": "compressed market with relative volume tests transition pressure"},
        {"name": "bb_compressed_and_low_volume", "family": "VOLATILITY_X_VOLUME", "features": ["bollinger_bandwidth_20b", "volume_z_96b"], "states": [("bollinger_bandwidth_20b", "low"), ("volume_z_96b", "low")], "why": "compressed market with low volume tests dormant range"},
        {"name": "rising_atr_and_high_volume", "family": "VOLATILITY_X_VOLUME", "features": ["atr_slope_96b", "volume_z_96b"], "states": [("atr_slope_96b", "rising"), ("volume_z_96b", "high")], "why": "ATR acceleration with volume confirms expansion pressure"},
        {"name": "rising_rv_and_high_volume", "family": "VOLATILITY_X_VOLUME", "features": ["rv_slope_96b", "volume_z_96b"], "states": [("rv_slope_96b", "rising"), ("volume_z_96b", "high")], "why": "RV acceleration with volume tests volatility expansion"},
        {"name": "low_vwap_displacement_and_low_atr", "family": "STRUCTURE_X_VOLATILITY", "features": ["rolling_vwap_z_96b", "atr_pct_12b"], "states": [("rolling_vwap_z_96b", "low_abs"), ("atr_pct_12b", "low")], "why": "near-VWAP low-volatility structure tests centered containment"},
        {"name": "high_vwap_displacement_and_rising_atr", "family": "STRUCTURE_X_VOLATILITY", "features": ["rolling_vwap_z_96b", "atr_slope_96b"], "states": [("rolling_vwap_z_96b", "high_abs"), ("atr_slope_96b", "rising")], "why": "large VWAP displacement with rising ATR tests directional stress"},
        {"name": "donchian_edge_and_rising_rv", "family": "STRUCTURE_X_VOLATILITY", "features": ["donchian_pos_96b", "rv_slope_96b"], "states": [("donchian_pos_96b", "edge"), ("rv_slope_96b", "rising")], "why": "channel-edge position with rising variance tests breakout risk"},
        {"name": "donchian_center_and_falling_rv", "family": "STRUCTURE_X_VOLATILITY", "features": ["donchian_pos_96b", "rv_slope_96b"], "states": [("donchian_pos_96b", "center"), ("rv_slope_96b", "falling")], "why": "channel-center position with falling variance tests range persistence"},
        {"name": "bb_squeeze_low_and_low_efficiency", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["bb_keltner_width_ratio_48b", "price_efficiency_96b"], "states": [("bb_keltner_width_ratio_48b", "low"), ("price_efficiency_96b", "low")], "why": "relative Bollinger/Keltner squeeze with low path efficiency"},
        {"name": "bb_squeeze_low_and_high_efficiency", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["bb_keltner_width_ratio_48b", "price_efficiency_96b"], "states": [("bb_keltner_width_ratio_48b", "low"), ("price_efficiency_96b", "high")], "why": "squeeze with efficient path tests breakout transition"},
        {"name": "low_atr_and_mid_donchian", "family": "STRUCTURE_X_VOLATILITY", "features": ["atr_pct_12b", "donchian_pos_96b"], "states": [("atr_pct_12b", "low"), ("donchian_pos_96b", "center")], "why": "quiet price centered in channel tests containment"},
        {"name": "low_atr_and_donchian_edge", "family": "STRUCTURE_X_VOLATILITY", "features": ["atr_pct_12b", "donchian_pos_96b"], "states": [("atr_pct_12b", "low"), ("donchian_pos_96b", "edge")], "why": "quiet price at channel edge tests pre-breakout"},
        {"name": "high_adx_and_rising_atr", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["adx_48b", "atr_slope_96b"], "states": [("adx_48b", "high"), ("atr_slope_96b", "rising")], "why": "trend strength plus ATR acceleration tests avoidance state"},
        {"name": "low_adx_and_falling_atr", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["adx_48b", "atr_slope_96b"], "states": [("adx_48b", "low"), ("atr_slope_96b", "falling")], "why": "weak trend plus falling ATR tests range"},
        {"name": "ema_spread_high_and_rising_atr", "family": "COMPRESSION_X_TREND_BREAKOUT", "features": ["ema_spread_atr_12_48b", "atr_slope_96b"], "states": [("ema_spread_atr_12_48b", "high_abs"), ("atr_slope_96b", "rising")], "why": "MA spread with accelerating ATR tests trend transition"},
        {"name": "ema_spread_low_and_falling_atr", "family": "VOLATILITY_X_PATH_EFFICIENCY", "features": ["ema_spread_atr_12_48b", "atr_slope_96b"], "states": [("ema_spread_atr_12_48b", "low_abs"), ("atr_slope_96b", "falling")], "why": "flat MA spread with falling ATR tests stable range"},
    ]


def add_interactions(data: pd.DataFrame, specs: list[dict], thresholds: dict) -> pd.DataFrame:
    data = data.copy()
    for feature in ["rolling_vwap_z_96b", "ema_spread_atr_12_48b"]:
        if feature in data:
            abs_value = data[feature].abs()
            low_abs = quantile_threshold(abs_value, 0.30)
            high_abs = quantile_threshold(abs_value, 0.70)
            thresholds.setdefault(feature, {})["abs_q30"] = low_abs
            thresholds.setdefault(feature, {})["abs_q70"] = high_abs
            data[f"{feature}__low_abs"] = binary_state(abs_value, "<=", low_abs)
            data[f"{feature}__high_abs"] = binary_state(abs_value, ">=", high_abs)
    if "donchian_pos_96b" in data:
        pos = pd.to_numeric(data["donchian_pos_96b"], errors="coerce")
        data["donchian_pos_96b__center"] = ((pos >= 0.35) & (pos <= 0.65)).where(pos.notna(), np.nan)
        data["donchian_pos_96b__edge"] = ((pos <= 0.20) | (pos >= 0.80)).where(pos.notna(), np.nan)
        thresholds.setdefault("donchian_pos_96b", {})["center"] = "0.35..0.65"
        thresholds.setdefault("donchian_pos_96b", {})["edge"] = "<=0.20 or >=0.80"

    for spec in specs:
        masks = []
        for feature, state in spec["states"]:
            column = f"{feature}__{state}"
            if column not in data:
                masks.append(pd.Series(np.nan, index=data.index))
            else:
                masks.append(data[column])
        mask = masks[0]
        for other in masks[1:]:
            mask = mask.astype("boolean") & other.astype("boolean")
        data[spec["name"]] = mask.astype(float)
        a, b = spec["features"]
        if a in data and b in data:
            data[f"{spec['name']}__product"] = pd.to_numeric(data[a], errors="coerce").rank(pct=True) * pd.to_numeric(data[b], errors="coerce").rank(pct=True)
    return data


def best_individual_rate(frame: pd.DataFrame, feature: str, target: str, state_hint: str) -> dict:
    if feature not in frame:
        return {"feature": feature, "n": 0, "conditioned_rate": None, "absolute_lift": None}
    if state_hint in {"low", "falling"}:
        column = f"{feature}__low" if state_hint == "low" else f"{feature}__falling"
    elif state_hint in {"high", "rising"}:
        column = f"{feature}__high" if state_hint == "high" else f"{feature}__rising"
    elif state_hint in {"low_abs", "high_abs", "center", "edge"}:
        column = f"{feature}__{state_hint}"
    else:
        column = f"{feature}__high"
    if column not in frame:
        return {"feature": feature, "n": 0, "conditioned_rate": None, "absolute_lift": None}
    row = state_row(frame, column, target)
    row["feature"] = feature
    return row


def evaluate_interactions(data: pd.DataFrame, specs: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = []
    temporal_rows = []
    regime_rows = []
    nonoverlap_rows = []
    model_rows = []
    for spec in specs:
        state = spec["name"]
        for horizon in sorted(data["horizon"].dropna().unique(), key=lambda h: HORIZON_ORDER.get(h, 0)):
            hdata = data[data["horizon"] == horizon].copy()
            step = max(1, int(HORIZON_ORDER.get(horizon, 60) / 5))
            reduced = hdata.sort_values("created_at").iloc[::step]
            for target in TARGETS:
                if target not in hdata or hdata[target].dropna().nunique() < 2:
                    continue
                base_rate = float(hdata[target].dropna().astype(float).mean())
                saturated = base_rate <= 0.02 or base_rate >= 0.98
                metrics = state_row(hdata, state, target)
                if metrics["n"] < 80:
                    caveat = "small-N"
                elif saturated:
                    caveat = "saturated target"
                else:
                    caveat = "usable"
                individual = [
                    best_individual_rate(hdata, feature, target, state_hint)
                    for feature, state_hint in spec["states"]
                ]
                best_lift = max([abs(item["absolute_lift"] or 0) for item in individual] or [0])
                inc_lift = abs(metrics["absolute_lift"] or 0) - best_lift
                auc_edge = point_biserial_auc_edge(hdata, state, target)
                non = state_row(reduced, state, target)
                nonoverlap_rows.append(
                    {
                        "interaction": state,
                        "target": target,
                        "horizon": horizon,
                        "full_n": metrics["n"],
                        "full_lift": metrics["absolute_lift"],
                        "nonoverlap_n": non["n"],
                        "nonoverlap_lift": non["absolute_lift"],
                        "survives_nonoverlap": bool(
                            non["n"] >= 25
                            and metrics["absolute_lift"] is not None
                            and non["absolute_lift"] is not None
                            and np.sign(metrics["absolute_lift"]) == np.sign(non["absolute_lift"])
                            and abs(non["absolute_lift"]) >= abs(metrics["absolute_lift"]) * 0.35
                        ),
                    }
                )
                for third, subset in hdata.groupby("chronological_third"):
                    trow = state_row(subset, state, target)
                    temporal_rows.append({"interaction": state, "target": target, "horizon": horizon, "third": third, **trow})
                for regime, subset in hdata.groupby("regime"):
                    rrow = state_row(subset, state, target)
                    if rrow["base_n"] >= 60:
                        regime_rows.append({"interaction": state, "target": target, "horizon": horizon, "regime": regime, **rrow})
                rows.append(
                    {
                        "interaction": state,
                        "family": spec["family"],
                        "why": spec["why"],
                        "features": "+".join(spec["features"]),
                        "target": target,
                        "horizon": horizon,
                        "n": metrics["n"],
                        "base_n": metrics["base_n"],
                        "base_rate": metrics["base_rate"],
                        "conditioned_rate": metrics["conditioned_rate"],
                        "absolute_lift": metrics["absolute_lift"],
                        "relative_lift": metrics["relative_lift"],
                        "auc_edge": auc_edge,
                        "best_individual_abs_lift": best_lift,
                        "incremental_abs_lift": inc_lift,
                        "best_individual_detail": json.dumps(individual, default=str),
                        "saturated_target": saturated,
                        "sample_caveat": caveat,
                    }
                )
    model_columns = [
        "interaction",
        "target",
        "horizon",
        "model1_brier",
        "model2_brier",
        "model3_brier",
        "base_brier",
        "model3_auc",
        "model3_pr_auc",
        "brier_improvement_vs_main1",
        "brier_improvement_vs_main2",
        "product_feature_present",
    ]
    return pd.DataFrame(rows), pd.DataFrame(temporal_rows), pd.DataFrame(regime_rows), pd.DataFrame(nonoverlap_rows), pd.DataFrame(columns=model_columns)


def run_model_diagnostics(data: pd.DataFrame, scorecard: pd.DataFrame, specs: list[dict]) -> pd.DataFrame:
    columns = [
        "interaction",
        "target",
        "horizon",
        "model1_brier",
        "model2_brier",
        "model3_brier",
        "base_brier",
        "model3_auc",
        "model3_pr_auc",
        "brier_improvement_vs_main1",
        "brier_improvement_vs_main2",
        "product_feature_present",
    ]
    target_set = {"path_inside_70", "range_breached", "oscillatory_path", "trend_efficiency_high", "one_sided_runaway", "realized_over_range_width_ge_1"}
    candidates = scorecard[
        (scorecard["target"].isin(target_set))
        & (scorecard["sample_caveat"] == "usable")
        & (scorecard["absolute_lift"].abs() >= 0.03)
    ].copy()
    if candidates.empty:
        return pd.DataFrame(columns=columns)
    candidates["model_priority"] = candidates["absolute_lift"].abs().fillna(0) + candidates["auc_edge"].fillna(0)
    candidates = candidates.sort_values("model_priority", ascending=False).head(60)
    spec_by_name = {spec["name"]: spec for spec in specs}
    rows = []
    for _, candidate in candidates.iterrows():
        spec = spec_by_name[candidate["interaction"]]
        hdata = data[data["horizon"] == candidate["horizon"]]
        target = candidate["target"]
        state = candidate["interaction"]
        model1 = fit_logistic_metrics(hdata, target, [spec["features"][0]])
        model2 = fit_logistic_metrics(hdata, target, spec["features"])
        model3_features = spec["features"] + ([state] if state in hdata else [])
        model3 = fit_logistic_metrics(hdata, target, model3_features)
        rows.append(
            {
                "interaction": state,
                "target": target,
                "horizon": candidate["horizon"],
                "model1_brier": model1["brier"],
                "model2_brier": model2["brier"],
                "model3_brier": model3["brier"],
                "base_brier": model3["base_brier"],
                "model3_auc": model3["auc"],
                "model3_pr_auc": model3["pr_auc"],
                "brier_improvement_vs_main1": None if model1["brier"] is None or model3["brier"] is None else model1["brier"] - model3["brier"],
                "brier_improvement_vs_main2": None if model2["brier"] is None or model3["brier"] is None else model2["brier"] - model3["brier"],
                "product_feature_present": f"{state}__product" in hdata,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def summarize(scorecard: pd.DataFrame, temporal: pd.DataFrame, regime: pd.DataFrame, nonoverlap: pd.DataFrame, models: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    temporal_summary = []
    for keys, group in temporal.groupby(["interaction", "target", "horizon"]):
        lifts = group["absolute_lift"].dropna()
        signs = set(np.sign(lifts[lifts != 0]))
        temporal_summary.append(
            {
                "interaction": keys[0],
                "target": keys[1],
                "horizon": keys[2],
                "thirds": int(group["third"].nunique()),
                "min_n": int(group["n"].min()),
                "lifts": ",".join(f"{v:.4f}" for v in lifts.tolist()),
                "chronologically_stable": bool(len(lifts) == 3 and len(signs) <= 1 and group["n"].min() >= 25),
            }
        )
    temporal_matrix = pd.DataFrame(temporal_summary)

    regime_summary = []
    for keys, group in regime.groupby(["interaction", "target", "horizon"]):
        usable = group[group["n"] >= 20]
        lifts = usable["absolute_lift"].dropna()
        signs = set(np.sign(lifts[lifts != 0]))
        regime_summary.append(
            {
                "interaction": keys[0],
                "target": keys[1],
                "horizon": keys[2],
                "regime_count": int(usable["regime"].nunique()),
                "positive_regimes": int((lifts > 0).sum()),
                "negative_regimes": int((lifts < 0).sum()),
                "regime_class": "UNIVERSAL" if len(signs) == 1 and usable["regime"].nunique() >= 3 else "REGIME-SPECIFIC" if usable["regime"].nunique() >= 1 else "SMALL-N",
            }
        )
    regime_matrix = pd.DataFrame(regime_summary)

    enriched = scorecard.merge(temporal_matrix, on=["interaction", "target", "horizon"], how="left")
    enriched = enriched.merge(regime_matrix, on=["interaction", "target", "horizon"], how="left")
    enriched = enriched.merge(nonoverlap[["interaction", "target", "horizon", "survives_nonoverlap", "nonoverlap_n", "nonoverlap_lift"]], on=["interaction", "target", "horizon"], how="left")
    enriched = enriched.merge(models[["interaction", "target", "horizon", "brier_improvement_vs_main2"]], on=["interaction", "target", "horizon"], how="left")
    enriched["rank_score"] = (
        enriched["absolute_lift"].abs().fillna(0).clip(upper=0.20) * 120
        + enriched["incremental_abs_lift"].fillna(0).clip(lower=-0.10, upper=0.10) * 80
        + enriched["auc_edge"].fillna(0).clip(upper=0.25) * 80
        + enriched["chronologically_stable"].fillna(False).astype(int) * 18
        + enriched["survives_nonoverlap"].fillna(False).astype(int) * 12
        + enriched["regime_count"].fillna(0).clip(upper=4) * 2
        + enriched["brier_improvement_vs_main2"].fillna(0).clip(lower=-0.01, upper=0.01) * 400
        - enriched["saturated_target"].astype(int) * 18
        - (enriched["n"] < 80).astype(int) * 18
    )

    agg_rows = []
    for interaction, group in enriched.groupby("interaction"):
        useful = group[(group["sample_caveat"] == "usable") & (group["chronologically_stable"] == True)]
        horizons = sorted(useful["horizon"].dropna().unique(), key=lambda h: HORIZON_ORDER.get(h, 0))
        best = group.sort_values("rank_score", ascending=False).iloc[0]
        strong = group[(group["rank_score"] >= 35) & (group["sample_caveat"] == "usable")]
        agg_rows.append(
            {
                "interaction": interaction,
                "family": best["family"],
                "features": best["features"],
                "best_target": best["target"],
                "best_horizon": best["horizon"],
                "best_n": int(best["n"]),
                "best_base_rate": best["base_rate"],
                "best_conditioned_rate": best["conditioned_rate"],
                "best_abs_lift": best["absolute_lift"],
                "best_incremental_abs_lift": best["incremental_abs_lift"],
                "best_rank_score": best["rank_score"],
                "useful_horizons": ",".join(horizons),
                "horizon_class": classify_horizon(horizons),
                "strong_hits": int(len(strong)),
                "chronological_hits": int((group["chronologically_stable"] == True).sum()),
                "nonoverlap_hits": int((group["survives_nonoverlap"] == True).sum()),
                "classification": classify_interaction(best, strong, horizons),
            }
        )
    summary = pd.DataFrame(agg_rows).sort_values("best_rank_score", ascending=False)
    horizon_matrix = enriched.groupby(["interaction", "horizon"])["rank_score"].max().reset_index().pivot(index="interaction", columns="horizon", values="rank_score").reset_index()
    return enriched.sort_values("rank_score", ascending=False), summary, temporal_matrix, regime_matrix, horizon_matrix


def classify_interaction(best: pd.Series, strong: pd.DataFrame, horizons: list[str]) -> str:
    if best["sample_caveat"] == "small-N":
        return "REJECT"
    if best["saturated_target"] and best["rank_score"] < 30:
        return "WEAK"
    if best["incremental_abs_lift"] <= -0.02:
        return "REDUNDANT"
    if not bool(best.get("chronologically_stable")):
        return "UNSTABLE"
    if best["rank_score"] >= 45 and len(strong) >= 6 and len(horizons) >= 3:
        return "CORE INTERACTION"
    if best["rank_score"] >= 35 and len(strong) >= 2:
        return "SECONDARY INTERACTION"
    if best.get("regime_class") == "REGIME-SPECIFIC":
        return "REGIME-SPECIFIC"
    if horizons and len(horizons) <= 2:
        return "HORIZON-SPECIFIC"
    return "WEAK"


def redundancy(summary: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
    interactions = summary["interaction"].tolist()
    rows = []
    for i, a in enumerate(interactions):
        for b in interactions[i + 1 :]:
            corr = phi_corr(data[a], data[b])
            if corr is not None and abs(corr) >= 0.65:
                rows.append({"interaction_a": a, "interaction_b": b, "phi_corr": corr, "redundancy": "HIGH" if abs(corr) >= 0.80 else "MODERATE"})
    return pd.DataFrame(rows).sort_values("phi_corr", key=lambda s: s.abs(), ascending=False) if rows else pd.DataFrame(columns=["interaction_a", "interaction_b", "phi_corr", "redundancy"])


def build_strategy_files(enriched: pd.DataFrame, summary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    grid_targets = {"oscillatory_path", "range_held", "trend_efficiency_high", "one_sided_runaway", "realized_over_range_width_ge_1"}
    iron_targets = {"path_inside_70", "range_breached", "max_abs_excursion_ge_1_5_atr", "realized_over_range_width_ge_1"}
    defensive_targets = {"fast_1atr_touch", "trend_efficiency_high", "one_sided_runaway", "upside_breakout_occurred", "downside_breakdown_occurred", "max_abs_excursion_ge_1_5_atr"}
    grid = enriched[enriched["target"].isin(grid_targets)].head(80)
    iron = enriched[enriched["target"].isin(iron_targets)].head(80)
    defensive = enriched[enriched["target"].isin(defensive_targets)].head(80)
    step13_individuals = [
        "atr_slope_96b",
        "rv_slope_96b",
        "atr_pct_12b",
        "bollinger_bandwidth_20b",
        "keltner_width_20b",
        "rv_12b",
        "price_efficiency_96b",
        "rolling_vwap_z_96b",
        "donchian_pos_96b",
        "volume_z_96b",
    ]
    redundant_step13_exclusions = {"rising_rv_and_high_volume", "keltner_compressed_and_falling_atr_slope"}
    step13_interactions = [
        interaction
        for interaction in summary[summary["classification"].isin(["CORE INTERACTION", "SECONDARY INTERACTION", "REGIME-SPECIFIC", "HORIZON-SPECIFIC"])]["interaction"].tolist()
        if interaction not in redundant_step13_exclusions
    ][:8]
    candidates = pd.DataFrame(
        [{"feature": item, "type": "individual_step11", "source": "Step 11 shortlist"} for item in step13_individuals]
        + [{"feature": item, "type": "interaction_step12", "source": "Step 12 evidence"} for item in step13_interactions]
    )
    return grid, iron, defensive, candidates


def main() -> None:
    load_dotenv(dotenv_path=ROOT / ".env")
    data, integrity = load_research_data()
    print("step12: constructing shortlist states", flush=True)
    available = [feature for feature in SHORTLIST if feature in data]
    data, thresholds = add_feature_states(data, available)
    specs = [spec for spec in interaction_specs() if all(feature in data for feature in spec["features"])]
    print(f"step12: evaluating {len(specs)} compact interactions", flush=True)
    data = add_interactions(data, specs, thresholds)
    scorecard, temporal, regime, nonoverlap, models = evaluate_interactions(data, specs)
    print("step12: running focused logistic diagnostics", flush=True)
    models = run_model_diagnostics(data, scorecard, specs)
    print("step12: summarizing scorecards", flush=True)
    enriched, summary, temporal_matrix, regime_matrix, horizon_matrix = summarize(scorecard, temporal, regime, nonoverlap, models)
    red = redundancy(summary, data)
    grid, iron, defensive, candidates = build_strategy_files(enriched, summary)

    saturated = (
        scorecard[scorecard["saturated_target"]][["target", "horizon", "base_rate"]]
        .drop_duplicates()
        .sort_values(["target", "horizon"])
    )
    proposed_states = summary[summary["classification"].isin(["CORE INTERACTION", "SECONDARY INTERACTION", "REGIME-SPECIFIC"])].head(10)
    final_verdict = "STEP 12 COMPLETE — LIMITED BUT USEFUL INTERACTIONS IDENTIFIED"
    if (summary["classification"] == "CORE INTERACTION").sum() >= 3:
        final_verdict = "STEP 12 COMPLETE — ROBUST STRATEGY-STATE INTERACTIONS IDENTIFIED"
    if summary["best_incremental_abs_lift"].fillna(0).max() <= 0.005:
        final_verdict = "STEP 12 COMPLETE — INTERACTIONS ADD LITTLE BEYOND INDIVIDUAL FEATURES"

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset_contract": {
            "record_type": BACKTEST_RECORD_TYPE,
            "model_version": MODEL_VERSION,
            "label_version": LABEL_VERSION,
            "feature_version": FEATURE_VERSION,
            "rich_forward_data_used": False,
            "production_modified": False,
        },
        "integrity": integrity,
        "step11_shortlist_used": available,
        "candidate_interactions_tested": len(specs),
        "interaction_families_tested": sorted({spec["family"] for spec in specs}),
        "thresholds": thresholds,
        "saturated_targets_excluded_or_downweighted": saturated.to_dict("records"),
        "top_interactions": summary.head(10).to_dict("records"),
        "weak_rejected_or_unstable": summary[summary["classification"].isin(["REDUNDANT", "WEAK", "UNSTABLE", "REJECT"])].to_dict("records"),
        "proposed_market_state_features": proposed_states.to_dict("records"),
        "final_questions": answer_final_questions(summary, enriched, red, candidates),
        "limitations": [
            "Primary data is BACKTEST probability_v1/label_v2/historical_reconstructible_v1 only.",
            "The persisted dataset has 25,186 joined predictions/outcomes, not the 25,236 count stated in the prompt; 4H and 8H have 4,181 each.",
            "Targets with base rates <=2% or >=98% are marked saturated and penalized.",
            "Non-overlap checks use every horizon/5m-th row as a simple secondary robustness diagnostic.",
            "No Probability V2, Feature Bridge, scheduler, production table, schema, collector, Grid, Iron Fly, or futures logic was changed.",
        ],
        "recommended_next_step": "Step 13 should carry forward the compact individual/interactions list and test a frozen challenger design offline only.",
        "final_verdict": final_verdict,
    }

    REPORT_DIR.mkdir(exist_ok=True)
    (REPORT_DIR / "step12_interaction_discovery.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    enriched.to_csv(REPORT_DIR / "step12_interaction_scorecard.csv", index=False)
    horizon_matrix.to_csv(REPORT_DIR / "step12_horizon_matrix.csv", index=False)
    regime_matrix.to_csv(REPORT_DIR / "step12_regime_matrix.csv", index=False)
    temporal_matrix.to_csv(REPORT_DIR / "step12_temporal_stability.csv", index=False)
    red.to_csv(REPORT_DIR / "step12_redundancy.csv", index=False)
    grid.to_csv(REPORT_DIR / "step12_grid_states.csv", index=False)
    iron.to_csv(REPORT_DIR / "step12_ironfly_states.csv", index=False)
    defensive.to_csv(REPORT_DIR / "step12_defensive_long_option_states.csv", index=False)
    candidates.to_csv(REPORT_DIR / "step12_step13_candidate_features.csv", index=False)
    print(json.dumps({"ok": True, "integrity": integrity, "candidate_interactions_tested": len(specs), "top": summary.head(5).to_dict("records"), "final_verdict": final_verdict}, indent=2, default=str))


def answer_final_questions(summary: pd.DataFrame, enriched: pd.DataFrame, red: pd.DataFrame, candidates: pd.DataFrame) -> dict:
    top = summary.head(10)
    def unique(values):
        result = []
        for value in values:
            if value not in result:
                result.append(value)
        return result

    def names(mask):
        return unique(top[mask]["interaction"].tolist())[:5]

    grid_good = enriched[
        (
            ((enriched["target"].isin(["oscillatory_path", "range_held"])) & (enriched["absolute_lift"] > 0))
            | ((enriched["target"].isin(["trend_efficiency_high", "one_sided_runaway"])) & (enriched["absolute_lift"] < 0))
        )
        & (enriched["sample_caveat"] == "usable")
    ]
    grid_bad = enriched[
        (
            ((enriched["target"].isin(["trend_efficiency_high", "one_sided_runaway"])) & (enriched["absolute_lift"] > 0))
            | ((enriched["target"].isin(["oscillatory_path", "range_held"])) & (enriched["absolute_lift"] < 0))
        )
        & (enriched["sample_caveat"] == "usable")
    ]
    iron_good = enriched[
        (
            ((enriched["target"] == "path_inside_70") & (enriched["absolute_lift"] > 0))
            | ((enriched["target"].isin(["range_breached", "realized_over_range_width_ge_1", "max_abs_excursion_ge_1_5_atr"])) & (enriched["absolute_lift"] < 0))
        )
        & (enriched["sample_caveat"] == "usable")
    ]
    iron_bad = enriched[
        (
            ((enriched["target"] == "path_inside_70") & (enriched["absolute_lift"] < 0))
            | ((enriched["target"].isin(["range_breached", "realized_over_range_width_ge_1", "max_abs_excursion_ge_1_5_atr"])) & (enriched["absolute_lift"] > 0))
        )
        & (enriched["sample_caveat"] == "usable")
    ]
    defensive = enriched[
        (
            ((enriched["target"].isin(["one_sided_runaway", "trend_efficiency_high", "fast_1atr_touch"])) & (enriched["absolute_lift"] > 0))
            | ((enriched["target"].isin(["path_inside_70", "range_held"])) & (enriched["absolute_lift"] < 0))
        )
        & (enriched["sample_caveat"] == "usable")
    ]
    return {
        "A_interactions_improve_over_individuals": bool((summary["best_incremental_abs_lift"].fillna(0) > 0.01).any()),
        "B_strongest_vol_state_x_change": names(top["family"] == "VOL_LEVEL_X_VOL_SLOPE"),
        "C_stable_low_vol_vs_pre_breakout": "Compare low_atr_pct_and_falling_atr_slope / low_rv_and_falling_rv_slope against low_atr_pct_and_rising_atr_slope / bb_compressed_and_rising_atr_slope.",
        "D_high_quality_grid_environments": unique(grid_good["interaction"].head(20).tolist())[:5],
        "E_dangerous_grid_runaway_states": unique(grid_bad["interaction"].head(20).tolist())[:5],
        "F_better_iron_fly_containment_states": unique(iron_good["interaction"].head(20).tolist())[:5],
        "G_iron_fly_avoidance_states": unique(iron_bad["interaction"].head(20).tolist())[:5],
        "H_defensive_long_option_hedge_states": unique(defensive["interaction"].head(20).tolist())[:5],
        "I_survive_chronological": unique(summary[summary["chronological_hits"] > 0]["interaction"].tolist())[:10],
        "J_survive_less_overlapping": unique(summary[summary["nonoverlap_hits"] > 0]["interaction"].tolist())[:10],
        "K_redundant_versions": red.head(10).to_dict("records"),
        "L_step13_features": candidates["feature"].tolist(),
    }


if __name__ == "__main__":
    main()
