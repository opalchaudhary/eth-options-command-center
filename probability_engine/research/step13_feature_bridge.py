from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from probability_engine.research.step11_indicators import add_indicators, feature_columns


FEATURE_CONTRACT_VERSION = "probability_v2_features_v1"
HORIZON_HOURS = {"1H": 1.0, "2H": 2.0, "4H": 4.0, "8H": 8.0, "12H": 12.0, "24H": 24.0}
LABEL_V2_TARGETS = {
    "mean_reversion_occurred": "mean_reversion",
    "upside_breakout_occurred": "upside_breakout",
    "downside_breakdown_occurred": "downside_breakdown",
    "range_held": "range_continuation",
    "trend_continuation_occurred": "trend_continuation",
}
RESEARCH_TARGETS = [
    "path_inside_70",
    "range_breached",
    "upper_breach_only",
    "lower_breach_only",
    "both_side_breach",
    "max_abs_excursion_atr",
    "max_abs_excursion_ge_1_5_atr",
    "realized_path_range",
    "realized_over_range_width",
    "realized_over_range_width_ge_1",
    "oscillatory_path",
    "trend_efficiency_high",
    "one_sided_runaway",
    "fast_1atr_touch",
    "up_excursion_ge_1_0_atr",
    "down_excursion_ge_1_0_atr",
]


@dataclass(frozen=True)
class BridgeResult:
    dataset: pd.DataFrame
    feature_dictionary: pd.DataFrame
    thresholds: pd.DataFrame
    validation: dict


def stable_csv_hash(frame: pd.DataFrame) -> str:
    payload = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def expanding_quantile(series: pd.Series, q: float, min_periods: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.expanding(min_periods=min_periods).quantile(q)


def expanding_binary_state(
    series: pd.Series,
    q: float,
    direction: str,
    min_periods: int,
) -> tuple[pd.Series, pd.Series]:
    values = pd.to_numeric(series, errors="coerce")
    threshold = expanding_quantile(values, q, min_periods)
    if direction == "le":
        state = values <= threshold
    elif direction == "ge":
        state = values >= threshold
    else:
        raise ValueError(f"Unsupported direction: {direction}")
    return state.where(values.notna() & threshold.notna(), np.nan).astype(float), threshold


def expanding_abs_state(
    series: pd.Series,
    q: float,
    direction: str,
    min_periods: int,
) -> tuple[pd.Series, pd.Series]:
    values = pd.to_numeric(series, errors="coerce").abs()
    return expanding_binary_state(values, q, direction, min_periods)


def load_step13_candidates(path: Path) -> pd.DataFrame:
    candidates = pd.read_csv(path)
    required = {"feature", "type", "source"}
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError(f"Step 13 candidate artifact is missing columns: {sorted(missing)}")
    return candidates


def prepare_point_in_time_features(
    ohlcv: pd.DataFrame,
    prediction_timestamps: pd.Series,
    individual_features: list[str],
) -> pd.DataFrame:
    indicators = add_indicators(ohlcv)
    available_features = [feature for feature in individual_features if feature in indicators.columns]
    feature_frame = indicators[["timestamp"] + sorted(set(available_features))].sort_values("timestamp")
    predictions = pd.DataFrame({"prediction_timestamp": pd.to_datetime(prediction_timestamps, utc=True)}).sort_values("prediction_timestamp")
    joined = pd.merge_asof(
        predictions,
        feature_frame,
        left_on="prediction_timestamp",
        right_on="timestamp",
        direction="backward",
        allow_exact_matches=True,
    )
    return joined.rename(columns={"timestamp": "feature_source_timestamp"})


def add_expanding_interaction_features(frame: pd.DataFrame, interaction_features: list[str], min_periods: int = 300) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = frame.sort_values("prediction_timestamp").copy()
    thresholds: list[dict] = []

    def add_state(name: str, series: pd.Series, q: float, direction: str) -> pd.Series:
        state, threshold = expanding_binary_state(series, q, direction, min_periods)
        thresholds.extend(
            {
                "prediction_timestamp": ts,
                "state": name,
                "threshold": value,
                "threshold_semantics": f"expanding q{int(q * 100)} using observations <= prediction_timestamp, min_periods={min_periods}",
            }
            for ts, value in zip(data["prediction_timestamp"], threshold)
            if pd.notna(value)
        )
        return state

    def add_abs_state(name: str, series: pd.Series, q: float, direction: str) -> pd.Series:
        state, threshold = expanding_abs_state(series, q, direction, min_periods)
        thresholds.extend(
            {
                "prediction_timestamp": ts,
                "state": name,
                "threshold": value,
                "threshold_semantics": f"expanding abs q{int(q * 100)} using observations <= prediction_timestamp, min_periods={min_periods}",
            }
            for ts, value in zip(data["prediction_timestamp"], threshold)
            if pd.notna(value)
        )
        return state

    states: dict[str, pd.Series] = {}
    for feature in [
        "atr_pct_12b",
        "atr_slope_96b",
        "rv_slope_96b",
        "volume_z_96b",
        "adx_48b",
        "keltner_width_20b",
        "bollinger_bandwidth_20b",
        "rv_12b",
    ]:
        if feature in data:
            states[f"{feature}__low"] = add_state(f"{feature}__low", data[feature], 0.30, "le")
            states[f"{feature}__high"] = add_state(f"{feature}__high", data[feature], 0.70, "ge")
            states[f"{feature}__falling"] = add_state(f"{feature}__falling", data[feature], 0.50, "le")
            states[f"{feature}__rising"] = add_state(f"{feature}__rising", data[feature], 0.50, "ge")

    for feature in ["rolling_vwap_z_96b", "ema_spread_atr_12_48b"]:
        if feature in data:
            states[f"{feature}__low_abs"] = add_abs_state(f"{feature}__low_abs", data[feature], 0.30, "le")
            states[f"{feature}__high_abs"] = add_abs_state(f"{feature}__high_abs", data[feature], 0.70, "ge")

    if "donchian_pos_96b" in data:
        pos = pd.to_numeric(data["donchian_pos_96b"], errors="coerce")
        states["donchian_pos_96b__center"] = ((pos >= 0.35) & (pos <= 0.65)).where(pos.notna(), np.nan).astype(float)
        states["donchian_pos_96b__edge"] = ((pos <= 0.20) | (pos >= 0.80)).where(pos.notna(), np.nan).astype(float)

    recipes = {
        "high_atr_pct_and_rising_atr_slope": ["atr_pct_12b__high", "atr_slope_96b__rising"],
        "ema_spread_high_and_rising_atr": ["ema_spread_atr_12_48b__high_abs", "atr_slope_96b__rising"],
        "low_vwap_displacement_and_low_atr": ["rolling_vwap_z_96b__low_abs", "atr_pct_12b__low"],
        "high_vwap_displacement_and_rising_atr": ["rolling_vwap_z_96b__high_abs", "atr_slope_96b__rising"],
        "low_atr_and_mid_donchian": ["atr_pct_12b__low", "donchian_pos_96b__center"],
        "rising_atr_and_high_volume": ["atr_slope_96b__rising", "volume_z_96b__high"],
        "low_atr_pct_and_falling_atr_slope": ["atr_pct_12b__low", "atr_slope_96b__falling"],
        "low_adx_and_falling_atr": ["adx_48b__low", "atr_slope_96b__falling"],
    }

    for interaction in interaction_features:
        parts = recipes.get(interaction)
        if not parts:
            continue
        mask = None
        for part in parts:
            state = states.get(part)
            if state is None:
                state = pd.Series(np.nan, index=data.index)
            mask = state.astype("boolean") if mask is None else mask & state.astype("boolean")
        data[interaction] = mask.astype(float)

    return data, pd.DataFrame(thresholds)


def build_feature_dictionary(candidates: pd.DataFrame) -> pd.DataFrame:
    definitions = {
        "atr_slope_96b": ("CONTINUOUS", "true_range rolling mean over 96 5m bars, differenced by 24 bars, divided by ATR_96", "96 bars plus 24-bar slope lag", "OHLC", "normalized slope", "warmup NaN preserved"),
        "rv_slope_96b": ("CONTINUOUS", "annualized rolling log-return std over 96 5m bars, differenced by 24 bars, divided by RV_96", "96 bars plus 24-bar slope lag", "close", "normalized slope", "warmup NaN preserved"),
        "atr_pct_12b": ("CONTINUOUS", "12-bar ATR divided by close", "12 bars", "OHLC close", "price-normalized", "warmup NaN preserved"),
        "bollinger_bandwidth_20b": ("CONTINUOUS", "20-bar Bollinger 2-sigma upper-lower width divided by SMA basis", "20 bars", "close", "price-normalized", "warmup NaN preserved"),
        "keltner_width_20b": ("CONTINUOUS", "4 * 20-bar ATR divided by 20-bar SMA basis", "20 bars", "OHLC close", "price-normalized", "warmup NaN preserved"),
        "rv_12b": ("CONTINUOUS", "annualized rolling log-return std over 12 5m bars", "12 bars", "close", "annualized percent", "warmup NaN preserved"),
        "price_efficiency_96b": ("CONTINUOUS", "absolute 96-bar close change divided by rolling sum of absolute 1-bar close changes", "96 bars", "close", "unitless 0..1-ish efficiency", "warmup NaN preserved"),
        "rolling_vwap_z_96b": ("CONTINUOUS", "close minus rolling 96-bar VWAP divided by 96-bar ATR", "96 bars", "OHLC volume", "ATR-normalized", "warmup NaN preserved"),
        "donchian_pos_96b": ("CONTINUOUS", "close position within 96-bar high-low channel", "96 bars", "high low close", "unit interval when channel nonzero", "warmup NaN preserved"),
        "volume_z_96b": ("CONTINUOUS", "volume minus 96-bar mean divided by 96-bar volume std", "96 bars", "volume", "z-score", "warmup NaN preserved"),
        "ema_spread_atr_12_48b": ("CONTINUOUS", "EMA(12)-EMA(48) divided by 48-bar ATR", "48 bars", "close OHLC", "ATR-normalized", "warmup NaN preserved"),
        "adx_48b": ("CONTINUOUS", "48-bar ADX from directional movement and ATR", "about 96 bars", "OHLC", "0..100 trend strength", "warmup NaN preserved"),
    }
    interaction_definitions = {
        "high_atr_pct_and_rising_atr_slope": "atr_pct_12b >= expanding q70 AND atr_slope_96b >= expanding q50",
        "ema_spread_high_and_rising_atr": "abs(ema_spread_atr_12_48b) >= expanding abs q70 AND atr_slope_96b >= expanding q50",
        "low_vwap_displacement_and_low_atr": "abs(rolling_vwap_z_96b) <= expanding abs q30 AND atr_pct_12b <= expanding q30",
        "high_vwap_displacement_and_rising_atr": "abs(rolling_vwap_z_96b) >= expanding abs q70 AND atr_slope_96b >= expanding q50",
        "low_atr_and_mid_donchian": "atr_pct_12b <= expanding q30 AND 0.35 <= donchian_pos_96b <= 0.65",
        "rising_atr_and_high_volume": "atr_slope_96b >= expanding q50 AND volume_z_96b >= expanding q70",
        "low_atr_pct_and_falling_atr_slope": "atr_pct_12b <= expanding q30 AND atr_slope_96b <= expanding q50",
        "low_adx_and_falling_atr": "adx_48b <= expanding q30 AND atr_slope_96b <= expanding q50",
    }
    rows = []
    for _, candidate in candidates.iterrows():
        feature = candidate["feature"]
        if candidate["type"] == "interaction_step12":
            formula = interaction_definitions.get(feature, "interaction recipe not implemented")
            rows.append(
                {
                    "feature": feature,
                    "feature_type": "BINARY_STATE",
                    "origin": "Step 12",
                    "formula": formula,
                    "lookback": "constituent lookbacks plus expanding threshold min_periods=300",
                    "source_columns": "selected Step 11 feature values",
                    "normalization": "binary state; thresholds fitted expanding at-or-before T",
                    "missing_value_behavior": "NaN until constituent feature and expanding threshold are available",
                    "timestamp_semantics": "all constituents computed from latest completed 5m candle at or before prediction_timestamp",
                }
            )
        else:
            dtype, formula, lookback, source, normalization, missing = definitions.get(
                feature,
                ("CONTINUOUS", "see Step 11 indicator implementation", "varies", "OHLCV", "varies", "warmup NaN preserved"),
            )
            rows.append(
                {
                    "feature": feature,
                    "feature_type": dtype,
                    "origin": "Step 11",
                    "formula": formula,
                    "lookback": lookback,
                    "source_columns": source,
                    "normalization": normalization,
                    "missing_value_behavior": missing,
                    "timestamp_semantics": "computed from latest completed 5m candle at or before prediction_timestamp",
                }
            )
    rows.extend(
        [
            {"feature": "horizon_hours", "feature_type": "HORIZON", "origin": "Step 13", "formula": "canonical horizon mapped to hours", "lookback": "none", "source_columns": "horizon", "normalization": "hours", "missing_value_behavior": "required", "timestamp_semantics": "prediction metadata"},
            {"feature": "regime", "feature_type": "REGIME", "origin": "Probability V1 snapshot", "formula": "frozen regime_v1 context stored on historical snapshot", "lookback": "existing regime engine", "source_columns": "snapshot.regime", "normalization": "categorical", "missing_value_behavior": "preserve missing", "timestamp_semantics": "snapshot context at prediction timestamp"},
        ]
    )
    return pd.DataFrame(rows)


def missingness_report(dataset: pd.DataFrame, feature_columns_: list[str], warmup_cutoff: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for feature in feature_columns_:
        missing = dataset[feature].isna()
        warmup = missing & (dataset["prediction_timestamp"] <= warmup_cutoff)
        rows.append(
            {
                "feature": feature,
                "available_rows": int((~missing).sum()),
                "missing_rows": int(missing.sum()),
                "missing_pct": float(missing.mean()),
                "warmup_related_missing_rows": int(warmup.sum()),
                "warmup_related_missing_pct": float(warmup.mean()),
                "unexpected_missing_rows": int((missing & ~warmup).sum()),
                "unexpected_missing_pct": float((missing & ~warmup).mean()),
            }
        )
    return pd.DataFrame(rows)


def distribution_report(dataset: pd.DataFrame, continuous_features: list[str]) -> pd.DataFrame:
    rows = []
    for feature in continuous_features:
        values = pd.to_numeric(dataset[feature], errors="coerce").dropna()
        if values.empty:
            rows.append({"feature": feature, "n": 0})
            continue
        rows.append(
            {
                "feature": feature,
                "n": int(len(values)),
                "min": float(values.min()),
                "p01": float(values.quantile(0.01)),
                "p10": float(values.quantile(0.10)),
                "median": float(values.quantile(0.50)),
                "p90": float(values.quantile(0.90)),
                "p99": float(values.quantile(0.99)),
                "max": float(values.max()),
                "mean": float(values.mean()),
                "std": float(values.std(ddof=0)),
                "near_constant": bool(values.nunique(dropna=True) <= 2 or values.std(ddof=0) < 1e-12),
                "extreme_outlier_flag": bool(values.std(ddof=0) > 0 and max(abs(values.min() - values.median()), abs(values.max() - values.median())) > 20 * values.std(ddof=0)),
            }
        )
    return pd.DataFrame(rows)


def redundancy_report(dataset: pd.DataFrame, continuous_features: list[str], binary_features: list[str]) -> pd.DataFrame:
    rows = []
    for i, left in enumerate(continuous_features):
        for right in continuous_features[i + 1 :]:
            data = dataset[[left, right]].dropna()
            if len(data) < 50:
                continue
            corr = data[left].rank().corr(data[right].rank())
            recommendation = "DROP_CANDIDATE" if abs(corr) >= 0.95 else "OPTIONAL" if abs(corr) >= 0.85 else "KEEP"
            rows.append({"feature_a": left, "feature_b": right, "relationship": "spearman", "value": float(corr), "recommendation": recommendation})
    for i, left in enumerate(binary_features):
        for right in binary_features[i + 1 :]:
            data = dataset[[left, right]].dropna()
            if len(data) < 50:
                continue
            a = data[left].astype(bool)
            b = data[right].astype(bool)
            union = (a | b).sum()
            jaccard = float((a & b).sum() / union) if union else 0.0
            recommendation = "DROP_CANDIDATE" if jaccard >= 0.90 else "OPTIONAL" if jaccard >= 0.75 else "KEEP"
            rows.append({"feature_a": left, "feature_b": right, "relationship": "jaccard_active_overlap", "value": jaccard, "recommendation": recommendation})
    return pd.DataFrame(rows).sort_values("value", key=lambda s: s.abs(), ascending=False)


def target_metadata(dataset: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
    rows = []
    for target in target_columns:
        if target not in dataset:
            continue
        for horizon, group in dataset.groupby("horizon"):
            values = group[target].dropna()
            base_rate = float(values.mean()) if len(values) else None
            if base_rate is None:
                flag = "NO_ELIGIBLE_ROWS"
            elif base_rate >= 0.98:
                flag = "SATURATED_HIGH"
            elif base_rate <= 0.02:
                flag = "SATURATED_LOW"
            else:
                flag = "USABLE"
            rows.append(
                {
                    "target": target,
                    "horizon": horizon,
                    "strategy_relevance": strategy_relevance(target),
                    "n": int(len(group)),
                    "eligibility_count": int(len(values)),
                    "positive_count": int(values.sum()) if len(values) else 0,
                    "base_rate": base_rate,
                    "saturation_flag": flag,
                }
            )
    return pd.DataFrame(rows)


def strategy_relevance(target: str) -> str:
    if target in {"path_inside_70", "range_breached", "max_abs_excursion_ge_1_5_atr", "realized_over_range_width_ge_1", "range_continuation"}:
        return "BOTH"
    if target in {"oscillatory_path", "trend_efficiency_high", "one_sided_runaway"}:
        return "GRID"
    if target in {"upper_breach_only", "lower_breach_only", "both_side_breach"}:
        return "IRON_FLY"
    if target in {"fast_1atr_touch", "up_excursion_ge_1_0_atr", "down_excursion_ge_1_0_atr"}:
        return "DEFENSIVE_LONG_OPTION"
    return "GENERIC_PROBABILITY"


def validate_bridge(dataset: pd.DataFrame, feature_columns_: list[str]) -> dict:
    duplicate_rows = int(dataset.duplicated(["prediction_timestamp", "horizon", "symbol", "record_type"]).sum())
    max_delta = dataset["feature_source_timestamp"] - dataset["prediction_timestamp"]
    return {
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "row_count": int(len(dataset)),
        "column_count": int(len(dataset.columns)),
        "timestamp_count": int(dataset["prediction_timestamp"].nunique()),
        "rows_by_horizon": {str(k): int(v) for k, v in dataset["horizon"].value_counts().sort_index().items()},
        "duplicates": duplicate_rows,
        "max_feature_source_after_prediction_seconds": float(max_delta.dt.total_seconds().max()),
        "no_lookahead_pass": bool(max_delta.dt.total_seconds().max() <= 0),
        "horizon_explicit": "horizon_hours" in dataset.columns,
        "regime_explicit": "regime" in dataset.columns,
        "missing_feature_cells": int(dataset[feature_columns_].isna().sum().sum()),
        "dataset_hash": stable_csv_hash(dataset),
    }


def chronological_split_plan(dataset: pd.DataFrame) -> dict:
    timestamps = pd.Series(sorted(dataset["prediction_timestamp"].drop_duplicates()))
    n = len(timestamps)
    train_end = timestamps.iloc[int(n * 0.60) - 1]
    validation_end = timestamps.iloc[int(n * 0.80) - 1]
    return {
        "recommended_scheme": "chronological 60/20/20 timestamp split plus horizon-specific non-overlap robustness samples",
        "train": {"start": timestamps.iloc[0].isoformat(), "end": train_end.isoformat(), "timestamps": int((timestamps <= train_end).sum()), "rows": int((dataset["prediction_timestamp"] <= train_end).sum())},
        "validation": {"start": timestamps[timestamps > train_end].iloc[0].isoformat(), "end": validation_end.isoformat(), "timestamps": int(((timestamps > train_end) & (timestamps <= validation_end)).sum()), "rows": int(((dataset["prediction_timestamp"] > train_end) & (dataset["prediction_timestamp"] <= validation_end)).sum())},
        "test": {"start": timestamps[timestamps > validation_end].iloc[0].isoformat(), "end": timestamps.iloc[-1].isoformat(), "timestamps": int((timestamps > validation_end).sum()), "rows": int((dataset["prediction_timestamp"] > validation_end).sum())},
        "overlap_recommendation": "For every horizon, report primary chronological validation/test and secondary less-overlapping samples using every horizon_minutes/5m-th timestamp with a purge/embargo of at least one horizon around split boundaries.",
    }


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
