create extension if not exists pgcrypto;

create table if not exists probability_market_snapshots (
    id uuid primary key default gen_random_uuid(),
    timestamp timestamptz not null default now(),
    symbol text not null,
    spot_price numeric,
    future_price numeric,
    return_5m numeric,
    return_15m numeric,
    return_1h numeric,
    return_4h numeric,
    vwap numeric,
    vwap_deviation_pct numeric,
    vwap_zscore numeric,
    atr numeric,
    atr_pct numeric,
    realized_volatility numeric,
    volume numeric,
    volume_zscore numeric,
    funding_rate numeric,
    funding_percentile numeric,
    open_interest numeric,
    oi_change_5m numeric,
    oi_change_1h numeric,
    oi_change_4h numeric,
    basis numeric,
    cvd_5m numeric,
    cvd_15m numeric,
    cvd_1h numeric,
    cvd_slope numeric,
    cvd_acceleration numeric,
    price_cvd_divergence numeric,
    buy_volume_ratio numeric,
    book_imbalance numeric,
    spread_bps numeric,
    bid_depth numeric,
    ask_depth numeric,
    atm_iv numeric,
    iv_rv_spread numeric,
    iv_percentile numeric,
    put_call_skew numeric,
    term_structure_signal numeric,
    regime text not null default 'UNKNOWN',
    feature_version text not null,
    regime_version text not null,
    delta_market_data_status text not null default 'UNKNOWN',
    orderflow_provider_status text not null default 'DISABLED',
    last_delta_update timestamptz,
    last_orderflow_update timestamptz,
    data_age_seconds numeric,
    metadata_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_probability_market_snapshots_symbol_time
    on probability_market_snapshots (symbol, timestamp desc);

create table if not exists probability_predictions (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    snapshot_id uuid references probability_market_snapshots(id),
    symbol text not null,
    horizon text not null,
    record_type text not null check (record_type in ('LIVE', 'BACKTEST')),
    model_version text not null,
    feature_version text not null,
    regime_version text not null,
    range_model_version text not null,
    prediction_status text not null,
    mean_reversion_probability numeric,
    upside_breakout_probability numeric,
    downside_breakdown_probability numeric,
    range_continuation_probability numeric,
    trend_continuation_probability numeric,
    confidence numeric,
    expected_price numeric,
    median_price numeric,
    expected_equilibrium numeric,
    range_50_lower numeric,
    range_50_upper numeric,
    range_70_lower numeric,
    range_70_upper numeric,
    range_90_lower numeric,
    range_90_upper numeric,
    analogue_sample_size integer not null default 0,
    metadata_json jsonb not null default '{}'::jsonb,
    constraint probability_predictions_live_snapshot_required
        check (record_type <> 'LIVE' or snapshot_id is not null),
    unique (snapshot_id, horizon, record_type, model_version)
);

create index if not exists idx_probability_predictions_symbol_horizon_created
    on probability_predictions (symbol, horizon, created_at desc);

create table if not exists probability_outcomes (
    id uuid primary key default gen_random_uuid(),
    prediction_id uuid not null references probability_predictions(id) on delete cascade,
    evaluated_at timestamptz not null default now(),
    actual_open numeric,
    actual_high numeric,
    actual_low numeric,
    actual_close numeric,
    maximum_up_excursion numeric,
    maximum_down_excursion numeric,
    mean_reversion_occurred boolean,
    mean_reversion_fraction numeric,
    upside_breakout_occurred boolean,
    downside_breakdown_occurred boolean,
    range_held boolean,
    trend_continuation_occurred boolean,
    range_50_covered boolean,
    range_70_covered boolean,
    range_90_covered boolean,
    upper_touch_occurred boolean,
    lower_touch_occurred boolean,
    metadata_json jsonb not null default '{}'::jsonb,
    unique (prediction_id)
);

create index if not exists idx_probability_outcomes_prediction
    on probability_outcomes (prediction_id);

create table if not exists probability_model_performance (
    id uuid primary key default gen_random_uuid(),
    calculated_at timestamptz not null default now(),
    event_name text not null,
    horizon text not null,
    model_version text not null,
    regime text not null,
    sample_count integer not null default 0,
    brier_score numeric,
    log_loss numeric,
    calibration_error numeric,
    range_50_coverage numeric,
    range_70_coverage numeric,
    range_90_coverage numeric,
    metadata_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_probability_model_performance_model_horizon_regime
    on probability_model_performance (model_version, horizon, regime, calculated_at desc);

create table if not exists probability_calibration (
    id uuid primary key default gen_random_uuid(),
    calculated_at timestamptz not null default now(),
    event_name text not null,
    horizon text not null,
    model_version text not null,
    regime text not null,
    bucket text not null,
    sample_count integer not null default 0,
    average_prediction numeric,
    actual_occurrence_rate numeric,
    metadata_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_probability_calibration_model_horizon_event
    on probability_calibration (model_version, horizon, event_name, calculated_at desc);

create table if not exists option_strike_recommendations (
    id uuid primary key default gen_random_uuid(),
    timestamp timestamptz not null default now(),
    prediction_id uuid references probability_predictions(id) on delete set null,
    symbol text not null,
    expiry text,
    option_type text not null,
    strike numeric,
    risk_tier text not null,
    recommendation_status text not null,
    touch_probability numeric,
    itm_probability numeric,
    premium numeric,
    premium_efficiency numeric,
    range_buffer_pct numeric,
    risk_score numeric,
    model_version text not null,
    metadata_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_option_strike_recommendations_expiry_type_time
    on option_strike_recommendations (expiry, option_type, timestamp desc);

create table if not exists option_strike_outcomes (
    id uuid primary key default gen_random_uuid(),
    recommendation_id uuid not null references option_strike_recommendations(id) on delete cascade,
    evaluated_at timestamptz not null default now(),
    touched boolean,
    closed_itm boolean,
    actual_high numeric,
    actual_low numeric,
    actual_close numeric,
    metadata_json jsonb not null default '{}'::jsonb,
    unique (recommendation_id)
);

-- V1 intentionally creates no retention/delete function for Probability Engine tables.
-- PROBABILITY_RETENTION_ENABLED must remain false until storage growth is measured.
