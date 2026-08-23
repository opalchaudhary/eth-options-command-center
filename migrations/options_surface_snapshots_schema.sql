create extension if not exists pgcrypto;

create table if not exists options_surface_snapshots (
    id uuid primary key default gen_random_uuid(),
    snapshot_timestamp timestamptz not null,
    symbol text not null,
    version text not null,
    surface_method_version text not null,
    logical_expiry_bucket text not null,
    actual_expiry timestamptz,
    dte_hours numeric,
    dte_days numeric,
    spot_price numeric,
    index_price numeric,
    atm_strike numeric,
    atm_call_mark numeric,
    atm_put_mark numeric,
    atm_call_bid numeric,
    atm_call_ask numeric,
    atm_put_bid numeric,
    atm_put_ask numeric,
    atm_call_iv numeric,
    atm_put_iv numeric,
    atm_iv numeric,
    atm_bid_iv numeric,
    atm_ask_iv numeric,
    atm_straddle_mark numeric,
    atm_straddle_bid numeric,
    atm_straddle_ask numeric,
    atm_straddle_mid numeric,
    implied_move_abs numeric,
    implied_move_pct numeric,
    realized_volatility_reference numeric,
    realized_vol_window text,
    iv_rv_spread numeric,
    iv_rv_ratio numeric,
    put_25d_iv numeric,
    call_25d_iv numeric,
    risk_reversal_25d numeric,
    put_skew_vs_atm numeric,
    call_skew_vs_atm numeric,
    skew_slope numeric,
    butterfly_25d numeric,
    total_call_oi numeric,
    total_put_oi numeric,
    put_call_oi_ratio numeric,
    largest_call_oi_strike numeric,
    largest_call_oi numeric,
    largest_put_oi_strike numeric,
    largest_put_oi numeric,
    distance_to_call_oi_wall_abs numeric,
    distance_to_call_oi_wall_pct numeric,
    distance_to_put_oi_wall_abs numeric,
    distance_to_put_oi_wall_pct numeric,
    atm_zone_call_oi numeric,
    atm_zone_put_oi numeric,
    oi_concentration numeric,
    total_call_volume numeric,
    total_put_volume numeric,
    put_call_volume_ratio numeric,
    largest_call_volume_strike numeric,
    largest_put_volume_strike numeric,
    atm_call_spread numeric,
    atm_put_spread numeric,
    atm_call_spread_pct numeric,
    atm_put_spread_pct numeric,
    atm_combined_liquidity_score numeric,
    valid_quoted_calls integer,
    valid_quoted_puts integer,
    usable_quote_pct numeric,
    surface_liquidity_score numeric,
    atm_call_delta numeric,
    atm_put_delta numeric,
    atm_gamma numeric,
    atm_theta numeric,
    atm_vega numeric,
    contracts_seen integer,
    valid_iv_contracts integer,
    valid_oi_contracts integer,
    valid_quote_contracts integer,
    atm_quality text,
    skew_quality text,
    oi_quality text,
    liquidity_quality text,
    source_status text not null,
    completeness numeric,
    staleness_seconds numeric,
    error_reason text,
    metadata_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (symbol, logical_expiry_bucket, snapshot_timestamp, version)
);

create index if not exists idx_options_surface_symbol_time
    on options_surface_snapshots (symbol, snapshot_timestamp desc);

create index if not exists idx_options_surface_bucket_time
    on options_surface_snapshots (logical_expiry_bucket, snapshot_timestamp desc);

create index if not exists idx_options_surface_version_time
    on options_surface_snapshots (version, snapshot_timestamp desc);

-- Verification:
-- select logical_expiry_bucket, count(*), min(snapshot_timestamp), max(snapshot_timestamp)
-- from options_surface_snapshots
-- where version = 'rich_data_v1_options_surface'
-- group by logical_expiry_bucket
-- order by logical_expiry_bucket;
