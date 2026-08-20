create extension if not exists pgcrypto;

create table if not exists derivatives_metric_snapshots (
    id uuid primary key default gen_random_uuid(),
    timestamp timestamptz not null,
    symbol text not null,
    version text not null,
    spot_price numeric,
    reference_price numeric,
    mark_price numeric,
    mark_premium numeric,
    mark_premium_pct numeric,
    open_interest numeric,
    oi_delta_5m numeric,
    oi_delta_pct_5m numeric,
    funding_rate numeric,
    funding_zscore numeric,
    funding_percentile numeric,
    source_status text not null default 'UNKNOWN',
    completeness numeric,
    staleness_seconds numeric,
    error_reason text,
    metadata_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (symbol, timestamp, version)
);

create index if not exists idx_derivatives_metric_snapshots_symbol_time
    on derivatives_metric_snapshots (symbol, timestamp desc);

create table if not exists orderflow_aggregates (
    id uuid primary key default gen_random_uuid(),
    bucket_timestamp timestamptz not null,
    symbol text not null,
    version text not null,
    trade_count integer not null default 0,
    total_volume numeric,
    taker_buy_volume numeric,
    taker_sell_volume numeric,
    taker_buy_ratio numeric,
    taker_sell_ratio numeric,
    net_taker_volume numeric,
    cvd_increment numeric,
    cvd_running numeric,
    cvd_5m numeric,
    cvd_15m numeric,
    cvd_1h numeric,
    average_trade_size numeric,
    max_trade_size numeric,
    large_trade_threshold numeric,
    large_buy_volume numeric,
    large_sell_volume numeric,
    large_trade_imbalance numeric,
    large_trade_count integer not null default 0,
    source_status text not null default 'UNKNOWN',
    completeness numeric,
    staleness_seconds numeric,
    error_reason text,
    metadata_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (symbol, bucket_timestamp, version)
);

create index if not exists idx_orderflow_aggregates_symbol_time
    on orderflow_aggregates (symbol, bucket_timestamp desc);

create table if not exists orderbook_aggregates (
    id uuid primary key default gen_random_uuid(),
    timestamp timestamptz not null,
    symbol text not null,
    version text not null,
    best_bid numeric,
    best_ask numeric,
    mid_price numeric,
    spread numeric,
    spread_bps numeric,
    bid_depth_10bp numeric,
    ask_depth_10bp numeric,
    bid_depth_25bp numeric,
    ask_depth_25bp numeric,
    bid_depth_50bp numeric,
    ask_depth_50bp numeric,
    bid_depth_100bp numeric,
    ask_depth_100bp numeric,
    imbalance_10bp numeric,
    imbalance_25bp numeric,
    imbalance_50bp numeric,
    imbalance_100bp numeric,
    weighted_book_imbalance numeric,
    microprice numeric,
    book_pressure text,
    nearest_major_bid_wall_distance numeric,
    nearest_major_ask_wall_distance numeric,
    major_bid_wall_size numeric,
    major_ask_wall_size numeric,
    liquidity_concentration numeric,
    source_status text not null default 'UNKNOWN',
    completeness numeric,
    staleness_seconds numeric,
    error_reason text,
    metadata_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (symbol, timestamp, version)
);

create index if not exists idx_orderbook_aggregates_symbol_time
    on orderbook_aggregates (symbol, timestamp desc);

