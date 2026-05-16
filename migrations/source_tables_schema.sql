create extension if not exists pgcrypto;

create table if not exists analytics_snapshots (
    id uuid primary key default gen_random_uuid(),
    snapshot_time timestamptz not null default now(),
    expiry_label text not null,
    spot_price numeric,
    max_pain numeric,
    atm_strike numeric,
    pcr numeric,
    atm_straddle_price numeric,
    expected_move_pct numeric,
    expected_move_upper numeric,
    expected_move_lower numeric
);

create index if not exists idx_analytics_snapshots_expiry_time
    on analytics_snapshots (expiry_label, snapshot_time desc);

create table if not exists premium_decay_snapshots (
    id uuid primary key default gen_random_uuid(),
    snapshot_time timestamptz not null default now(),
    expiry_label text not null,
    atm_strike numeric,
    atm_ce_price numeric,
    atm_pe_price numeric,
    atm_straddle_price numeric
);

create index if not exists idx_premium_decay_snapshots_expiry_time
    on premium_decay_snapshots (expiry_label, snapshot_time desc);

create table if not exists option_chain_snapshots (
    id uuid primary key default gen_random_uuid(),
    snapshot_time timestamptz not null default now(),
    expiry_label text not null,
    expiry_date text,
    strike numeric,
    option_type text,
    mark_price numeric,
    oi numeric,
    volume numeric,
    iv numeric,
    delta numeric,
    gamma numeric,
    theta numeric,
    vega numeric
);

create index if not exists idx_option_chain_snapshots_expiry_time
    on option_chain_snapshots (expiry_label, snapshot_time desc);

create table if not exists orderbook_insights (
    id uuid primary key default gen_random_uuid(),
    timestamp timestamptz not null default now(),
    last_updated_at timestamptz,
    symbol text not null,
    eth_price numeric,
    best_bid numeric,
    best_ask numeric,
    spread numeric,
    spread_pct numeric,
    spread_quality text,
    bid_depth numeric,
    ask_depth numeric,
    imbalance_ratio numeric,
    bias text,
    nearest_bid_wall_price numeric,
    nearest_bid_wall_size numeric,
    nearest_ask_wall_price numeric,
    nearest_ask_wall_size numeric,
    trap_risk text,
    execution_signal text
);

create index if not exists idx_orderbook_insights_symbol_time
    on orderbook_insights (symbol, timestamp desc);

create table if not exists eth_ohlcv (
    id uuid primary key default gen_random_uuid(),
    symbol text not null,
    resolution text not null,
    candle_time timestamptz not null,
    epoch_time bigint,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    unique (symbol, resolution, candle_time)
);

create index if not exists idx_eth_ohlcv_symbol_resolution_time
    on eth_ohlcv (symbol, resolution, candle_time desc);

create table if not exists eth_market_events (
    id uuid primary key default gen_random_uuid(),
    symbol text not null,
    resolution text not null,
    event_type text not null,
    direction text,
    event_time timestamptz not null,
    price numeric not null,
    reference_price numeric,
    strength numeric,
    metadata jsonb not null default '{}'::jsonb,
    unique (symbol, resolution, event_type, event_time, price)
);

create index if not exists idx_eth_market_events_symbol_resolution_time
    on eth_market_events (symbol, resolution, event_time desc);

create table if not exists eth_smc_zones (
    id uuid primary key default gen_random_uuid(),
    symbol text not null,
    resolution text not null,
    zone_type text not null,
    direction text,
    start_time timestamptz not null,
    end_time timestamptz,
    price_low numeric not null,
    price_high numeric not null,
    strength numeric,
    status text not null default 'active',
    metadata jsonb not null default '{}'::jsonb,
    unique (symbol, resolution, zone_type, start_time, price_low, price_high)
);

create index if not exists idx_eth_smc_zones_symbol_resolution_status
    on eth_smc_zones (symbol, resolution, status, start_time desc);

create table if not exists eth_volume_profile (
    id uuid primary key default gen_random_uuid(),
    symbol text not null,
    resolution text not null,
    price_level numeric not null,
    volume numeric,
    profile_type text,
    metadata jsonb not null default '{}'::jsonb,
    unique (symbol, resolution, price_level)
);

create index if not exists idx_eth_volume_profile_symbol_resolution_price
    on eth_volume_profile (symbol, resolution, price_level);
