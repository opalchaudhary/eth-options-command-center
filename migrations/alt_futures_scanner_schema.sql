create extension if not exists pgcrypto;

create table if not exists alt_futures_scanner_snapshots (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    symbol text not null,
    price numeric,
    score numeric,
    classification text,
    direction text,
    indicators_json jsonb not null default '{}'::jsonb,
    funding_rate numeric,
    open_interest numeric,
    oi_change_pct numeric,
    volume numeric,
    volume_change_pct numeric,
    spread numeric,
    spread_pct numeric,
    liquidity_score numeric,
    trend_score numeric,
    smc_score numeric,
    final_reason text,
    selected boolean not null default false,
    raw_snapshot_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_alt_futures_scanner_snapshots_created_at
    on alt_futures_scanner_snapshots (created_at desc);

create index if not exists idx_alt_futures_scanner_snapshots_symbol
    on alt_futures_scanner_snapshots (symbol, created_at desc);

create index if not exists idx_alt_futures_scanner_snapshots_selected
    on alt_futures_scanner_snapshots (selected, created_at desc);

create table if not exists alt_futures_trade_journal (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    closed_at timestamptz,
    trade_id text not null unique,
    symbol text not null,
    direction text not null check (direction in ('LONG', 'SHORT', 'NO_TRADE')),
    entry_price numeric,
    exit_price numeric,
    stop_loss numeric,
    take_profit_1 numeric,
    take_profit_2 numeric,
    trailing_stop numeric,
    liquidation_price_estimate numeric,
    leverage numeric,
    position_size numeric,
    margin_used_usdt numeric,
    margin_used_inr numeric,
    risk_usdt numeric,
    risk_inr numeric,
    expected_reward_usdt numeric,
    expected_reward_inr numeric,
    rr_ratio numeric,
    wallet_before_usdt numeric,
    wallet_after_usdt numeric,
    status text not null check (status in ('OPEN', 'CLOSED', 'CANCELLED', 'SKIPPED')),
    pnl_inr numeric not null default 0,
    pnl_usdt numeric not null default 0,
    unrealized_pnl_usdt numeric not null default 0,
    reason_for_entry text,
    reason_for_exit text,
    market_regime_at_entry text,
    scanner_score_at_entry numeric,
    trade_confidence text,
    raw_trade_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_alt_futures_trade_journal_status
    on alt_futures_trade_journal (status, created_at desc);

create index if not exists idx_alt_futures_trade_journal_symbol
    on alt_futures_trade_journal (symbol, created_at desc);

create index if not exists idx_alt_futures_trade_journal_trade_id
    on alt_futures_trade_journal (trade_id);

create table if not exists alt_futures_trade_events (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    trade_id text,
    event_type text not null,
    price numeric,
    pnl_usdt numeric,
    pnl_inr numeric,
    wallet_equity_usdt numeric,
    reason text,
    raw_event_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_alt_futures_trade_events_created_at
    on alt_futures_trade_events (created_at desc);

create index if not exists idx_alt_futures_trade_events_trade_id
    on alt_futures_trade_events (trade_id, created_at desc);

create table if not exists alt_futures_wallet_ledger (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    wallet_balance_inr numeric,
    wallet_balance_usdt numeric,
    equity_inr numeric,
    equity_usdt numeric,
    available_balance_usdt numeric,
    used_margin_usdt numeric,
    realized_pnl_usdt numeric,
    unrealized_pnl_usdt numeric,
    max_drawdown_pct numeric,
    event_type text not null default 'SNAPSHOT',
    trade_id text,
    notes text,
    ledger_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_alt_futures_wallet_ledger_created_at
    on alt_futures_wallet_ledger (created_at desc);

create index if not exists idx_alt_futures_wallet_ledger_trade_id
    on alt_futures_wallet_ledger (trade_id, created_at desc);

create table if not exists alt_futures_engine_runs (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    cycle_started_at timestamptz,
    cycle_finished_at timestamptz,
    status text not null,
    action text,
    error text,
    interval_seconds integer,
    opened_trade_id uuid,
    selected_symbol text,
    selected_direction text,
    selected_score numeric,
    open_position_count integer,
    scanned_symbol_count integer,
    cycle_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_alt_futures_engine_runs_created_at
    on alt_futures_engine_runs (created_at desc);

create index if not exists idx_alt_futures_engine_runs_status
    on alt_futures_engine_runs (status, created_at desc);
