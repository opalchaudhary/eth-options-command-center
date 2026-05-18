create extension if not exists pgcrypto;

create table if not exists futures_paper_wallet (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    starting_balance_inr numeric not null default 50000,
    starting_balance_usdt numeric not null default 588.2353,
    current_balance_usdt numeric,
    current_balance_inr numeric,
    available_balance_usdt numeric,
    used_margin_usdt numeric,
    unrealized_pnl_usdt numeric,
    realized_pnl_usdt numeric,
    equity_usdt numeric,
    max_drawdown_pct numeric,
    total_trades integer not null default 0,
    winning_trades integer not null default 0,
    losing_trades integer not null default 0,
    win_rate numeric,
    status text not null default 'ACTIVE',
    snapshot_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_futures_paper_wallet_created_at
    on futures_paper_wallet (created_at desc);

create index if not exists idx_futures_paper_wallet_status
    on futures_paper_wallet (status, created_at desc);

create table if not exists futures_paper_trades (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    trade_id text not null unique,
    symbol text not null default 'ETHUSDT.PERP',
    direction text not null check (direction in ('LONG', 'SHORT', 'NO_TRADE')),
    status text not null check (status in ('OPEN', 'CLOSED', 'CANCELLED', 'REJECTED')),
    entry_price numeric,
    exit_price numeric,
    mark_price numeric,
    stop_loss numeric,
    take_profit numeric,
    trailing_stop numeric,
    liquidation_price_estimate numeric,
    leverage numeric,
    lots integer,
    position_size_eth numeric,
    margin_used_usdt numeric,
    risk_amount_usdt numeric,
    risk_pct numeric,
    expected_reward_usdt numeric,
    reward_pct numeric,
    rr_ratio numeric,
    realized_pnl_usdt numeric not null default 0,
    realized_pnl_inr numeric not null default 0,
    unrealized_pnl_usdt numeric not null default 0,
    entry_confidence_score numeric,
    exit_confidence_score numeric,
    entry_reason text,
    exit_reason text,
    market_regime text,
    trend_context text,
    smc_context jsonb not null default '{}'::jsonb,
    volume_context jsonb not null default '{}'::jsonb,
    options_context jsonb not null default '{}'::jsonb,
    liquidation_context jsonb not null default '{}'::jsonb,
    raw_snapshot_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_futures_paper_trades_status
    on futures_paper_trades (status, created_at desc);

create index if not exists idx_futures_paper_trades_trade_id
    on futures_paper_trades (trade_id);

create index if not exists idx_futures_paper_trades_created_at
    on futures_paper_trades (created_at desc);

create table if not exists futures_trade_journal (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    trade_id text,
    event_type text not null,
    event_description text,
    price numeric,
    wallet_equity_usdt numeric,
    pnl_usdt numeric,
    risk_state text,
    action_taken text,
    reason text,
    raw_data_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_futures_trade_journal_created_at
    on futures_trade_journal (created_at desc);

create index if not exists idx_futures_trade_journal_trade_id
    on futures_trade_journal (trade_id, created_at desc);

create table if not exists futures_model_training_dataset (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    trade_id text,
    features_json jsonb not null default '{}'::jsonb,
    label text,
    pnl_after_15m numeric,
    pnl_after_30m numeric,
    pnl_after_1h numeric,
    pnl_after_3h numeric,
    max_favorable_excursion numeric,
    max_adverse_excursion numeric,
    final_outcome text,
    model_ready boolean not null default false
);

create index if not exists idx_futures_model_training_dataset_created_at
    on futures_model_training_dataset (created_at desc);

create index if not exists idx_futures_model_training_dataset_trade_id
    on futures_model_training_dataset (trade_id);

create index if not exists idx_futures_model_training_dataset_model_ready
    on futures_model_training_dataset (model_ready, created_at desc);

create table if not exists futures_trading_engine_runs (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    cycle_started_at timestamptz,
    cycle_finished_at timestamptz,
    status text not null,
    action text,
    error text,
    interval_seconds integer,
    opened_trade_id uuid,
    selected_direction text,
    selected_score numeric,
    open_position_count integer,
    closed_trade_count integer,
    cycle_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_futures_trading_engine_runs_created_at
    on futures_trading_engine_runs (created_at desc);

create index if not exists idx_futures_trading_engine_runs_status
    on futures_trading_engine_runs (status, created_at desc);
