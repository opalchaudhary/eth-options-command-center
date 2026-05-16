create extension if not exists pgcrypto;

create table if not exists recommendation_journal (
    id uuid primary key default gen_random_uuid(),
    recommendation_key text not null unique,
    created_at timestamptz not null default now(),
    spot_price numeric,
    expiry_label text,
    market_regime text,
    directional_bias text,
    suggested_strategy text,
    suggested_sell_strike numeric,
    suggested_hedge_strike numeric,
    confidence_score integer,
    signal_conflict_score integer,
    warnings jsonb not null default '[]'::jsonb,
    reasoning_text text,
    raw_input_snapshot jsonb not null default '{}'::jsonb,
    recommendation_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_recommendation_journal_created_at
    on recommendation_journal (created_at desc);

create index if not exists idx_recommendation_journal_strategy
    on recommendation_journal (suggested_strategy);

create table if not exists paper_trades (
    id uuid primary key default gen_random_uuid(),
    recommendation_id uuid not null references recommendation_journal(id) on delete cascade,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    closed_at timestamptz,
    status text not null check (status in ('OPEN', 'CLOSED', 'EXPIRED')),
    strategy text,
    side text,
    expiry_label text,
    entry_spot numeric,
    current_spot numeric,
    exit_spot numeric,
    lots integer not null default 0,
    eth_quantity numeric not null default 0,
    entry_premium_usdt numeric not null default 0,
    exit_premium_usdt numeric,
    margin_used_usdt numeric not null default 0,
    margin_used_inr numeric not null default 0,
    max_risk_usdt numeric not null default 0,
    max_risk_inr numeric not null default 0,
    wallet_capital_inr numeric not null default 50000,
    wallet_capital_usdt numeric not null default 588.2353,
    inr_per_usdt numeric not null default 85,
    eth_lot_size numeric not null default 0.01,
    unrealized_pnl_usdt numeric not null default 0,
    unrealized_pnl_inr numeric not null default 0,
    realized_pnl_usdt numeric not null default 0,
    realized_pnl_inr numeric not null default 0,
    exit_reason text check (exit_reason in ('TP', 'SL', 'EXPIRY', 'MANUAL', 'ENGINE_EXIT') or exit_reason is null),
    trade_json jsonb not null default '{}'::jsonb,
    unique (recommendation_id)
);

create index if not exists idx_paper_trades_status
    on paper_trades (status, created_at desc);

create index if not exists idx_paper_trades_strategy
    on paper_trades (strategy);

create table if not exists recommendation_outcomes (
    id uuid primary key default gen_random_uuid(),
    recommendation_id uuid not null references recommendation_journal(id) on delete cascade,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    result_1h text,
    result_3h text,
    result_expiry text,
    spot_1h numeric,
    spot_3h numeric,
    spot_expiry numeric,
    max_favourable_excursion numeric,
    max_adverse_excursion numeric,
    strategy_profitable_1h boolean,
    strategy_profitable_3h boolean,
    strategy_profitable_expiry boolean,
    confidence_matched_actual boolean,
    outcome_json jsonb not null default '{}'::jsonb,
    unique (recommendation_id)
);

create index if not exists idx_recommendation_outcomes_updated_at
    on recommendation_outcomes (updated_at desc);
