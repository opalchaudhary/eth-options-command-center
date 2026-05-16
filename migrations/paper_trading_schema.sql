create extension if not exists pgcrypto;

alter table if exists paper_trades
    add column if not exists selection_score numeric,
    add column if not exists entry_reason text,
    add column if not exists wallet_before jsonb not null default '{}'::jsonb,
    add column if not exists wallet_after jsonb not null default '{}'::jsonb,
    add column if not exists entry_greeks jsonb not null default '{}'::jsonb,
    add column if not exists current_greeks jsonb not null default '{}'::jsonb,
    add column if not exists exit_greeks jsonb not null default '{}'::jsonb,
    add column if not exists exit_reason_label text,
    add column if not exists exit_reason_detail text,
    add column if not exists exit_signal jsonb not null default '{}'::jsonb;

create table if not exists paper_recommendation_evaluations (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    expiry_label text,
    strategy text,
    recommendation_id uuid references recommendation_journal(id) on delete set null,
    selected boolean not null default false,
    selection_score numeric,
    rejection_reasons jsonb not null default '[]'::jsonb,
    wallet_state jsonb not null default '{}'::jsonb,
    risk_json jsonb not null default '{}'::jsonb,
    insight_json jsonb not null default '{}'::jsonb,
    candidate_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_paper_recommendation_evaluations_created_at
    on paper_recommendation_evaluations (created_at desc);

create index if not exists idx_paper_recommendation_evaluations_selected
    on paper_recommendation_evaluations (selected, created_at desc);

create table if not exists paper_wallet_snapshots (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    starting_capital_inr numeric not null default 50000,
    starting_capital_usdt numeric not null default 588.2353,
    available_margin_usdt numeric,
    used_margin_usdt numeric,
    realized_pnl_usdt numeric,
    unrealized_pnl_usdt numeric,
    current_equity_usdt numeric,
    margin_health_pct numeric,
    book_greeks jsonb not null default '{}'::jsonb,
    snapshot_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_paper_wallet_snapshots_created_at
    on paper_wallet_snapshots (created_at desc);
