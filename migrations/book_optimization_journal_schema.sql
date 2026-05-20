create extension if not exists pgcrypto;

create table if not exists book_optimization_journal (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    spot_price numeric,
    expiry_context jsonb not null default '[]'::jsonb,
    market_regime text,
    input_delta numeric,
    input_gamma numeric,
    input_theta numeric,
    input_vega numeric,
    margin_used numeric,
    margin_available_to_withdraw numeric,
    portfolio_equity numeric,
    margin_usage_pct numeric,
    wallet_size numeric,
    health_status text,
    risk_score numeric,
    ideal_delta_min numeric,
    ideal_delta_max numeric,
    ideal_gamma_min numeric,
    ideal_gamma_max numeric,
    ideal_theta_min numeric,
    ideal_theta_max numeric,
    ideal_vega_min numeric,
    ideal_vega_max numeric,
    suggested_action_json jsonb not null default '{}'::jsonb,
    reasoning_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_book_optimization_journal_created_at
    on book_optimization_journal (created_at desc);

create index if not exists idx_book_optimization_journal_health
    on book_optimization_journal (health_status, created_at desc);

create index if not exists idx_book_optimization_journal_market_regime
    on book_optimization_journal (market_regime, created_at desc);

alter table if exists book_optimization_journal
    add column if not exists margin_available_to_withdraw numeric,
    add column if not exists portfolio_equity numeric,
    add column if not exists margin_usage_pct numeric;
