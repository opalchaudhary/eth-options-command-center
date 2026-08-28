create extension if not exists pgcrypto;

create table if not exists grid_bots (
    bot_id text primary key,
    bot_name text not null,
    product_symbol text not null,
    product_id integer,
    environment text not null default 'testnet',
    status text not null default 'CREATED',
    current_status text not null default 'CREATED',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists grid_runs (
    run_id text primary key,
    bot_id text not null,
    status text not null,
    config_version integer not null,
    execution_event_mode text,
    operational_state text,
    starting_config_version integer,
    active_config_version integer,
    ending_config_version integer,
    started_at timestamptz,
    stopped_at timestamptz,
    reference_price numeric,
    starting_market_price numeric,
    ending_market_price numeric,
    starting_account_equity numeric,
    ending_account_equity numeric,
    stop_reason text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists grid_active_run_locks (
    lock_name text primary key,
    run_id text not null unique,
    created_at timestamptz not null default now()
);

create table if not exists grid_config_versions (
    run_id text not null,
    bot_id text not null,
    config_version integer not null,
    effective_from timestamptz not null default now(),
    effective_to timestamptz,
    grid_type text,
    lower_price numeric,
    upper_price numeric,
    grid_count integer,
    spacing_type text,
    lot_size numeric,
    max_inventory_lots numeric,
    allocated_capital numeric,
    risk_capital numeric,
    risk_thresholds jsonb not null default '{}'::jsonb,
    reason text,
    regrid_required boolean not null default false,
    config jsonb not null,
    immutable boolean not null default true,
    created_at timestamptz not null default now(),
    primary key (run_id, config_version)
);

create table if not exists grid_levels (
    run_id text not null,
    config_version integer not null,
    level_id text not null,
    level_index integer not null,
    side text not null,
    price numeric not null,
    quantity numeric,
    spacing_absolute numeric,
    spacing_percentage numeric,
    state text not null default 'active',
    created_at timestamptz not null default now(),
    retired_at timestamptz,
    primary key (run_id, config_version, level_id)
);

create table if not exists grid_order_proposals (
    proposal_id text primary key,
    run_id text not null,
    bot_id text not null,
    config_version integer not null,
    level_id text,
    client_order_id text not null unique,
    side text not null,
    price numeric not null,
    quantity numeric not null,
    order_kind text,
    source_fill_id text,
    status text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists grid_orders (
    order_id text primary key,
    run_id text not null,
    bot_id text,
    config_version integer,
    level_id text,
    client_order_id text not null unique,
    exchange_order_id text,
    side text not null,
    price numeric not null,
    requested_quantity numeric not null,
    filled_quantity numeric not null default 0,
    remaining_quantity numeric not null default 0,
    average_fill_price numeric,
    order_type text,
    time_in_force text,
    post_only boolean not null default true,
    reduce_only boolean not null default false,
    status text not null,
    submitted_at timestamptz,
    cancelled_at timestamptz,
    rejection_reason text,
    order_kind text,
    source_fill_id text,
    raw jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists grid_fills (
    fill_id text primary key,
    run_id text not null,
    bot_id text,
    order_id text not null,
    level_id text,
    config_version integer,
    exchange_order_id text,
    exchange_fill_id text unique,
    side text not null,
    price numeric not null,
    quantity numeric not null,
    quantity_lots numeric,
    base_quantity numeric,
    notional numeric,
    notional_value numeric,
    liquidity_role text not null default 'unknown',
    maker_taker_role text not null default 'unknown',
    fee numeric,
    exchange_fee numeric,
    trading_fee numeric,
    fee_currency text,
    fee_status text not null default 'UNAVAILABLE',
    fee_source text,
    exchange_timestamp timestamptz,
    detected_at timestamptz not null default now(),
    rest_detection_latency numeric,
    raw jsonb,
    created_at timestamptz not null default now()
);

create table if not exists grid_cycles (
    cycle_id text primary key,
    run_id text not null,
    bot_id text,
    config_version integer,
    entry_config_version integer,
    exit_config_version integer,
    entry_fill_id text not null,
    exit_fill_id text not null,
    direction text,
    entry_level text,
    exit_level text,
    level_id text,
    quantity_lots numeric,
    base_quantity numeric,
    entry_price numeric,
    exit_price numeric,
    gross_pnl numeric,
    gross_grid_pnl numeric not null default 0,
    entry_fee numeric,
    exit_fee numeric,
    total_trading_fees numeric,
    exchange_fees numeric not null default 0,
    funding numeric not null default 0,
    other_costs numeric not null default 0,
    other_credits numeric not null default 0,
    other_costs_credits numeric not null default 0,
    net_pnl numeric,
    net_grid_pnl numeric not null default 0,
    opened_at timestamptz,
    closed_at timestamptz,
    holding_duration interval,
    duration_seconds integer,
    status text not null default 'PARTIAL',
    fee_to_gross_profit_ratio numeric,
    accounting_warnings jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    unique (run_id, entry_fill_id, exit_fill_id)
);

create table if not exists grid_exchange_costs (
    cost_id text primary key,
    run_id text not null,
    order_id text,
    fill_id text,
    cost_type text not null,
    amount numeric not null,
    currency text not null,
    direction text not null check (direction in ('debit', 'credit')),
    exchange_transaction_id text,
    config_version integer,
    attribution_status text,
    source text,
    exchange_timestamp timestamptz,
    raw jsonb,
    created_at timestamptz not null default now()
);

create table if not exists grid_bot_snapshots (
    snapshot_id text primary key,
    run_id text not null,
    timestamp timestamptz not null,
    eth_price numeric,
    active_config_version integer,
    inventory numeric,
    pending_exposure numeric,
    open_orders integer,
    gross_grid_pnl numeric,
    net_grid_pnl numeric,
    inventory_pnl numeric,
    exchange_fees numeric,
    funding numeric,
    account_equity numeric,
    margin_metrics jsonb,
    grr numeric,
    drawdown jsonb,
    risk_state text,
    execution_mode text,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists grid_risk_snapshots (
    snapshot_id text primary key default gen_random_uuid()::text,
    run_id text not null,
    timestamp timestamptz not null,
    risk_state text not null,
    inventory_utilisation numeric,
    grr numeric,
    margin_state jsonb,
    current_exposure jsonb,
    projected_exposure jsonb,
    drawdown jsonb,
    risk_thresholds jsonb,
    violations jsonb,
    blocked_orders jsonb,
    cancelled_orders jsonb,
    reason_codes jsonb
);

create table if not exists grid_parameter_changes (
    change_id text primary key,
    run_id text not null,
    bot_id text,
    from_config_version integer,
    to_config_version integer not null,
    reason text,
    changed_by text,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists grid_events (
    event_id text primary key,
    bot_id text,
    run_id text,
    event_type text not null,
    payload jsonb,
    created_at timestamptz not null default now()
);

create table if not exists grid_daily_performance (
    run_id text not null,
    date date not null,
    metrics jsonb not null,
    primary key (run_id, date)
);

create table if not exists grid_performance_daily (
    run_id text not null,
    date date not null,
    metrics jsonb not null,
    created_at timestamptz not null default now(),
    primary key (run_id, date)
);

create table if not exists grid_run_summaries (
    summary_id text primary key,
    run_id text not null unique,
    bot_id text not null,
    gridbot_version text not null,
    summary jsonb not null,
    immutable boolean not null default true,
    created_at timestamptz not null default now(),
    finalised_at timestamptz not null
);

create unique index if not exists idx_grid_runs_single_active_v01
    on grid_runs ((true))
    where status in ('STARTING', 'RUNNING', 'PAUSED', 'REGRID_PENDING');

create unique index if not exists idx_grid_order_proposals_client_order_id
    on grid_order_proposals (client_order_id);

create unique index if not exists idx_grid_orders_client_order_id
    on grid_orders (client_order_id);

create unique index if not exists idx_grid_orders_exchange_order_id
    on grid_orders (exchange_order_id)
    where exchange_order_id is not null and exchange_order_id <> '';

create index if not exists idx_grid_order_proposals_source_fill
    on grid_order_proposals (run_id, source_fill_id);

create index if not exists idx_grid_orders_source_fill
    on grid_orders (run_id, source_fill_id);

create unique index if not exists idx_grid_fills_exchange_fill_id
    on grid_fills (exchange_fill_id)
    where exchange_fill_id is not null and exchange_fill_id <> '';

create unique index if not exists idx_grid_run_summaries_run_id
    on grid_run_summaries (run_id);

create index if not exists idx_grid_runs_bot_created on grid_runs (bot_id, created_at desc);
create index if not exists idx_grid_levels_run_config on grid_levels (run_id, config_version, level_index);
create index if not exists idx_grid_orders_run on grid_orders (run_id, submitted_at desc);
create index if not exists idx_grid_fills_run on grid_fills (run_id, detected_at desc);
create index if not exists idx_grid_fills_run_config on grid_fills (run_id, config_version);
create index if not exists idx_grid_fills_fee_status on grid_fills (run_id, fee_status);
create index if not exists idx_grid_events_run on grid_events (run_id, created_at desc);
create index if not exists idx_grid_bot_snapshots_run on grid_bot_snapshots (run_id, timestamp desc);
create index if not exists idx_grid_cycles_run_config on grid_cycles (run_id, config_version);
create index if not exists idx_grid_exchange_costs_run_config on grid_exchange_costs (run_id, config_version);
create unique index if not exists idx_grid_exchange_costs_exchange_tx
    on grid_exchange_costs (exchange_transaction_id, cost_type)
    where exchange_transaction_id is not null and exchange_transaction_id <> '';

create or replace function reject_grid_run_summary_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.immutable is true then
        raise exception 'Grid Run Summary is immutable and cannot be modified';
    end if;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$$;

drop trigger if exists grid_run_summaries_immutable_update on grid_run_summaries;
create trigger grid_run_summaries_immutable_update
before update on grid_run_summaries
for each row execute function reject_grid_run_summary_mutation();

drop trigger if exists grid_run_summaries_immutable_delete on grid_run_summaries;
create trigger grid_run_summaries_immutable_delete
before delete on grid_run_summaries
for each row execute function reject_grid_run_summary_mutation();

do $$
begin
    if to_regclass('public.grid_exchange_cost_ledger') is not null then
        insert into grid_exchange_costs (
            cost_id, run_id, order_id, fill_id, cost_type, amount, currency,
            direction, exchange_transaction_id, exchange_timestamp, raw
        )
        select
            'legacy_' || cost_id::text,
            run_id,
            order_id,
            fill_id,
            exchange_cost_type,
            amount,
            currency,
            direction,
            exchange_transaction_id,
            "timestamp",
            raw_reference
        from grid_exchange_cost_ledger
        on conflict (cost_id) do nothing;
    end if;
end;
$$;
