alter table if exists grid_fills
    add column if not exists config_version integer,
    add column if not exists exchange_order_id text,
    add column if not exists quantity_lots numeric,
    add column if not exists base_quantity numeric,
    add column if not exists notional_value numeric,
    add column if not exists maker_taker_role text not null default 'unknown',
    add column if not exists trading_fee numeric,
    add column if not exists fee_status text not null default 'UNAVAILABLE',
    add column if not exists fee_source text;

alter table if exists grid_cycles
    add column if not exists config_version integer,
    add column if not exists entry_config_version integer,
    add column if not exists exit_config_version integer,
    add column if not exists direction text,
    add column if not exists entry_level text,
    add column if not exists exit_level text,
    add column if not exists quantity_lots numeric,
    add column if not exists base_quantity numeric,
    add column if not exists entry_price numeric,
    add column if not exists exit_price numeric,
    add column if not exists gross_pnl numeric,
    add column if not exists entry_fee numeric,
    add column if not exists exit_fee numeric,
    add column if not exists total_trading_fees numeric,
    add column if not exists other_costs numeric not null default 0,
    add column if not exists other_credits numeric not null default 0,
    add column if not exists net_pnl numeric,
    add column if not exists opened_at timestamptz,
    add column if not exists closed_at timestamptz,
    add column if not exists duration_seconds integer,
    add column if not exists status text not null default 'PARTIAL',
    add column if not exists fee_to_gross_profit_ratio numeric,
    add column if not exists accounting_warnings jsonb not null default '[]'::jsonb;

alter table if exists grid_exchange_costs
    add column if not exists config_version integer,
    add column if not exists attribution_status text,
    add column if not exists source text;

alter table if exists grid_order_proposals
    add column if not exists source_fill_id text;

alter table if exists grid_orders
    add column if not exists source_fill_id text;

create index if not exists idx_grid_fills_run_config
    on grid_fills (run_id, config_version);

create index if not exists idx_grid_fills_fee_status
    on grid_fills (run_id, fee_status);

create index if not exists idx_grid_cycles_run_config
    on grid_cycles (run_id, config_version);

create index if not exists idx_grid_exchange_costs_run_config
    on grid_exchange_costs (run_id, config_version);

create unique index if not exists idx_grid_exchange_costs_exchange_tx
    on grid_exchange_costs (exchange_transaction_id, cost_type)
    where exchange_transaction_id is not null and exchange_transaction_id <> '';

create index if not exists idx_grid_order_proposals_source_fill
    on grid_order_proposals (run_id, source_fill_id);

create index if not exists idx_grid_orders_source_fill
    on grid_orders (run_id, source_fill_id);
