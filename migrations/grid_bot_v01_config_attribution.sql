alter table if exists grid_fills
    add column if not exists config_version integer;

alter table if exists grid_cycles
    add column if not exists config_version integer,
    add column if not exists entry_config_version integer,
    add column if not exists exit_config_version integer;

alter table if exists grid_exchange_costs
    add column if not exists config_version integer;

alter table if exists grid_order_proposals
    add column if not exists source_fill_id text;

alter table if exists grid_orders
    add column if not exists source_fill_id text;

create index if not exists idx_grid_fills_run_config
    on grid_fills (run_id, config_version);

create index if not exists idx_grid_cycles_run_config
    on grid_cycles (run_id, config_version);

create index if not exists idx_grid_exchange_costs_run_config
    on grid_exchange_costs (run_id, config_version);

create index if not exists idx_grid_order_proposals_source_fill
    on grid_order_proposals (run_id, source_fill_id);

create index if not exists idx_grid_orders_source_fill
    on grid_orders (run_id, source_fill_id);
