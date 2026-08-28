create table if not exists grid_health_events (
    issue_key text primary key,
    run_id text,
    bot_id text,
    code text not null,
    severity text not null check (severity in ('INFO', 'WARNING', 'CRITICAL')),
    message text not null,
    first_seen timestamptz not null,
    last_seen timestamptz not null,
    occurrence_count integer not null default 1,
    active boolean not null default true,
    resolved_at timestamptz,
    operator_attention_required boolean not null default false,
    context jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_grid_health_events_run_active
    on grid_health_events (run_id, active, last_seen desc);

create index if not exists idx_grid_health_events_code_active
    on grid_health_events (code, active, last_seen desc);
