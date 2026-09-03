create extension if not exists pgcrypto;

create table if not exists grid_parameter_recommendations (
    recommendation_id text primary key,
    created_at timestamptz not null default now(),
    requested_at timestamptz not null,
    prediction_timestamp timestamptz,
    symbol text not null,
    recommender_version text not null,
    policy_version text,
    probability_model_version text,
    spot_price numeric,
    selected_operating_horizon text,
    path_inside_70 numeric,
    realized_over_range_width_ge_1 numeric,
    upside_probability numeric,
    downside_probability numeric,
    range_70_lower numeric,
    range_70_upper numeric,
    v2_ood boolean,
    v2_abstained boolean,
    v2_stale boolean,
    source_prediction_id text,
    recommended_grid_type text,
    recommended_lower_price numeric,
    recommended_upper_price numeric,
    recommended_grid_count integer,
    recommended_spacing_type text,
    recommended_grid_step numeric,
    recommender_confidence numeric,
    recommendation_action text not null,
    reason_codes jsonb not null default '[]'::jsonb,
    reasons jsonb not null default '[]'::jsonb,
    active_run_id text,
    bot_id text,
    config_version integer,
    current_grid_type text,
    current_lower_price numeric,
    current_upper_price numeric,
    current_grid_count integer,
    current_spacing_type text,
    current_grid_step numeric,
    current_lot_size numeric,
    current_max_inventory_lots numeric,
    probability_source text,
    range_source text,
    spot_source text,
    current_grid_source text,
    metadata_json jsonb not null default '{}'::jsonb,
    immutable boolean not null default true
);

create index if not exists idx_grid_param_recs_created_at
    on grid_parameter_recommendations (created_at desc);

create index if not exists idx_grid_param_recs_prediction_timestamp
    on grid_parameter_recommendations (prediction_timestamp desc);

create index if not exists idx_grid_param_recs_symbol
    on grid_parameter_recommendations (symbol);

create index if not exists idx_grid_param_recs_horizon
    on grid_parameter_recommendations (selected_operating_horizon);

create index if not exists idx_grid_param_recs_recommender_version
    on grid_parameter_recommendations (recommender_version);

create index if not exists idx_grid_param_recs_action
    on grid_parameter_recommendations (recommendation_action);

create index if not exists idx_grid_param_recs_active_run
    on grid_parameter_recommendations (active_run_id)
    where active_run_id is not null;

create or replace function reject_grid_parameter_recommendation_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.immutable is true then
        raise exception 'Grid Parameter Recommendation snapshot is immutable and cannot be modified';
    end if;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$$;

drop trigger if exists grid_parameter_recommendations_immutable_update on grid_parameter_recommendations;
create trigger grid_parameter_recommendations_immutable_update
before update on grid_parameter_recommendations
for each row execute function reject_grid_parameter_recommendation_mutation();

drop trigger if exists grid_parameter_recommendations_immutable_delete on grid_parameter_recommendations;
create trigger grid_parameter_recommendations_immutable_delete
before delete on grid_parameter_recommendations
for each row execute function reject_grid_parameter_recommendation_mutation();
