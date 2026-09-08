create extension if not exists pgcrypto;

create table if not exists grid_parameter_recommendation_outcomes (
    outcome_id uuid primary key default gen_random_uuid(),
    recommendation_id text not null references grid_parameter_recommendations(recommendation_id) on delete restrict,
    horizon text not null,
    evaluated_at timestamptz not null default now(),
    evaluation_start timestamptz not null,
    evaluation_end timestamptz not null,
    symbol text not null,
    recommender_version text not null,
    recommended_lower_price numeric not null,
    recommended_upper_price numeric not null,
    recommended_width numeric not null,
    actual_forward_low numeric,
    actual_forward_high numeric,
    actual_path_width numeric,
    stayed_inside_recommended_range boolean not null,
    upper_breached boolean not null,
    lower_breached boolean not null,
    both_sides_breached boolean not null,
    first_breach_side text,
    first_breach_time timestamptz,
    minutes_to_first_breach numeric,
    max_excursion_above_upper numeric not null default 0,
    max_excursion_below_lower numeric not null default 0,
    actual_recommended_width_ratio numeric,
    range_utilization numeric,
    spot_at_evaluation_end numeric,
    ohlcv_source text not null default 'eth_ohlcv',
    ohlcv_resolution text,
    expected_candles integer not null default 0,
    actual_candles integer not null default 0,
    data_completeness_pct numeric not null default 0,
    data_quality_flag text not null,
    metadata_json jsonb not null default '{}'::jsonb,
    immutable boolean not null default true,
    created_at timestamptz not null default now(),
    unique (recommendation_id, horizon)
);

create index if not exists idx_grid_param_rec_outcomes_recommendation
    on grid_parameter_recommendation_outcomes (recommendation_id);

create index if not exists idx_grid_param_rec_outcomes_horizon
    on grid_parameter_recommendation_outcomes (horizon);

create index if not exists idx_grid_param_rec_outcomes_version
    on grid_parameter_recommendation_outcomes (recommender_version);

create index if not exists idx_grid_param_rec_outcomes_evaluation_end
    on grid_parameter_recommendation_outcomes (evaluation_end desc);

create or replace function reject_grid_parameter_recommendation_outcome_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.immutable is true then
        raise exception 'Grid Parameter Recommendation outcome is immutable and cannot be modified';
    end if;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$$;

drop trigger if exists grid_parameter_recommendation_outcomes_immutable_update on grid_parameter_recommendation_outcomes;
create trigger grid_parameter_recommendation_outcomes_immutable_update
before update on grid_parameter_recommendation_outcomes
for each row execute function reject_grid_parameter_recommendation_outcome_mutation();

drop trigger if exists grid_parameter_recommendation_outcomes_immutable_delete on grid_parameter_recommendation_outcomes;
create trigger grid_parameter_recommendation_outcomes_immutable_delete
before delete on grid_parameter_recommendation_outcomes
for each row execute function reject_grid_parameter_recommendation_outcome_mutation();
