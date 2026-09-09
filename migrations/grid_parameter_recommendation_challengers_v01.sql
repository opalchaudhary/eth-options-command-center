create extension if not exists pgcrypto;

create table if not exists grid_parameter_recommendation_challengers (
    challenger_id text primary key,
    recommendation_id text not null references grid_parameter_recommendations(recommendation_id) on delete restrict,
    challenger_policy text not null,
    challenger_policy_version text not null,
    generated_at timestamptz not null default now(),
    prediction_timestamp timestamptz,
    symbol text not null,
    horizon text,
    shadow_action text not null,
    shadow_grid_type text,
    shadow_lower_price numeric,
    shadow_upper_price numeric,
    shadow_grid_count integer,
    shadow_spacing_type text,
    shadow_grid_step numeric,
    no_grid boolean not null default false,
    no_grid_reason text,
    metadata_json jsonb not null default '{}'::jsonb,
    immutable boolean not null default true,
    created_at timestamptz not null default now(),
    unique (recommendation_id, challenger_policy)
);

create index if not exists idx_grid_param_rec_challengers_recommendation
    on grid_parameter_recommendation_challengers (recommendation_id);

create index if not exists idx_grid_param_rec_challengers_policy
    on grid_parameter_recommendation_challengers (challenger_policy, challenger_policy_version);

create index if not exists idx_grid_param_rec_challengers_generated
    on grid_parameter_recommendation_challengers (generated_at desc);

create index if not exists idx_grid_param_rec_challengers_horizon
    on grid_parameter_recommendation_challengers (horizon);

create table if not exists grid_parameter_recommendation_challenger_outcomes (
    challenger_outcome_id uuid primary key default gen_random_uuid(),
    challenger_id text not null references grid_parameter_recommendation_challengers(challenger_id) on delete restrict,
    recommendation_id text not null references grid_parameter_recommendations(recommendation_id) on delete restrict,
    challenger_policy text not null,
    challenger_policy_version text not null,
    horizon text not null,
    evaluated_at timestamptz not null default now(),
    evaluation_start timestamptz not null,
    evaluation_end timestamptz not null,
    symbol text not null,
    no_grid boolean not null default false,
    shadow_action text not null,
    shadow_lower_price numeric,
    shadow_upper_price numeric,
    shadow_width numeric,
    width_vs_champion numeric,
    actual_forward_low numeric,
    actual_forward_high numeric,
    actual_path_width numeric,
    stayed_inside_shadow_range boolean not null,
    upper_breached boolean not null,
    lower_breached boolean not null,
    both_sides_breached boolean not null,
    first_breach_side text,
    first_breach_time timestamptz,
    minutes_to_first_breach numeric,
    max_excursion_above_upper numeric,
    max_excursion_below_lower numeric,
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
    unique (challenger_id, horizon)
);

create index if not exists idx_grid_param_rec_challenger_outcomes_challenger
    on grid_parameter_recommendation_challenger_outcomes (challenger_id);

create index if not exists idx_grid_param_rec_challenger_outcomes_recommendation
    on grid_parameter_recommendation_challenger_outcomes (recommendation_id);

create index if not exists idx_grid_param_rec_challenger_outcomes_policy
    on grid_parameter_recommendation_challenger_outcomes (challenger_policy, challenger_policy_version);

create index if not exists idx_grid_param_rec_challenger_outcomes_horizon
    on grid_parameter_recommendation_challenger_outcomes (horizon);

create index if not exists idx_grid_param_rec_challenger_outcomes_evaluation_end
    on grid_parameter_recommendation_challenger_outcomes (evaluation_end desc);

create or replace function reject_grid_parameter_recommendation_challenger_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.immutable is true then
        raise exception 'Grid Parameter Recommendation challenger snapshot is immutable and cannot be modified';
    end if;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$$;

drop trigger if exists grid_parameter_recommendation_challengers_immutable_update on grid_parameter_recommendation_challengers;
create trigger grid_parameter_recommendation_challengers_immutable_update
before update on grid_parameter_recommendation_challengers
for each row execute function reject_grid_parameter_recommendation_challenger_mutation();

drop trigger if exists grid_parameter_recommendation_challengers_immutable_delete on grid_parameter_recommendation_challengers;
create trigger grid_parameter_recommendation_challengers_immutable_delete
before delete on grid_parameter_recommendation_challengers
for each row execute function reject_grid_parameter_recommendation_challenger_mutation();

create or replace function reject_grid_parameter_recommendation_challenger_outcome_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.immutable is true then
        raise exception 'Grid Parameter Recommendation challenger outcome is immutable and cannot be modified';
    end if;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$$;

drop trigger if exists grid_parameter_recommendation_challenger_outcomes_immutable_update on grid_parameter_recommendation_challenger_outcomes;
create trigger grid_parameter_recommendation_challenger_outcomes_immutable_update
before update on grid_parameter_recommendation_challenger_outcomes
for each row execute function reject_grid_parameter_recommendation_challenger_outcome_mutation();

drop trigger if exists grid_parameter_recommendation_challenger_outcomes_immutable_delete on grid_parameter_recommendation_challenger_outcomes;
create trigger grid_parameter_recommendation_challenger_outcomes_immutable_delete
before delete on grid_parameter_recommendation_challenger_outcomes
for each row execute function reject_grid_parameter_recommendation_challenger_outcome_mutation();
