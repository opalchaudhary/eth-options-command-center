-- STEP 19 MANUAL MIGRATION DRAFT: Probability V2.1 shadow-only production schema.
-- Additive and V2.1-specific. Do not grant trading authority from these tables.

create table if not exists probability_v2_1_shadow_predictions (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    prediction_timestamp timestamptz not null,
    symbol text not null,
    record_type text not null default 'LIVE',
    model_version text not null,
    model_id text not null,
    target text not null,
    horizon text not null,
    v2_baseline_prediction_id uuid references probability_v2_shadow_predictions(id) on delete restrict,
    v2_baseline_probability numeric not null check (v2_baseline_probability >= 0 and v2_baseline_probability <= 1),
    effective_probability numeric not null check (effective_probability >= 0 and effective_probability <= 1),
    inference_source text not null check (inference_source in ('RICH_V2_1', 'FALLBACK_V2_0')),
    fallback_reason text,
    feature_version text not null,
    label_version text not null default 'label_v2',
    calibration_version text not null,
    rich_family text,
    feature_source_timestamp timestamptz,
    feature_freshness_seconds numeric,
    model_artifact_hash text,
    manifest_hash text,
    high_scrutiny boolean not null default false,
    metadata_json jsonb not null default '{}'::jsonb,
    unique (prediction_timestamp, symbol, model_version, target, horizon)
);

create index if not exists idx_probability_v2_1_shadow_latest
    on probability_v2_1_shadow_predictions (symbol, prediction_timestamp desc, target, horizon);

create index if not exists idx_probability_v2_1_shadow_maturity
    on probability_v2_1_shadow_predictions (record_type, model_version, horizon, prediction_timestamp);

create index if not exists idx_probability_v2_1_shadow_source
    on probability_v2_1_shadow_predictions (inference_source, fallback_reason, prediction_timestamp desc);

create index if not exists idx_probability_v2_1_shadow_baseline
    on probability_v2_1_shadow_predictions (v2_baseline_prediction_id);

create table if not exists probability_v2_1_shadow_outcomes (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    prediction_id uuid not null references probability_v2_1_shadow_predictions(id) on delete cascade,
    label_version text not null default 'label_v2',
    target text not null,
    horizon text not null,
    evaluated_at timestamptz not null,
    outcome boolean,
    actual_open numeric,
    actual_high numeric,
    actual_low numeric,
    actual_close numeric,
    maximum_up_excursion numeric,
    maximum_down_excursion numeric,
    realized_path_range numeric,
    realized_over_range_width numeric,
    metadata_json jsonb not null default '{}'::jsonb,
    unique (prediction_id, label_version, target)
);

create index if not exists idx_probability_v2_1_shadow_outcomes_prediction
    on probability_v2_1_shadow_outcomes (prediction_id, label_version, target);

create index if not exists idx_probability_v2_1_shadow_outcomes_eval
    on probability_v2_1_shadow_outcomes (evaluated_at desc, target, horizon);

-- Rollback, if required:
-- drop table if exists probability_v2_1_shadow_outcomes;
-- drop table if exists probability_v2_1_shadow_predictions;
