-- STEP 16 MANUAL MIGRATION: Probability V2 candidate live shadow storage.
-- Research/shadow only. Do not run automatically from application code.
-- V1 tables remain unchanged and probability_v1 remains the permanent control.

create extension if not exists pgcrypto;

create table if not exists probability_v2_feature_snapshots (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    prediction_timestamp timestamptz not null,
    symbol text not null,
    record_type text not null check (record_type in ('LIVE', 'BACKTEST')),
    model_version text not null,
    feature_version text not null,
    manifest_hash text not null,
    feature_source_cutoff timestamptz,
    regime text,
    feature_vector_json jsonb not null default '{}'::jsonb,
    threshold_metadata_json jsonb not null default '{}'::jsonb,
    ood_status text not null default 'NOT_EVALUATED',
    ood_reason text,
    ood_feature_count integer not null default 0,
    abstained boolean not null default false,
    abstention_reason text,
    metadata_json jsonb not null default '{}'::jsonb,
    unique (prediction_timestamp, symbol, record_type, model_version, feature_version, manifest_hash)
);

create index if not exists idx_probability_v2_feature_snapshots_time
    on probability_v2_feature_snapshots (symbol, prediction_timestamp desc);

create table if not exists probability_v2_shadow_predictions (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    feature_snapshot_id uuid references probability_v2_feature_snapshots(id) on delete cascade,
    prediction_timestamp timestamptz not null,
    symbol text not null,
    record_type text not null check (record_type in ('LIVE', 'BACKTEST')),
    model_version text not null,
    model_id text not null,
    target text not null,
    horizon text not null,
    raw_probability numeric,
    calibrated_probability numeric,
    feature_version text not null,
    label_version text not null,
    calibration_version text not null,
    regime text,
    historical_quality_grade text,
    derived boolean not null default false,
    derived_from_model_id text,
    abstained boolean not null default false,
    abstention_reason text,
    ood_status text not null default 'NOT_EVALUATED',
    ood_reason text,
    ood_feature_count integer not null default 0,
    feature_source_cutoff timestamptz,
    manifest_hash text not null,
    model_artifact_hash text,
    metadata_json jsonb not null default '{}'::jsonb,
    unique (prediction_timestamp, symbol, model_version, target, horizon)
);

create index if not exists idx_probability_v2_shadow_predictions_latest
    on probability_v2_shadow_predictions (symbol, prediction_timestamp desc, target, horizon);

create index if not exists idx_probability_v2_shadow_predictions_maturity
    on probability_v2_shadow_predictions (record_type, model_version, horizon, prediction_timestamp);

create table if not exists probability_v2_shadow_outcomes (
    id uuid primary key default gen_random_uuid(),
    prediction_id uuid not null references probability_v2_shadow_predictions(id) on delete cascade,
    evaluated_at timestamptz not null default now(),
    label_version text not null,
    target text not null,
    horizon text not null,
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

create index if not exists idx_probability_v2_shadow_outcomes_prediction
    on probability_v2_shadow_outcomes (prediction_id);

-- Verification queries:
-- select to_regclass('probability_v2_feature_snapshots');
-- select to_regclass('probability_v2_shadow_predictions');
-- select to_regclass('probability_v2_shadow_outcomes');
