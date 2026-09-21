-- STEP 18B.2 MANUAL MIGRATION DRAFT: V2 outcome frontier state.
-- Additive V2-only infrastructure. Do not run automatically from application code.
-- Purpose: make routine V2 outcome discovery depend on new/pending work instead of
-- historical anti-joins across probability_v2_shadow_predictions/outcomes.

create table if not exists probability_v2_outcome_frontier_state (
    horizon text primary key,
    cursor_prediction_timestamp timestamptz not null,
    cursor_prediction_id uuid,
    updated_at timestamptz not null default now(),
    metadata_json jsonb not null default '{}'::jsonb
);

create table if not exists probability_v2_outcome_retry_queue (
    prediction_id uuid primary key references probability_v2_shadow_predictions(id) on delete cascade,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    retry_after timestamptz not null default now(),
    attempt_count integer not null default 0,
    horizon text not null,
    target text not null,
    prediction_timestamp timestamptz not null,
    reason text not null,
    last_error text,
    metadata_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_probability_v2_outcome_retry_due
    on probability_v2_outcome_retry_queue (retry_after, prediction_timestamp, horizon);

create index if not exists idx_probability_v2_outcome_retry_horizon_time
    on probability_v2_outcome_retry_queue (horizon, prediction_timestamp, prediction_id);

-- Existing supporting index from migrations/probability_v2_shadow_schema.sql:
-- idx_probability_v2_shadow_predictions_maturity
--   on probability_v2_shadow_predictions (record_type, model_version, horizon, prediction_timestamp)
--
-- Existing idempotency guard:
-- probability_v2_shadow_outcomes unique (prediction_id, label_version, target)
--
-- Rollback, if required:
-- drop table if exists probability_v2_outcome_retry_queue;
-- drop table if exists probability_v2_outcome_frontier_state;
