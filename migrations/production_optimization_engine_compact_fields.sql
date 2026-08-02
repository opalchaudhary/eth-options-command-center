-- Production optimization fields for compact engine persistence.
-- Safe to run multiple times. Existing raw JSON columns are kept nullable.

do $$
declare
    tbl text;
begin
    foreach tbl in array array[
        'paper_trading_engine_runs',
        'paper_recommendation_evaluations',
        'futures_trading_engine_runs',
        'recommendation_journal',
        'alt_futures_scanner_snapshots',
        'alt_futures_engine_runs'
    ]
    loop
        execute format('alter table if exists %I add column if not exists symbol text', tbl);
        execute format('alter table if exists %I add column if not exists engine_name text', tbl);
        execute format('alter table if exists %I add column if not exists run_status text', tbl);
        execute format('alter table if exists %I add column if not exists action text', tbl);
        execute format('alter table if exists %I add column if not exists market_regime text', tbl);
        execute format('alter table if exists %I add column if not exists selected_strategy text', tbl);
        execute format('alter table if exists %I add column if not exists selected_expiry text', tbl);
        execute format('alter table if exists %I add column if not exists selected_strikes jsonb', tbl);
        execute format('alter table if exists %I add column if not exists top_candidates_summary jsonb', tbl);
        execute format('alter table if exists %I add column if not exists greeks_summary jsonb', tbl);
        execute format('alter table if exists %I add column if not exists confidence_score numeric', tbl);
        execute format('alter table if exists %I add column if not exists risk_score numeric', tbl);
        execute format('alter table if exists %I add column if not exists margin_used numeric', tbl);
        execute format('alter table if exists %I add column if not exists lot_size numeric', tbl);
        execute format('alter table if exists %I add column if not exists pnl numeric', tbl);
        execute format('alter table if exists %I add column if not exists unrealized_pnl numeric', tbl);
        execute format('alter table if exists %I add column if not exists realized_pnl numeric', tbl);
        execute format('alter table if exists %I add column if not exists decision_reason text', tbl);
        execute format('alter table if exists %I add column if not exists decision_hash text', tbl);
        execute format('alter table if exists %I add column if not exists snapshot_refs jsonb', tbl);
        execute format('alter table if exists %I add column if not exists payload_truncated boolean default false', tbl);
    end loop;
end $$;

create index if not exists idx_paper_trading_engine_runs_created_at on paper_trading_engine_runs (created_at desc);
create index if not exists idx_paper_trading_engine_runs_symbol on paper_trading_engine_runs (symbol);
create index if not exists idx_paper_trading_engine_runs_engine_name on paper_trading_engine_runs (engine_name);
create index if not exists idx_paper_trading_engine_runs_action on paper_trading_engine_runs (action);
create index if not exists idx_paper_trading_engine_runs_selected_strategy on paper_trading_engine_runs (selected_strategy);
create index if not exists idx_paper_trading_engine_runs_decision_hash on paper_trading_engine_runs (decision_hash);

create index if not exists idx_paper_recommendation_evaluations_created_at on paper_recommendation_evaluations (created_at desc);
create index if not exists idx_paper_recommendation_evaluations_symbol on paper_recommendation_evaluations (symbol);
create index if not exists idx_paper_recommendation_evaluations_engine_name on paper_recommendation_evaluations (engine_name);
create index if not exists idx_paper_recommendation_evaluations_action on paper_recommendation_evaluations (action);
create index if not exists idx_paper_recommendation_evaluations_selected_strategy on paper_recommendation_evaluations (selected_strategy);
create index if not exists idx_paper_recommendation_evaluations_decision_hash on paper_recommendation_evaluations (decision_hash);

create index if not exists idx_futures_trading_engine_runs_created_at on futures_trading_engine_runs (created_at desc);
create index if not exists idx_futures_trading_engine_runs_symbol on futures_trading_engine_runs (symbol);
create index if not exists idx_futures_trading_engine_runs_engine_name on futures_trading_engine_runs (engine_name);
create index if not exists idx_futures_trading_engine_runs_action on futures_trading_engine_runs (action);
create index if not exists idx_futures_trading_engine_runs_selected_strategy on futures_trading_engine_runs (selected_strategy);
create index if not exists idx_futures_trading_engine_runs_decision_hash on futures_trading_engine_runs (decision_hash);

create index if not exists idx_recommendation_journal_created_at on recommendation_journal (created_at desc);
create index if not exists idx_recommendation_journal_symbol on recommendation_journal (symbol);
create index if not exists idx_recommendation_journal_engine_name on recommendation_journal (engine_name);
create index if not exists idx_recommendation_journal_action on recommendation_journal (action);
create index if not exists idx_recommendation_journal_selected_strategy on recommendation_journal (selected_strategy);
create index if not exists idx_recommendation_journal_decision_hash on recommendation_journal (decision_hash);

create index if not exists idx_alt_futures_scanner_snapshots_created_at on alt_futures_scanner_snapshots (created_at desc);
create index if not exists idx_alt_futures_scanner_snapshots_symbol on alt_futures_scanner_snapshots (symbol);
create index if not exists idx_alt_futures_scanner_snapshots_engine_name on alt_futures_scanner_snapshots (engine_name);
create index if not exists idx_alt_futures_scanner_snapshots_action on alt_futures_scanner_snapshots (action);
create index if not exists idx_alt_futures_scanner_snapshots_selected_strategy on alt_futures_scanner_snapshots (selected_strategy);
create index if not exists idx_alt_futures_scanner_snapshots_decision_hash on alt_futures_scanner_snapshots (decision_hash);
