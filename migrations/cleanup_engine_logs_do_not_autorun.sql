-- DO NOT AUTO-RUN.
-- Back up/export these tables before deleting rows:
-- paper_trading_engine_runs, paper_recommendation_evaluations,
-- futures_trading_engine_runs, recommendation_journal, alt_futures_scanner_snapshots.
-- This script intentionally does not touch raw market data tables.

begin;

-- Delete old engine/evaluation logs older than 7 days.
delete from paper_trading_engine_runs
where created_at < now() - interval '7 days';

delete from paper_recommendation_evaluations
where created_at < now() - interval '7 days';

delete from futures_trading_engine_runs
where created_at < now() - interval '7 days';

delete from recommendation_journal
where created_at < now() - interval '7 days';

delete from alt_futures_scanner_snapshots
where created_at < now() - interval '7 days';

commit;

vacuum analyze paper_trading_engine_runs;
vacuum analyze paper_recommendation_evaluations;
vacuum analyze futures_trading_engine_runs;
vacuum analyze recommendation_journal;
vacuum analyze alt_futures_scanner_snapshots;

-- DEV/TEST ONLY, after backup/export:
-- truncate table paper_trading_engine_runs restart identity;
-- truncate table paper_recommendation_evaluations restart identity;
-- truncate table futures_trading_engine_runs restart identity;
-- truncate table recommendation_journal restart identity;
-- truncate table alt_futures_scanner_snapshots restart identity;

-- LAST RESORT ONLY, after backup and during a maintenance window:
-- vacuum full paper_trading_engine_runs;
-- vacuum full paper_recommendation_evaluations;
-- vacuum full futures_trading_engine_runs;
-- vacuum full recommendation_journal;
-- vacuum full alt_futures_scanner_snapshots;
