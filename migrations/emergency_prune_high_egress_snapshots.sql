-- MANUAL RUN ONLY.
-- Use this in Supabase SQL Editor when project egress is close to the plan limit.
-- It keeps recent data needed by the dashboard and removes old high-volume snapshots.

begin;

delete from option_chain_snapshots
where snapshot_time < now() - interval '6 hours';

delete from premium_decay_snapshots
where snapshot_time < now() - interval '12 hours';

delete from analytics_snapshots
where snapshot_time < now() - interval '2 days';

delete from orderbook_insights
where timestamp < now() - interval '12 hours';

delete from paper_trading_engine_runs
where created_at < now() - interval '2 days';

delete from paper_recommendation_evaluations
where created_at < now() - interval '2 days';

delete from futures_trading_engine_runs
where created_at < now() - interval '2 days';

delete from alt_futures_scanner_snapshots
where created_at < now() - interval '2 days';

commit;

vacuum analyze option_chain_snapshots;
vacuum analyze premium_decay_snapshots;
vacuum analyze analytics_snapshots;
vacuum analyze orderbook_insights;
vacuum analyze paper_trading_engine_runs;
vacuum analyze paper_recommendation_evaluations;
vacuum analyze futures_trading_engine_runs;
vacuum analyze alt_futures_scanner_snapshots;
