-- MANUAL RUN ONLY.
-- Use this in Supabase SQL Editor when project egress is close to the plan limit.
-- This chunked version avoids one large transaction and skips VACUUM, because the
-- Supabase SQL Editor can fail to fetch results for long-running maintenance work.
--
-- Run this script repeatedly until each deleted_* value returns 0.

with deleted as (
    delete from option_chain_snapshots
    where ctid in (
        select ctid
        from option_chain_snapshots
        where snapshot_time < now() - interval '6 hours'
        limit 50000
    )
    returning 1
)
select count(*) as deleted_option_chain_snapshots from deleted;

with deleted as (
    delete from premium_decay_snapshots
    where ctid in (
        select ctid
        from premium_decay_snapshots
        where snapshot_time < now() - interval '12 hours'
        limit 20000
    )
    returning 1
)
select count(*) as deleted_premium_decay_snapshots from deleted;

with deleted as (
    delete from analytics_snapshots
    where ctid in (
        select ctid
        from analytics_snapshots
        where snapshot_time < now() - interval '2 days'
        limit 20000
    )
    returning 1
)
select count(*) as deleted_analytics_snapshots from deleted;

with deleted as (
    delete from orderbook_insights
    where ctid in (
        select ctid
        from orderbook_insights
        where timestamp < now() - interval '12 hours'
        limit 20000
    )
    returning 1
)
select count(*) as deleted_orderbook_insights from deleted;
