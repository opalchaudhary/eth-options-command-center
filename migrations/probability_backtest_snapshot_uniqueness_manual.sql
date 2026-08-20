-- Step 8B manual guardrail.
-- Do not run until any existing duplicate BACKTEST snapshot timestamps have
-- been reviewed/resolved. This is intentionally not applied automatically.

select
    symbol,
    timestamp,
    feature_version,
    count(*) as snapshot_count
from probability_market_snapshots
where feature_version = 'historical_reconstructible_v1'
  and metadata_json->>'record_type' = 'BACKTEST'
group by symbol, timestamp, feature_version
having count(*) > 1
order by timestamp;

-- After the preflight query returns zero rows, this unique index prevents
-- duplicate reconstructed BACKTEST snapshots for the same symbol/time/feature set.
create unique index if not exists idx_probability_backtest_snapshot_unique
    on probability_market_snapshots (symbol, timestamp, feature_version)
    where metadata_json->>'record_type' = 'BACKTEST';
