-- Step 8B manual guardrail.
-- Do not run until any existing duplicate BACKTEST snapshot timestamps have
-- been reviewed/resolved. This is intentionally not applied automatically.

select
    symbol,
    timestamp,
    feature_version,
    metadata_json->>'backtest_version' as backtest_version,
    count(*) as snapshot_count
from probability_market_snapshots
where feature_version = 'historical_reconstructible_v1'
  and metadata_json->>'record_type' = 'BACKTEST'
group by symbol, timestamp, feature_version, metadata_json->>'backtest_version'
having count(*) > 1
order by timestamp;

-- After the preflight query returns zero rows, this unique index prevents
-- duplicate reconstructed BACKTEST snapshots for the same symbol/time/feature
-- set/backtest methodology while allowing future backtest versions to coexist.
create unique index if not exists idx_probability_backtest_snapshot_unique
    on probability_market_snapshots (
        symbol,
        timestamp,
        feature_version,
        (metadata_json->>'backtest_version')
    )
    where metadata_json->>'record_type' = 'BACKTEST';

select
    symbol,
    created_at,
    horizon,
    model_version,
    feature_version,
    metadata_json->>'backtest_version' as backtest_version,
    count(*) as prediction_count
from probability_predictions
where record_type = 'BACKTEST'
  and feature_version = 'historical_reconstructible_v1'
group by
    symbol,
    created_at,
    horizon,
    model_version,
    feature_version,
    metadata_json->>'backtest_version'
having count(*) > 1
order by created_at, horizon;

create unique index if not exists idx_probability_backtest_prediction_unique
    on probability_predictions (
        symbol,
        created_at,
        horizon,
        model_version,
        feature_version,
        (metadata_json->>'backtest_version')
    )
    where record_type = 'BACKTEST';
