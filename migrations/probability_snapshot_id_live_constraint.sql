-- Enforce snapshot linkage for scheduler-generated live probability predictions.
-- This aborts if legacy LIVE rows exist without snapshot_id; review those rows before retrying.

begin;

do $$
declare
    null_live_count integer;
begin
    select count(*)
    into null_live_count
    from probability_predictions
    where record_type = 'LIVE'
      and snapshot_id is null;

    if null_live_count > 0 then
        raise exception 'Cannot enforce probability_predictions_live_snapshot_required: % LIVE prediction rows have null snapshot_id', null_live_count;
    end if;
end $$;

alter table probability_predictions
    drop constraint if exists probability_predictions_snapshot_id_fkey;

alter table probability_predictions
    add constraint probability_predictions_snapshot_id_fkey
    foreign key (snapshot_id)
    references probability_market_snapshots(id);

alter table probability_predictions
    drop constraint if exists probability_predictions_live_snapshot_required;

alter table probability_predictions
    add constraint probability_predictions_live_snapshot_required
    check (record_type <> 'LIVE' or snapshot_id is not null);

commit;
