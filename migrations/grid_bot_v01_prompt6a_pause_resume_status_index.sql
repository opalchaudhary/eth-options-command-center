-- Prompt 6A: keep the single-active-run guard aligned with pause/resume
-- intermediate lifecycle states. Run manually in Supabase SQL editor when
-- upgrading an existing GridBot V0.1 schema.

drop index if exists idx_grid_runs_single_active_v01;

create unique index if not exists idx_grid_runs_single_active_v01
    on grid_runs ((true))
    where status in ('STARTING', 'RUNNING', 'PAUSING', 'PAUSED', 'RESUMING', 'REGRID_PENDING');
