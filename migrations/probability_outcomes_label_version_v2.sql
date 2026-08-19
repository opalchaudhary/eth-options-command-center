-- Step 7B: allow auditable Label V2 outcomes to coexist with historical Label V1 rows.
-- Run manually in the Supabase SQL Editor before deploying Label V2 code.

alter table probability_outcomes
    add column if not exists label_version text not null default 'label_v1';

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'probability_outcomes_label_version_check'
          and conrelid = 'probability_outcomes'::regclass
    ) then
        alter table probability_outcomes
            add constraint probability_outcomes_label_version_check
            check (label_version in ('label_v1', 'label_v2'));
    end if;
end $$;

do $$
declare
    existing_constraint record;
begin
    for existing_constraint in
        select c.conname
        from pg_constraint c
        join pg_attribute a
          on a.attrelid = c.conrelid
         and a.attnum = c.conkey[1]
        where c.conrelid = 'probability_outcomes'::regclass
          and c.contype = 'u'
          and array_length(c.conkey, 1) = 1
          and a.attname = 'prediction_id'
    loop
        execute format(
            'alter table probability_outcomes drop constraint %I',
            existing_constraint.conname
        );
    end loop;
end $$;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'probability_outcomes_prediction_label_version_key'
          and conrelid = 'probability_outcomes'::regclass
    ) then
        alter table probability_outcomes
            add constraint probability_outcomes_prediction_label_version_key
            unique (prediction_id, label_version);
    end if;
end $$;

create index if not exists idx_probability_outcomes_label_version
    on probability_outcomes (label_version, evaluated_at desc);

create index if not exists idx_probability_outcomes_prediction_label_version
    on probability_outcomes (prediction_id, label_version);
