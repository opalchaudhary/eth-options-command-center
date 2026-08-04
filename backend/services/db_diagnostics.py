from pathlib import Path


ENGINE_TABLES = [
    "recommendation_journal",
]


DIAGNOSTICS_SQL = """
select
  s.schemaname,
  s.relname as table_name,
  pg_size_pretty(pg_total_relation_size(format('%I.%I', s.schemaname, s.relname)::regclass)) as total_size,
  pg_total_relation_size(format('%I.%I', s.schemaname, s.relname)::regclass) as total_size_bytes,
  s.n_live_tup as live_rows,
  s.n_dead_tup as dead_rows,
  s.n_tup_ins as inserts,
  s.n_tup_upd as updates,
  s.n_tup_del as deletes,
  s.last_vacuum,
  s.last_autovacuum
from pg_stat_user_tables s
where s.relname in (
  'recommendation_journal'
)
order by total_size_bytes desc;

with recent as (
  select 'recommendation_journal' as table_name, date_trunc('minute', created_at) as minute, count(*) as inserts
  from recommendation_journal where created_at > now() - interval '1 hour' group by 1, 2
)
select table_name, max(inserts) as peak_inserts_per_minute, avg(inserts)::numeric(10,2) as avg_inserts_per_minute
from recent
group by table_name
order by peak_inserts_per_minute desc;
"""


def select_star_warnings():
    root = Path(__file__).resolve().parents[2]
    warnings = []
    for path in root.rglob("*.py"):
        if ".git" in path.parts or "__pycache__" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for line_no, line in enumerate(text.splitlines(), start=1):
            if '"select": "*"' in line or "'select': '*'" in line or 'select("*")' in line or "select('*')" in line:
                warnings.append(
                    {
                        "file": str(path.relative_to(root)),
                        "line": line_no,
                        "warning": "Wildcard select found in code path",
                    }
                )
    return warnings


def diagnostics_payload():
    return {
        "ok": True,
        "engine_tables": ENGINE_TABLES,
        "warnings": {
            "select_star": select_star_warnings(),
            "high_write_frequency_threshold_per_minute": 2,
        },
        "sql_to_run_in_supabase": DIAGNOSTICS_SQL.strip(),
    }
