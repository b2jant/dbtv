# Performance benchmark harness

Collect cold, warm, refresh, and offline runs for the same selector and hardware. Keep
the remote dbt command baseline separately, then summarize dbtv `run.json` artifacts:

```bash
uv run python benchmarks/summarize_runs.py /path/to/project/.dbtv/runs/*/run.json
```

Report selection, source sizes, row counts, remote baseline, dbtv planning/extraction/
binding/dbt phase times, total wall time, peak RSS from the host benchmark tool, cache
size, DuckDB size, and hardware. A pilot should gate on relative warm/offline speedup,
bounded memory, and no regression in planning/startup p95.
