# Paper Trading Daemon

The Paper Trading engine does not depend on buttons, Streamlit session state, or the Paper Trading page refresh cycle.

There are two supported runtime modes.

## Streamlit-hosted mode

When the Streamlit Cloud app is awake, `app.py` and the Paper Trading page start one process-local background worker with `paper_trading_runtime.start_streamlit_paper_trading_worker()`.

This is suitable if you access the app often enough that Streamlit Cloud does not hibernate it. Streamlit Community Cloud can sleep after inactivity, so trading pauses while the app is asleep and resumes when the app wakes.

## Always-on worker mode

To keep trading even when Streamlit Cloud sleeps, run `paper_trading_daemon.py` on always-on compute:

- a VPS
- a cloud worker service
- a container platform
- a background worker process on the same host as your deployed app

Required environment variables:

```text
SUPABASE_URL=...
SUPABASE_KEY=...
PAPER_TRADING_INTERVAL_SECONDS=60
PAPER_TRADING_LIMIT_EXPIRIES=6
```

Generic command:

```bash
python paper_trading_daemon.py
```

Container command:

```bash
docker build -f Dockerfile.worker -t eth-options-paper-worker .
docker run --env-file .env eth-options-paper-worker
```

Platforms that support Procfile workers can run:

```text
worker: python paper_trading_daemon.py
```

Both modes write every cycle to `paper_trading_engine_runs`, plus recommendation evaluations, wallet snapshots, and paper trades. The Paper Trading page reads those records and shows the latest engine heartbeat and trade state.
