# Paper Trading Daemon

The Paper Trading engine is intentionally independent of Streamlit. The web app does not start trades, refresh trades, or control execution. It only reads persisted state from Supabase.

To keep trading while your browser and Windows machine are closed, run `paper_trading_daemon.py` on always-on compute:

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

The daemon writes every cycle to `paper_trading_engine_runs`, plus recommendation evaluations, wallet snapshots, and paper trades. When you later open the Streamlit app, the Paper Trading page reads those records and shows what happened while you were away.
