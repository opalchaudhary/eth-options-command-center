import threading

import alt_futures_trading_daemon


_lock = threading.Lock()
_stop_event = threading.Event()
_thread = None


def _worker_loop():
    print("Streamlit-hosted alt futures scanner worker started.")

    while not _stop_event.is_set():
        interval_seconds = alt_futures_trading_daemon.daemon_config()

        try:
            evaluation = alt_futures_trading_daemon.run_cycle(interval_seconds)
            print(f"Alt futures scanner cycle: {evaluation.get('action')}")
        except Exception as exc:
            print(f"Alt futures scanner cycle failed: {exc}")

        _stop_event.wait(interval_seconds)


def start_streamlit_alt_futures_worker():
    global _thread

    with _lock:
        if _thread and _thread.is_alive():
            return {
                "running": True,
                "mode": "streamlit-hosted",
            }

        _stop_event.clear()
        _thread = threading.Thread(
            target=_worker_loop,
            name="streamlit-alt-futures-worker",
            daemon=True,
        )
        _thread.start()
        return {
            "running": True,
            "mode": "streamlit-hosted",
        }


def streamlit_alt_futures_worker_status():
    return {
        "running": bool(_thread and _thread.is_alive()),
        "mode": "streamlit-hosted",
    }
