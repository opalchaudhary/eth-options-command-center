from backend import config
from rich_data.orderflow_ws import WebsocketOrderflowService


_service = WebsocketOrderflowService()


def start_orderflow_ws_service():
    if not config.RICH_ORDERFLOW_WS_ENABLED:
        return False
    return _service.start()


def stop_orderflow_ws_service():
    _service.stop()


def orderflow_ws_status():
    return _service.snapshot()

