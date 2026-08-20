from rich_data.derivatives import DerivativesCollector
from rich_data.orderbook import OrderbookCollector
from rich_data.orderflow import OrderflowCollector


def run_rich_derivatives_job():
    return DerivativesCollector().collect()


def run_rich_orderflow_job():
    return OrderflowCollector().collect()


def run_rich_orderbook_job():
    return OrderbookCollector().collect()

