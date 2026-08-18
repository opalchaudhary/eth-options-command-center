from typing import Protocol


class OrderFlowProvider(Protocol):
    async def get_trades(self, symbol: str, limit: int = 1000) -> list[dict]:
        ...

    async def get_order_book(self, symbol: str, depth: int = 50) -> dict:
        ...


class DisabledOrderFlowProvider:
    async def get_trades(self, symbol: str, limit: int = 1000) -> list[dict]:
        return []

    async def get_order_book(self, symbol: str, depth: int = 50) -> dict:
        return {"symbol": symbol, "bids": [], "asks": [], "status": "DISABLED"}

