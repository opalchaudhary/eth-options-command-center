from probability_engine.repositories.base_repository import SupabaseRepository


class SnapshotRepository(SupabaseRepository):
    table_name = "probability_market_snapshots"

    def latest(self, symbol="ETHUSD"):
        params = {"symbol": f"eq.{symbol}", "order": "timestamp.desc", "limit": "1"}
        rows = self.read(params=params)
        return rows[0] if rows else None

