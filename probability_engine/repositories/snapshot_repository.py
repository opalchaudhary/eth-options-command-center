from probability_engine.repositories.base_repository import SupabaseRepository


class SnapshotRepository(SupabaseRepository):
    table_name = "probability_market_snapshots"

    def latest(self, symbol="ETHUSD"):
        params = {"symbol": f"eq.{symbol}", "order": "timestamp.desc", "limit": "1"}
        rows = self.read(params=params)
        return rows[0] if rows else None

    def by_ids(self, snapshot_ids):
        clean_ids = [str(item) for item in snapshot_ids if item]
        if not clean_ids:
            return {}
        rows = self.read(
            params={
                "select": "id,spot_price,vwap,vwap_zscore,atr,atr_pct,return_1h,return_4h,regime,timestamp",
                "id": f"in.({','.join(clean_ids)})",
                "limit": str(len(clean_ids)),
            }
        )
        if rows is None:
            return {}
        if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
            records = [] if rows.empty else rows.to_dict("records")
        elif isinstance(rows, dict):
            records = [rows]
        else:
            records = list(rows or [])
        return {row.get("id"): row for row in records if row.get("id")}
