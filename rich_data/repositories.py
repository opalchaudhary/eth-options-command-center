import logging

import database_reader
import storage


logger = logging.getLogger(__name__)


class RichDataRepository:
    table_name = None
    conflict_columns = None

    def upsert_many(self, rows):
        if not rows:
            return True
        if not storage.SUPABASE_URL or not storage.SUPABASE_KEY:
            logger.warning("rich_data.supabase.not_configured")
            return False

        url = f"{storage.SUPABASE_URL}/rest/v1/{self.table_name}"
        params = {}
        if self.conflict_columns:
            params["on_conflict"] = self.conflict_columns

        try:
            response = storage.requests.post(
                url,
                headers={
                    **storage.HEADERS,
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                params=params,
                json=rows,
                timeout=15,
            )
            if response.status_code in [200, 201, 204]:
                return True
            logger.error(
                "rich_data.upsert.failed table=%s status=%s body=%s",
                self.table_name,
                response.status_code,
                response.text,
            )
            return False
        except Exception:
            logger.exception("rich_data.upsert.exception table=%s", self.table_name)
            return False

    def upsert_one(self, row):
        return self.upsert_many([row] if row else [])

    def read(self, params=None):
        return database_reader.read_supabase_table(self.table_name, params=params or {})


class DerivativesMetricRepository(RichDataRepository):
    table_name = "derivatives_metric_snapshots"
    conflict_columns = "symbol,timestamp,version"

    def latest_before(self, symbol="ETHUSD", before_iso=None):
        params = {
            "select": "timestamp,open_interest,funding_rate",
            "symbol": f"eq.{symbol}",
            "order": "timestamp.desc",
            "limit": "1",
        }
        if before_iso:
            params["timestamp"] = f"lt.{before_iso}"
        rows = self.read(params)
        if hasattr(rows, "empty"):
            return None if rows.empty else rows.iloc[0].to_dict()
        return rows[0] if rows else None

    def recent_funding(self, symbol="ETHUSD", limit=288):
        rows = self.read(
            {
                "select": "funding_rate",
                "symbol": f"eq.{symbol}",
                "funding_rate": "not.is.null",
                "order": "timestamp.desc",
                "limit": str(limit),
            }
        )
        if hasattr(rows, "empty"):
            return [] if rows.empty else rows["funding_rate"].dropna().tolist()
        return [row.get("funding_rate") for row in rows or [] if row.get("funding_rate") is not None]


class OrderflowAggregateRepository(RichDataRepository):
    table_name = "orderflow_aggregates"
    conflict_columns = "symbol,bucket_timestamp,version"

    def recent_cvd(self, symbol="ETHUSD", before_iso=None, limit=60):
        params = {
            "select": "bucket_timestamp,cvd_increment,cvd_running",
            "symbol": f"eq.{symbol}",
            "order": "bucket_timestamp.desc",
            "limit": str(limit),
        }
        if before_iso:
            params["bucket_timestamp"] = f"lt.{before_iso}"
        rows = self.read(params)
        if hasattr(rows, "empty"):
            return [] if rows.empty else rows.to_dict("records")
        return list(rows or [])


class OrderbookAggregateRepository(RichDataRepository):
    table_name = "orderbook_aggregates"
    conflict_columns = "symbol,timestamp,version"

