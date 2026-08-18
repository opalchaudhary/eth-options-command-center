import logging

import database_reader
import storage


logger = logging.getLogger(__name__)


class SupabaseRepository:
    table_name: str

    def insert(self, payload):
        return storage.post_to_supabase(self.table_name, payload)

    def insert_returning(self, payload):
        return storage.post_to_supabase_returning(self.table_name, payload)

    def read(self, params=None):
        return database_reader.read_supabase_table(self.table_name, params=params or {})

    def safe_insert(self, model_or_payload):
        payload = model_or_payload.to_record() if hasattr(model_or_payload, "to_record") else model_or_payload
        try:
            ok = self.insert(payload)
            logger.info("probability.repository.insert", extra={"table": self.table_name, "ok": bool(ok)})
            return bool(ok)
        except Exception:
            logger.exception("Probability repository insert failed for %s", self.table_name)
            return False

    def safe_insert_returning(self, model_or_payload):
        payload = model_or_payload.to_record() if hasattr(model_or_payload, "to_record") else model_or_payload
        try:
            rows = self.insert_returning(payload)
            logger.info("probability.repository.insert", extra={"table": self.table_name, "ok": bool(rows)})
            return rows[0] if isinstance(rows, list) and rows else None
        except Exception:
            logger.exception("Probability repository insert failed for %s", self.table_name)
            return None
