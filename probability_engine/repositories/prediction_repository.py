from probability_engine.repositories.base_repository import SupabaseRepository


class PredictionRepository(SupabaseRepository):
    table_name = "probability_predictions"

    def safe_insert(self, model_or_payload):
        payload = model_or_payload.to_record() if hasattr(model_or_payload, "to_record") else model_or_payload
        if payload.get("record_type") == "LIVE" and not payload.get("snapshot_id"):
            return False
        return super().safe_insert(payload)

    def latest(self, symbol="ETHUSD", horizon=None, limit=25):
        params = {"symbol": f"eq.{symbol}", "order": "created_at.desc", "limit": str(limit)}
        if horizon:
            params["horizon"] = f"eq.{horizon.upper()}"
        return self.read(params=params)

    def mature_unevaluated(self, before_iso, limit=100):
        params = {
            "created_at": f"lte.{before_iso}",
            "record_type": "eq.LIVE",
            "order": "created_at.asc",
            "limit": str(limit),
        }
        return self.read(params=params)
