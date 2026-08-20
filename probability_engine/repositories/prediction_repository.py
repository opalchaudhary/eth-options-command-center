from probability_engine.repositories.base_repository import SupabaseRepository


PREDICTION_EVALUATION_SELECT = (
    "id,created_at,snapshot_id,symbol,horizon,record_type,model_version,"
    "feature_version,regime_version,range_model_version,prediction_status,"
    "mean_reversion_probability,upside_breakout_probability,"
    "downside_breakdown_probability,range_continuation_probability,"
    "trend_continuation_probability,confidence,expected_price,median_price,"
    "expected_equilibrium,range_50_lower,range_50_upper,range_70_lower,"
    "range_70_upper,range_90_lower,range_90_upper,analogue_sample_size,"
    "metadata_json"
)


def _pending_outcome_select():
    return f"{PREDICTION_EVALUATION_SELECT},pending_outcome:probability_outcomes!left()"


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

    def mature_unevaluated(self, before_iso, limit=100, offset=0, label_version="label_v2"):
        params = {
            "select": _pending_outcome_select(),
            "created_at": f"lte.{before_iso}",
            "record_type": "eq.LIVE",
            "order": "created_at.asc",
            "limit": str(limit),
            "offset": str(offset),
            "pending_outcome.label_version": f"eq.{label_version}",
            "pending_outcome": "is.null",
        }
        return self.read(params=params)
