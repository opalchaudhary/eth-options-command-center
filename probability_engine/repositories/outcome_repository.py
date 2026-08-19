from probability_engine.repositories.base_repository import SupabaseRepository


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        if rows.empty:
            return []
        return rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


class OutcomeRepository(SupabaseRepository):
    table_name = "probability_outcomes"

    def for_prediction(self, prediction_id, label_version=None):
        params = {"prediction_id": f"eq.{prediction_id}", "limit": "1"}
        if label_version:
            params["label_version"] = f"eq.{label_version}"
        else:
            params["order"] = "label_version.desc,evaluated_at.desc"
        rows = self.read(params=params)
        records = _records(rows)
        return records[0] if records else None

    def existing_prediction_ids(self, prediction_ids, label_version="label_v2"):
        clean_ids = [str(item) for item in prediction_ids if item]
        if not clean_ids:
            return set()
        params = {
            "select": "prediction_id",
            "prediction_id": f"in.({','.join(clean_ids)})",
            "limit": str(len(clean_ids)),
        }
        if label_version:
            params["label_version"] = f"eq.{label_version}"
        rows = self.read(
            params=params
        )
        return {row.get("prediction_id") for row in _records(rows) if row.get("prediction_id")}

    def safe_insert_outcome(self, prediction_id, outcome, label_version="label_v2"):
        payload = {"prediction_id": prediction_id, "label_version": label_version, **outcome}
        return self.safe_insert(payload)
