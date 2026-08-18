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

    def for_prediction(self, prediction_id):
        rows = self.read(params={"prediction_id": f"eq.{prediction_id}", "limit": "1"})
        records = _records(rows)
        return records[0] if records else None

    def existing_prediction_ids(self, prediction_ids):
        clean_ids = [str(item) for item in prediction_ids if item]
        if not clean_ids:
            return set()
        rows = self.read(
            params={
                "select": "prediction_id",
                "prediction_id": f"in.({','.join(clean_ids)})",
                "limit": str(len(clean_ids)),
            }
        )
        return {row.get("prediction_id") for row in _records(rows) if row.get("prediction_id")}

    def safe_insert_outcome(self, prediction_id, outcome):
        payload = {"prediction_id": prediction_id, **outcome}
        return self.safe_insert(payload)
