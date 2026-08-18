from probability_engine.repositories.base_repository import SupabaseRepository


class OutcomeRepository(SupabaseRepository):
    table_name = "probability_outcomes"

    def for_prediction(self, prediction_id):
        rows = self.read(params={"prediction_id": f"eq.{prediction_id}", "limit": "1"})
        if rows is None:
            return None
        if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
            if rows.empty:
                return None
            return rows.to_dict("records")[0]
        if isinstance(rows, dict):
            return rows
        return rows[0] if rows else None
