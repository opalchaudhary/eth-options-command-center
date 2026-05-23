import math
from datetime import date, datetime

import pandas as pd

try:
    import numpy as np
except Exception:
    np = None


def to_jsonable(value):
    if isinstance(value, pd.DataFrame):
        return [to_jsonable(row) for row in value.to_dict(orient="records")]

    if isinstance(value, pd.Series):
        return to_jsonable(value.to_dict())

    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]

    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()

    if not isinstance(value, (list, tuple, dict, set)):
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None

    if np is not None and isinstance(value, np.generic):
        return to_jsonable(value.item())

    return value
