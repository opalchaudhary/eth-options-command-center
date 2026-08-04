def clamp(value, low=0, high=100):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return low
    return max(low, min(high, value))


def confidence_label(score):
    score = clamp(score)
    if score >= 72:
        return "HIGH"
    if score >= 52:
        return "MEDIUM"
    return "LOW"


def weighted_score(components, weights):
    total_weight = 0
    total_score = 0
    details = {}
    for key, weight in weights.items():
        value = components.get(key)
        if value is None:
            details[key] = None
            continue
        total_weight += weight
        total_score += clamp(value) * weight
        details[key] = round(clamp(value), 2)
    if total_weight == 0:
        return 0, details
    return round(total_score / total_weight, 2), details


def data_quality_score(required_inputs, optional_inputs=None):
    required_inputs = required_inputs or {}
    optional_inputs = optional_inputs or {}
    required_count = len(required_inputs)
    optional_count = len(optional_inputs)
    required_available = sum(1 for value in required_inputs.values() if bool(value))
    optional_available = sum(1 for value in optional_inputs.values() if bool(value))
    if required_count == 0 and optional_count == 0:
        return 0
    required_score = (required_available / required_count) * 75 if required_count else 75
    optional_score = (optional_available / optional_count) * 25 if optional_count else 25
    return round(required_score + optional_score, 2)


def missing_inputs(inputs):
    return [key for key, value in (inputs or {}).items() if not bool(value)]


def status_from_score(score, recommended_threshold, watchlist_threshold):
    score = clamp(score)
    if score >= recommended_threshold:
        return "RECOMMENDED"
    if score >= watchlist_threshold:
        return "WATCHLIST"
    return "NOT_RECOMMENDED"
