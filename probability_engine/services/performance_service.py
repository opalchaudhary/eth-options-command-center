import math


def brier_score(predictions, outcomes):
    pairs = [(float(p), 1.0 if o else 0.0) for p, o in zip(predictions, outcomes)]
    if not pairs:
        return None
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs)


def log_loss(predictions, outcomes, eps=1e-9):
    pairs = [(min(1 - eps, max(eps, float(p))), 1.0 if o else 0.0) for p, o in zip(predictions, outcomes)]
    if not pairs:
        return None
    return -sum(o * math.log(p) + (1 - o) * math.log(1 - p) for p, o in pairs) / len(pairs)


def calibration_buckets(predictions, outcomes, bucket_size=0.1):
    buckets = []
    for start in [i * bucket_size for i in range(int(1 / bucket_size))]:
        end = start + bucket_size
        items = [(float(p), bool(o)) for p, o in zip(predictions, outcomes) if start <= float(p) < end or (end >= 1 and float(p) == 1)]
        if not items:
            buckets.append({"bucket": f"{int(start*100)}-{int(end*100)}", "sample_count": 0, "average_prediction": None, "actual_occurrence_rate": None})
            continue
        buckets.append({
            "bucket": f"{int(start*100)}-{int(end*100)}",
            "sample_count": len(items),
            "average_prediction": sum(p for p, _ in items) / len(items),
            "actual_occurrence_rate": sum(1 for _, o in items if o) / len(items),
        })
    return buckets


def calibration_error(predictions, outcomes):
    buckets = [bucket for bucket in calibration_buckets(predictions, outcomes) if bucket["sample_count"]]
    total = sum(bucket["sample_count"] for bucket in buckets)
    if not total:
        return None
    return sum(
        bucket["sample_count"] * abs(bucket["average_prediction"] - bucket["actual_occurrence_rate"])
        for bucket in buckets
    ) / total

