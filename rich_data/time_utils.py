from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc)


def parse_delta_timestamp(value):
    if value in [None, ""]:
        return None

    try:
        numeric = int(value)
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None

    if numeric > 10**15:
        seconds = numeric / 1_000_000
    elif numeric > 10**12:
        seconds = numeric / 1_000
    else:
        seconds = numeric

    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def floor_time(dt, seconds):
    if dt is None:
        dt = utc_now()
    dt = dt.astimezone(timezone.utc)
    epoch = int(dt.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=timezone.utc)

