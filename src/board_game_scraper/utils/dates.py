from datetime import UTC, datetime, timezone


def now(tz: timezone = UTC) -> datetime:
    """Return the current time with the given timezone."""
    return datetime.now(tz)
