from __future__ import annotations

from datetime import UTC, datetime, timezone
from datetime import date as date_cls
from typing import TYPE_CHECKING

from attrs.converters import to_bool

from board_game_scraper.utils.strings import normalize_space

if TYPE_CHECKING:
    from typing import Any


def parse_int(string: Any, base: int = 10) -> int | None:
    """Safely convert an object to int if possible, else return None."""

    if isinstance(string, int):
        return string

    try:
        return int(string, base=base)
    except (TypeError, ValueError):
        pass

    try:
        return int(string)
    except (TypeError, ValueError):
        pass

    return None


def parse_float(number: Any) -> float | None:
    """Safely convert an object to float if possible, else return None."""

    try:
        return float(number)
    except (TypeError, ValueError):
        pass

    return None


def _add_tz(
    date: datetime,
    tzinfo: timezone = UTC,
) -> datetime:
    return date if date.tzinfo else date.replace(tzinfo=tzinfo)


def parse_date(  # noqa: PLR0911
    date: Any,
    tzinfo: timezone = UTC,
    format_str: str | None = None,
) -> datetime | None:
    """
    Try to turn input into a datetime object.

    Unless `None`, result will always be timezone-aware, defaulting to UTC.
    """

    if not date:
        return None

    # already a datetime
    if isinstance(date, datetime):
        return _add_tz(date, tzinfo)

    # date without time
    if isinstance(date, date_cls):
        return datetime(date.year, date.month, date.day, tzinfo=tzinfo)

    # parse as epoch time
    timestamp = parse_float(date)
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp, tzinfo)

    if format_str:
        try:
            # parse as string in given format
            return _add_tz(datetime.strptime(date, format_str), tzinfo)  # noqa: DTZ007
        except (TypeError, ValueError):
            pass

    try:
        import dateutil.parser

        # parse as string
        return _add_tz(dateutil.parser.parse(date), tzinfo)
    except (ImportError, TypeError, ValueError):
        pass

    try:
        # parse as (year, month, day, hour, minute, second, microsecond, tzinfo)
        return _add_tz(datetime(*date), tzinfo)  # noqa: DTZ001
    except (TypeError, ValueError):
        pass

    try:
        # parse as time.struct_time
        return datetime(*date[:6], tzinfo=tzinfo)  # type: ignore[misc]
    except (TypeError, ValueError):
        pass

    return None


def parse_bool(value: Any) -> bool | None:
    if isinstance(value, (str, bytes)):
        value = normalize_space(value)
    if value in ("", None):
        return None
    try:
        return to_bool(value)
    except ValueError:
        return None
