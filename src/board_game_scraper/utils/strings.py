"""String utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def lower_or_none(value: Any) -> str | None:
    """Lower a string or return None."""

    return value.lower() if isinstance(value, str) else None
