"""String utilities."""

from __future__ import annotations

import string as string_lib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

PRINTABLE_SET = frozenset(string_lib.printable)
NON_PRINTABLE_SET = frozenset(chr(i) for i in range(128)) - PRINTABLE_SET
NON_PRINTABLE_TANSLATE = {ord(character): None for character in NON_PRINTABLE_SET}


def to_str(string: Any, encoding: str = "utf-8") -> str | None:
    """Safely returns either string or None."""

    string = (
        string
        if isinstance(string, str)
        else string.decode(encoding)
        if isinstance(string, bytes)
        else None
    )

    if string is None:
        return None
    assert isinstance(string, str)
    return string.translate(NON_PRINTABLE_TANSLATE)


def normalize_space(item: Any, *, preserve_newline: bool = False) -> str:
    """Normalize space in a string."""

    item = to_str(item)

    if not item:
        return ""

    assert isinstance(item, str)

    if preserve_newline:
        return "\n".join(normalize_space(line) for line in item.splitlines()).strip()

    return " ".join(item.split())


def lower_or_none(value: Any) -> str | None:
    """Lower a string or return None."""

    return value.lower() if isinstance(value, str) else None
