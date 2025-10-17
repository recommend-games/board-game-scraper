from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TypeVar

    Typed = TypeVar("Typed")


def clear_iterable[Typed](items: Iterable[Typed | None]) -> Iterable[Typed]:
    """Return unique items in order of first ocurrence."""
    return OrderedDict.fromkeys(filter(None, items)).keys()


def clear_list[Typed](items: Iterable[Typed | None]) -> list[Typed]:
    """Return unique items in order of first ocurrence."""
    return list(clear_iterable(items))
