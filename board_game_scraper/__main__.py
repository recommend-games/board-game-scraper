# -*- coding: utf-8 -*-

"""Command line entry point."""

import logging

from pathlib import Path

from pytility import normalize_space
from scrapy.cmdline import execute
from scrapy.utils.job import job_dir
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import garbage_collect

LOGGER = logging.getLogger(__name__)


def _find_state(
    path_dir, state_file=".state", delete=("shutdown", "closespider_timeout")
):
    path_dir = Path(path_dir).resolve()
    delete = frozenset(arg_to_iter(delete))
    result = {}

    for sub_dir in sorted(path_dir.iterdir(), reverse=True):
        print(sub_dir)
        state_path = sub_dir / state_file
        print(state_path)

        if not sub_dir.is_dir() or not state_path.is_file():
            continue

        try:
            with state_path.open() as file_obj:
                state = normalize_space(next(file_obj, None))
        except Exception:
            LOGGER.exeception("Unable to read a state from <%s>", state_path)
            state = None

        if not state:
            continue

        result[sub_dir.name] = state

        if state in delete:
            LOGGER.info("Deleting <%s> with state <%s>", sub_dir, state)
            # TODO delete

    return result


if __name__ == "__main__":
    try:
        execute()
    finally:
        garbage_collect()
