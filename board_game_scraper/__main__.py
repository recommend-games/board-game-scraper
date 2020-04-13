# -*- coding: utf-8 -*-

"""Command line entry point."""

import argparse
import logging
import sys

from pathlib import Path

from pytility import normalize_space
from scrapy.cmdline import execute
from scrapy.utils.job import job_dir as job_dir_from_settings
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.project import get_project_settings
from scrapy.utils.python import garbage_collect

from .utils import now

LOGGER = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%dT%H-%M-%S"
RESUMABLE_STATES = frozenset(("shutdown", "closespider_timeout"))


def _find_states(
    path_dir, state_file=".state", delete="finished", delete_non_state=False,
):
    path_dir = Path(path_dir).resolve()
    delete = frozenset(arg_to_iter(delete))
    result = {}

    if not path_dir.is_dir():
        LOGGER.warning("<%s> is not an existing dir", path_dir)
        return result

    for sub_dir in path_dir.iterdir():
        state_path = sub_dir / state_file

        if not sub_dir.is_dir() or not state_path.is_file():
            continue

        try:
            with state_path.open() as file_obj:
                state = normalize_space(next(file_obj, None))
        except Exception:
            LOGGER.exeception("Unable to read a state from <%s>", state_path)
            state = None

        if state in delete or (delete_non_state and not state):
            LOGGER.info("Deleting <%s> with state <%s>", sub_dir, state)
            # TODO delete
            continue

        if state:
            result[sub_dir.name] = state

    return result


def _parse_args():
    parser = argparse.ArgumentParser(description="TODO")
    parser.add_argument("spider", help="TODO")
    parser.add_argument("--job-dir", "-j", help="TODO")
    parser.add_argument("--feeds-dir", "-f", help="TODO")
    parser.add_argument("--dont-run-before", "-d", help="TODO")
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    return parser.parse_args()


def main():
    """Command line entry point."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    settings = get_project_settings()

    base_dir = Path(settings["BASE_DIR"]).resolve()
    feeds_dir = Path(args.feeds_dir) if args.feeds_dir else base_dir / "feeds"
    feeds_dir = feeds_dir.resolve()
    out_file = feeds_dir / "%(name)s" / "%(class)s" / "%(time)s.jl"

    from_settings = job_dir_from_settings(settings)
    job_dir = (
        Path(args.job_dir)
        if args.job_dir
        else Path(from_settings)
        if from_settings
        else base_dir / "jobs" / args.spider
    )
    job_dir = job_dir.resolve()

    # TODO sleep if don't run before

    states = _find_states(job_dir)

    running = sorted(sub_dir for sub_dir, state in states.items() if state == "running")

    if len(running) > 1:
        LOGGER.warning(
            "Found %d running jobs %s, please check and fix!", len(running), running
        )
        return

    if running:
        LOGGER.info("Found a running job <%s>, skipping...", running[0])
        return

    resumable = sorted(
        sub_dir for sub_dir, state in states.items() if state in RESUMABLE_STATES
    )

    if len(resumable) > 1:
        LOGGER.warning(
            "Found %d resumable jobs %s, please check and fix!",
            len(resumable),
            resumable,
        )
        return

    if resumable:
        LOGGER.info("Resuming previous job <%s>", resumable[0])

    job_tag = resumable[0] if resumable else now().strftime(DATE_FORMAT)
    curr_job = job_dir / job_tag

    command = [
        "scrapy",
        "crawl",
        args.spider,
        "--output",
        str(out_file),
        "--set",
        f"JOBDIR={curr_job}",
    ]

    if args.dont_run_before:
        command += ["--set", f"DONT_RUN_BEFORE_FILE=${args.dont_run_before}"]

    try:
        execute(argv=command)
    finally:
        garbage_collect()


if __name__ == "__main__":
    main()
