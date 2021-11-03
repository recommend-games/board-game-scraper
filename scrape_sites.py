#!/usr/bin/env python3

"""Run subprocesses."""

import logging
import os
import signal
import sys

from subprocess import Popen

from pytility import parse_int

LOGGER = logging.getLogger(__name__)

MAX_SLEEP_PROCESS = parse_int(os.getenv("MAX_SLEEP_PROCESS")) or 0
CLOSESPIDER_TIMEOUT = 36_000  # 10h
DONT_RUN_BEFORE_SEC = 21_600  # 6h

CMDS = [
    # [
    #     "python",
    #     "-m",
    #     "board_game_scraper",
    #     "bga",
    #     "--max-sleep-process",
    #     f"{MAX_SLEEP_PROCESS}",
    #     "--interrupted-exit-code",
    #     "1",
    #     "--set",
    #     f"CLOSESPIDER_TIMEOUT={CLOSESPIDER_TIMEOUT}",
    #     "--set",
    #     f"DONT_RUN_BEFORE_SEC={DONT_RUN_BEFORE_SEC}",
    # ],
    [
        "python",
        "-m",
        "board_game_scraper",
        "dbpedia",
        "--max-sleep-process",
        f"{MAX_SLEEP_PROCESS}",
        "--interrupted-exit-code",
        "1",
        "--set",
        f"CLOSESPIDER_TIMEOUT={CLOSESPIDER_TIMEOUT}",
        "--set",
        f"DONT_RUN_BEFORE_SEC={DONT_RUN_BEFORE_SEC}",
    ],
    [
        "python",
        "-m",
        "board_game_scraper",
        "luding",
        "--max-sleep-process",
        f"{MAX_SLEEP_PROCESS}",
        "--interrupted-exit-code",
        "1",
        "--set",
        f"CLOSESPIDER_TIMEOUT={CLOSESPIDER_TIMEOUT}",
        "--set",
        f"DONT_RUN_BEFORE_SEC={DONT_RUN_BEFORE_SEC}",
    ],
    [
        "python",
        "-m",
        "board_game_scraper",
        "spielen",
        "--max-sleep-process",
        f"{MAX_SLEEP_PROCESS}",
        "--interrupted-exit-code",
        "1",
        "--set",
        f"CLOSESPIDER_TIMEOUT={CLOSESPIDER_TIMEOUT}",
        "--set",
        f"DONT_RUN_BEFORE_SEC={DONT_RUN_BEFORE_SEC}",
    ],
    [
        "python",
        "-m",
        "board_game_scraper",
        "wikidata",
        "--max-sleep-process",
        f"{MAX_SLEEP_PROCESS}",
        "--interrupted-exit-code",
        "1",
        "--set",
        f"CLOSESPIDER_TIMEOUT={CLOSESPIDER_TIMEOUT}",
        "--set",
        f"DONT_RUN_BEFORE_SEC={DONT_RUN_BEFORE_SEC}",
    ],
]


def run_cmds(cmds, fwd_signals=(signal.SIGINT, signal.SIGTERM, signal.SIGUSR1)):
    """Run a subprocess."""

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(fwd_signals)

    for cmd in cmds:
        LOGGER.info(cmd)

        with Popen(cmd) as proc:
            proc.interrupted = True

            def _forward_signal(signum, _, *, proc=proc):
                proc.interrupted = True
                LOGGER.info(
                    "Received signal <%d>, let the child process handle it",
                    signum,
                )
                LOGGER.warning(
                    "Scraping will stop after the current process has finished"
                )

            for sig in fwd_signals:
                signal.signal(sig, _forward_signal)

            LOGGER.info("Started process <%d>", proc.pid)
            proc.communicate()

        LOGGER.info("Process finished with exitcode <%d>", proc.returncode)

        if proc.returncode != 0 or proc.interrupted:
            LOGGER.warning("Process got interrupted, aborting!")
            break


if __name__ == "__main__":
    run_cmds(cmds=CMDS)
