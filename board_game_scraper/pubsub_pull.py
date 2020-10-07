# -*- coding: utf-8 -*-

"""TODO."""

import argparse
import logging
import sys

from pathlib import Path

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def _parse_args():
    parser = argparse.ArgumentParser(description="TODO.")

    return parser.parse_args()


def main():
    """TODO."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)


if __name__ == "__main__":
    main()
