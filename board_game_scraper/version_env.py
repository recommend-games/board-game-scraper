# -*- coding: utf-8 -*-

"""TODO."""

import argparse
import logging
import sys

from .__version__ import __version__

LOGGER = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser(description="TODO.")
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

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

    with open("VERSION", "w") as f:
        f.write(__version__)


if __name__ == "__main__":
    main()
