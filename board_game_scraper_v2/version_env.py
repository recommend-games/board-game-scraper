# -*- coding: utf-8 -*-

"""Read library __version__ and write to .env."""

import argparse
import logging
import sys

from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryFile

from .__version__ import __version__

LOGGER = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Read library __version__ and write to .env."
    )
    parser.add_argument(
        "--target",
        "-t",
        default=".env",
        help="target file",
    )
    parser.add_argument(
        "--variable",
        "-V",
        default="LIBRARY_VERSION",
        help="variable name",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    return parser.parse_args()


def main():
    """Read library __version__ and write to .env."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    target = Path(args.target).resolve()

    LOGGER.info("Trying to write version %s to file <%s>...", __version__, target)

    if not target.exists():
        LOGGER.error("Target file <%s> does not exist, aborting", target)
        sys.exit(1)

    LOGGER.info("Looking for environment variable <%s>", args.variable)

    with TemporaryFile("w+") as temp:
        with target.open() as in_file:
            for line in in_file:
                if line.startswith(f"{args.variable}="):
                    temp.write(f"{args.variable}={__version__}\n")
                else:
                    temp.write(line)

        temp.seek(0)

        with target.open("w") as out_file:
            copyfileobj(temp, out_file)

    LOGGER.info("Done.")


if __name__ == "__main__":
    main()
