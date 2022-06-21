# -*- coding: utf-8 -*-

"""Zip ranking files."""

import argparse
import logging
import sys
import zipfile

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

from pytility import parse_date

from .utils import now

BASE_DIR = Path(__file__).resolve().parent.parent
DUMMY_DATE = datetime(1970, 1, 1)
DATE_FORMAT = "%Y-%m-%dT%H-%M-%S"
LOGGER = logging.getLogger(__name__)


def file_date(
    path: Union[Path, str],
    *,
    format_str: Optional[str] = None,
) -> Optional[datetime]:
    """TODO."""

    path = Path(path)

    if format_str:
        # TODO not the most efficient way to do this
        dummy_date = DUMMY_DATE.strftime(format_str)

        date_from_name = parse_date(
            date=path.name[: len(dummy_date)],
            tzinfo=timezone.utc,
            format_str=format_str,
        )

        if date_from_name is not None:
            return date_from_name

    file_stats = path.stat()
    return (
        parse_date(date=file_stats.st_ctime, tzinfo=timezone.utc)
        or parse_date(date=file_stats.st_mtime, tzinfo=timezone.utc)
        or parse_date(date=file_stats.st_atime, tzinfo=timezone.utc)
    )


def zip_ranking_files(
    *,
    rankings_dir: Union[Path, str],
    rankings_file_glob: str,
    output_file: Union[Path, str],
    dry_run: bool = False,
) -> None:
    """TODO."""

    rankings_dir = Path(rankings_dir).resolve()
    output_file = Path(output_file).resolve()

    LOGGER.info(
        "Compressing rankings file in <%s> matching glob <%s>, storing the output to <%s>",
        rankings_dir,
        rankings_file_glob,
        output_file,
    )

    if dry_run:
        for rankings_file in rankings_dir.glob(rankings_file_glob):
            LOGGER.info("[Dry run] Compressing file <%s>…", rankings_file)
        LOGGER.info("Done.")
        return

    with zipfile.ZipFile(
        file=output_file,
        mode="x",
        compression=zipfile.ZIP_DEFLATED,
    ) as zip_file:
        for rankings_file in rankings_dir.glob(rankings_file_glob):
            LOGGER.info("Compressing file <%s>…", rankings_file)
            zip_file.write(
                filename=rankings_file,
                arcname=rankings_file.relative_to(rankings_dir),
            )

    LOGGER.info("Done.")


def _parse_args():
    parser = argparse.ArgumentParser(description="TODO")
    parser.add_argument("--out-dir", "-d", default=BASE_DIR / "backup", help="TODO")
    parser.add_argument(
        "--out-file",
        "-f",
        default="bgg-rankings-%Y-%m-%d.zip",
        help="TODO",
    )
    parser.add_argument("--in-dir", "-i", default=BASE_DIR / "feeds", help="TODO")
    parser.add_argument(
        "--glob",
        "-g",
        default="bgg_rankings*/GameItem/*.jl",
        help="TODO",
    )
    parser.add_argument(
        "--delete-files-older-than-days",
        "-D",
        type=int,
        help="TODO",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="TODO",
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
    """Command line entry point."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    out_file = now().strftime(args.out_file)
    out_dir = Path(args.out_dir).resolve()
    out_path = out_dir / out_file

    if not args.dry_run:
        if out_path.exists():
            LOGGER.info("Output file <%s> already exists, aborting…", out_path)
            sys.exit(1)
        out_dir.mkdir(parents=True, exist_ok=True)

    zip_ranking_files(
        rankings_dir=args.in_dir,
        rankings_file_glob=args.glob,
        output_file=out_path,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
