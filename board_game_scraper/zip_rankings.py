# -*- coding: utf-8 -*-

"""Zip ranking files."""

import argparse
import logging
import sys
import zipfile

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

from pytility import parse_date

from .utils import now

BASE_DIR = Path(__file__).resolve().parent.parent
DUMMY_DATE = datetime(1970, 1, 1)
DATE_FORMAT = "%Y-%m-%dT%H-%M-%S"
LOGGER = logging.getLogger(__name__)


def zip_ranking_files(
    *,
    rankings_dir: Union[Path, str],
    rankings_file_glob: str,
    output_file: Union[Path, str],
    dry_run: bool = False,
) -> None:
    """Zip the ranking files in a given directory."""

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


def file_date(
    path: Union[Path, str],
    *,
    format_str: Optional[str] = None,
) -> Optional[datetime]:
    """Get the date from a given file, either from its name or the access stats."""

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


def delete_older_files(
    *,
    dir_path: Union[Path, str],
    file_glob: str,
    older_than: timedelta,
    dry_run: bool = False,
):
    """Delete files in a directory older than a given age."""
    dir_path = Path(dir_path).resolve()
    cutoff_date = now() - older_than

    LOGGER.info(
        "Deleting file in <%s> matching glob <%s> older than <%s>",
        dir_path,
        file_glob,
        cutoff_date,
    )

    for file_path in dir_path.glob(file_glob):
        if file_date(path=file_path, format_str=DATE_FORMAT) < cutoff_date:
            if dry_run:
                LOGGER.info("[Dry run] Deleting <%s>…", file_path)
            else:
                LOGGER.info("Deleting <%s>…", file_path)
                file_path.unlink()

    LOGGER.info("Done.")


def _parse_args():
    parser = argparse.ArgumentParser(description="Zip ranking files.")
    parser.add_argument(
        "--out-dir",
        "-d",
        default=BASE_DIR / "backup",
        help="Output directory",
    )
    parser.add_argument(
        "--out-file",
        "-f",
        default="bgg-rankings-%Y-%m-%d.zip",
        help="Output file name",
    )
    parser.add_argument(
        "--in-dir",
        "-i",
        default=BASE_DIR / "feeds",
        help="Input directory",
    )
    parser.add_argument(
        "--glob",
        "-g",
        default="bgg_rankings*/GameItem/*.jl",
        help="File glob for input files",
    )
    parser.add_argument(
        "--delete-files-older-than-days",
        "-D",
        type=int,
        help="If given, files older than this will be deleted after zipping",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Dry run, don't write or delete anything",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Log level (repeat for more verbosity)",
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

    if args.delete_files_older_than_days:
        delete_older_files(
            dir_path=args.in_dir,
            file_glob=args.glob,
            older_than=timedelta(days=args.delete_files_older_than_days),
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
