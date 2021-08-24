# -*- coding: utf-8 -*-

"""News syncing, merging, splitting, and uploading."""

import argparse
import logging
import os.path
import sys

from datetime import timedelta, timezone
from pathlib import Path
from shutil import rmtree
from subprocess import run
from time import sleep

from pytility import parse_date

from .merge import merge_files
from .split import split_files
from .utils import date_from_file, now

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def update_news(
    s3_src, path_feeds, path_merged, path_split, s3_dst, split_size=None, log_level=None
):
    """News syncing, merging, splitting, and uploading."""

    path_feeds = Path(path_feeds).resolve()
    path_merged = Path(path_merged).resolve()
    path_split = Path(path_split).resolve()

    LOGGER.info(
        "Sync from <%s>, merge from <%s> into <%s>, split into <%s>, upload to <%s>",
        s3_src,
        path_feeds,
        path_merged,
        path_split,
        s3_dst,
    )

    LOGGER.info("Deleting existing dir <%s>", path_split.parent)
    rmtree(path_split.parent, ignore_errors=True)

    path_feeds.mkdir(parents=True, exist_ok=True)
    path_merged.parent.mkdir(parents=True, exist_ok=True)
    path_split.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("S3 sync from <%s> to <%s>", s3_src, path_feeds)
    run(["aws", "s3", "sync", s3_src, os.path.join(path_feeds, "")], check=True)

    merge_files(
        in_paths=path_feeds.rglob("*.jl"),
        out_path=path_merged,
        keys="article_id",
        key_types="string",
        latest=("published_at", "scraped_at"),
        latest_types=("date", "date"),
        latest_required=True,
        sort_latest=True,
        sort_descending=True,
        concat_output=True,
        log_level=log_level,
    )

    split_files(
        path_in=path_merged, path_out=path_split, size=split_size, exclude_empty=True
    )

    LOGGER.info("S3 sync from <%s> to <%s>", path_split.parent, s3_dst)
    run(
        [
            "aws",
            "s3",
            "sync",
            "--acl",
            "public-read",
            "--exclude",
            ".gitignore",
            "--exclude",
            ".DS_Store",
            "--exclude",
            ".bucket",
            "--size-only",
            "--delete",
            os.path.join(path_split.parent, ""),
            s3_dst,
        ],
        check=True,
    )

    LOGGER.info("Done updating news.")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="News syncing, merging, splitting, and uploading."
    )
    parser.add_argument(
        "--src-bucket",
        "-b",
        default="scrape.news.recommend.games",
        help="S3 bucket with scraped data",
    )
    parser.add_argument(
        "--dst-bucket",
        "-B",
        default="news.recommend.games",
        help="S3 bucket to upload to",
    )
    parser.add_argument(
        "--feeds", "-f", default=BASE_DIR / "feeds" / "news", help="Scraped items"
    )
    parser.add_argument(
        "--merged",
        "-m",
        default=BASE_DIR / "feeds" / "news_merged.jl",
        help="Merged file",
    )
    parser.add_argument(
        "--split",
        "-s",
        default=BASE_DIR / "feeds" / "news_hosting" / "news_{number:05d}.json",
        help="Split file template",
    )
    parser.add_argument(
        "--split-size",
        "-S",
        type=int,
        default=25,
        help="number of items in each result file",
    )
    parser.add_argument(
        "--dont-run-before", "-d", help="Either a date or a file with date information"
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=10 * 60,  # 10 minutes
        help="number of seconds to wait before next run",
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

    dont_run_before = parse_date(
        args.dont_run_before, tzinfo=timezone.utc
    ) or date_from_file(args.dont_run_before, tzinfo=timezone.utc)

    if dont_run_before:
        LOGGER.info("Don't run before %s", dont_run_before.isoformat())
        sleep_seconds = dont_run_before.timestamp() - now().timestamp()
        if sleep_seconds > 0:
            LOGGER.info("Going to sleep for %.1f seconds", sleep_seconds)
            sleep(sleep_seconds)

    if args.interval and args.dont_run_before and not parse_date(args.dont_run_before):
        dont_run_before = now() + timedelta(seconds=args.interval)
        LOGGER.info(
            "Don't run next time before %s, writing tag to <%s>",
            dont_run_before.isoformat(),
            args.dont_run_before,
        )
        with open(args.dont_run_before, "w") as file_obj:
            file_obj.write(dont_run_before.isoformat())

    update_news(
        s3_src=f"s3://{args.src_bucket}/",
        path_feeds=args.feeds,
        path_merged=args.merged,
        path_split=args.split,
        s3_dst=f"s3://{args.dst_bucket}/",
        split_size=args.split_size,
        log_level="DEBUG"
        if args.verbose > 1
        else "INFO"
        if args.verbose > 0
        else "WARN",
    )


if __name__ == "__main__":
    main()
