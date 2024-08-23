# -*- coding: utf-8 -*-

"""News syncing, merging, splitting, and uploading."""

import argparse
import logging
import sys

from datetime import date, timedelta, timezone
from pathlib import Path
from shutil import rmtree
from time import sleep
from typing import TYPE_CHECKING, Optional, Union

from pytility import parse_date

from .merge import merge_files
from .split import split_files
from .utils import date_from_file, now

if TYPE_CHECKING:
    try:
        from git import Repo
    except ImportError:
        pass

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def _get_git_repo(path: Union[Path, str, None]) -> Optional["Repo"]:
    if not path:
        return None

    path = Path(path).resolve()

    try:
        from git import InvalidGitRepositoryError, NoSuchPathError, Repo
    except ImportError:
        LOGGER.exception("Unable to import Git library")
        return None

    try:
        LOGGER.info("Trying to find Git repo at <%s> or its parents", path)
        repo = Repo(path=path, search_parent_directories=True)
    except (InvalidGitRepositoryError, NoSuchPathError):
        LOGGER.exception("Path <%s> does not point to a valid Git repo", path)
        return None

    return repo


def update_news(
    *,
    path_feeds,
    path_merged,
    path_split,
    split_git_update=False,
    split_size=None,
    log_level=None,
    dry_run: bool = False,
):
    """News syncing, merging, splitting, and uploading."""

    dry_run_prefix = "[DRY RUN] " if dry_run else ""

    path_feeds = Path(path_feeds).resolve()
    path_merged = Path(path_merged).resolve()
    path_split = Path(path_split).resolve()

    LOGGER.info(
        "%sMerge from <%s> into <%s> and split into <%s>",
        dry_run_prefix,
        path_feeds,
        path_merged,
        path_split,
    )

    if split_git_update:
        repo = _get_git_repo(path_split.parent)
        if repo is None or not repo.working_dir:
            repo = None
            split_git_update = False
            path_git = None
            git_rel_path = None
            LOGGER.error(
                "%sUnable to update Git repo <%s>",
                dry_run_prefix,
                path_split.parent,
            )

        else:
            path_git = Path(repo.working_dir).resolve()
            git_rel_path = path_split.parent.relative_to(path_git)
            LOGGER.info("%sUpdate Git repo <%s>", dry_run_prefix, path_git)
            if not dry_run:
                LOGGER.info("Pulling latest commits from remote <%s>", repo.remotes[0])
                try:
                    repo.remotes[0].pull(ff_only=True)
                except Exception:
                    LOGGER.exception(
                        "Unable to pull from remote <%s> to repo <%s>",
                        repo.remotes[0],
                        path_git,
                    )

    else:
        repo = None

    LOGGER.info("%sDeleting existing dir <%s>", dry_run_prefix, path_split.parent)
    if not dry_run:
        if repo is None:
            rmtree(path_split.parent, ignore_errors=True)
        else:
            try:
                deleted_files = repo.index.remove(
                    items=[str(git_rel_path)],
                    working_tree=True,
                    r=True,
                )
                LOGGER.info(
                    "Deleted %d files from <%s>",
                    len(deleted_files),
                    path_split.parent,
                )
            except Exception:
                LOGGER.exception(
                    "Unable to remove path <%s> from Git",
                    path_split.parent,
                )

        path_feeds.mkdir(parents=True, exist_ok=True)
        path_merged.parent.mkdir(parents=True, exist_ok=True)
        path_split.parent.mkdir(parents=True, exist_ok=True)

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
        dry_run=dry_run,
    )

    split_files(
        path_in=path_merged,
        path_out=path_split,
        size=split_size,
        exclude_empty=True,
        indent=4,
        dry_run=dry_run,
    )

    if repo is not None and git_rel_path:
        message = f"Automatic commit by <{__name__}> {date.today().isoformat()}"
        LOGGER.info(
            "%sCommitting changes to repo <%s> with message <%s>",
            dry_run_prefix,
            path_git,
            message,
        )
        if not dry_run:
            try:
                repo.index.add(items=[str(git_rel_path)])
                repo.index.commit(message=message, skip_hooks=True)
            except Exception:
                LOGGER.exception(
                    "There was a problem commit to Git repo <%s>",
                    path_git,
                )

        for remote in repo.remotes:
            LOGGER.info("%sPushing changes to remote <%s>", dry_run_prefix, remote)
            if not dry_run:
                try:
                    remote.push()
                except Exception:
                    LOGGER.exception(
                        "There was a problem pushing to remote <%s>",
                        remote,
                    )

    LOGGER.info("%sDone updating news.", dry_run_prefix)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="News syncing, merging, splitting, and uploading.",
    )
    parser.add_argument(
        "--feeds",
        "-f",
        default=BASE_DIR / "feeds" / "news",
        help="Scraped items",
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
        "--git",
        "-g",
        action="store_true",
        help="Update the Git repo of the split files",
    )
    parser.add_argument(
        "--dont-run-before",
        "-d",
        help="Either a date or a file with date information",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=10 * 60,  # 10 minutes
        help="number of seconds to wait before next run",
    )
    parser.add_argument("--dry-run", "-n", action="store_true", help="dry run")
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
        args.dont_run_before,
        tzinfo=timezone.utc,
    ) or date_from_file(
        args.dont_run_before,
        tzinfo=timezone.utc,
    )

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
        with open(args.dont_run_before, "w", encoding="utf-8") as file_obj:
            file_obj.write(dont_run_before.isoformat())

    update_news(
        path_feeds=args.feeds,
        path_merged=args.merged,
        path_split=args.split,
        split_size=args.split_size,
        split_git_update=args.git,
        log_level="DEBUG"
        if args.verbose > 1
        else "INFO"
        if args.verbose > 0
        else "WARN",
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
