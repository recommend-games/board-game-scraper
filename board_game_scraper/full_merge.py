# -*- coding: utf-8 -*-

"""Stop scraper, merge files, delete old files, and resume scraping."""

import argparse
import logging
import subprocess
import sys

from datetime import timedelta
from pathlib import Path
from time import sleep

from pytility import parse_bool, parse_float
from scrapy.utils.project import get_project_settings
from yaml import safe_load

from .merge import merge_files
from .utils import now

LOGGER = logging.getLogger(__name__)
SETTINGS = get_project_settings()
BASE_DIR = Path(SETTINGS.get("BASE_DIR") or ".").resolve()
FEEDS_DIR = BASE_DIR / "feeds"
DATA_DIR = (BASE_DIR / ".." / "board-game-data").resolve()


def merge_config(
    spider, item="GameItem", in_paths=None, out_path=None, full=False, **kwargs
):
    """Returns arguments for merging a given spider."""

    curr_date = now()
    curr_date_str = curr_date.strftime("%Y-%m-%dT%H-%M-%S")

    kwargs["in_paths"] = in_paths or FEEDS_DIR / spider / item / "*"
    kwargs.setdefault("keys", f"{spider}_id")
    kwargs.setdefault("key_types", "int" if spider in ("bgg", "luding") else "str")
    kwargs.setdefault("latest", "scraped_at")
    kwargs.setdefault("latest_types", "date")
    kwargs.setdefault("latest_min", curr_date - timedelta(days=90))
    kwargs.setdefault("concat_output", True)

    if parse_bool(full):
        kwargs["out_path"] = (
            out_path or FEEDS_DIR / spider / item / f"{curr_date_str}-merged.jl"
        )

    else:
        kwargs["out_path"] = out_path or DATA_DIR / "scraped" / f"{spider}_{item}.jl"
        kwargs.setdefault(
            "fieldnames_exclude",
            ("image_file", "rules_file", "published_at", "updated_at", "scraped_at"),
        )
        kwargs.setdefault("sort_keys", True)

    return kwargs


def merge_configs(spider, full=False):
    """Yields configs for all items in a given spider."""

    full = parse_bool(full)

    if spider == "bga":
        yield merge_config(spider="bga", item="GameItem", full=full)
        yield merge_config(
            spider="bga",
            item="RatingItem",
            full=full,
            keys=("bga_user_id", "bga_id"),
            fieldnames_exclude=("bgg_user_play_count",)
            if parse_bool(full)
            else ("bgg_user_play_count", "published_at", "updated_at", "scraped_at"),
        )
        return

    if spider == "bgg":
        yield merge_config(spider="bgg", item="GameItem", full=full)
        yield merge_config(
            spider="bgg",
            item="UserItem",
            full=full,
            keys="bgg_user_name",
            key_types="istr",
            fieldnames_exclude=None if full else ("published_at", "scraped_at"),
        )
        yield merge_config(
            spider="bgg",
            item="RatingItem",
            full=full,
            keys=("bgg_user_name", "bgg_id"),
            key_types=("istr", "int"),
            fieldnames_exclude=None if full else ("published_at", "scraped_at"),
        )
        return

    if spider == "bgg_hotness":
        yield merge_config(
            spider="bgg_hotness",
            item="GameItem",
            full=full,
            keys=("published_at", "bgg_id"),
            key_types=("date", "int"),
            latest_min=None,
            fieldnames=None
            if full
            else (
                "published_at",
                "rank",
                "bgg_id",
                "name",
                "year",
                "image_url",
            ),
            fieldnames_exclude=None,
            sort_keys=False,
            sort_fields=("published_at", "rank"),
        )
        return

    if spider == "bgg_rankings":
        yield merge_config(
            spider="bgg_rankings",
            item="GameItem",
            full=full,
            keys=("published_at", "bgg_id"),
            key_types=("date", "int"),
            latest_min=now() - timedelta(days=7),
            fieldnames=None
            if full
            else (
                "published_at",
                "bgg_id",
                "rank",
                "name",
                "year",
                "num_votes",
                "bayes_rating",
                "avg_rating",
            ),
            fieldnames_exclude=None,
            sort_keys=False,
            sort_fields=("published_at", "rank"),
        )
        return

    # TODO news merge config

    yield merge_config(spider=spider, item="GameItem", full=full)


def _parse_timeout(timeout):
    if timeout is None or timeout == "":
        return None

    timeout_float = parse_float(timeout)
    if timeout_float is not None:
        return timeout_float

    try:
        import pytimeparse
    except ImportError:
        return None

    return pytimeparse.parse(timeout)


def _docker_container(name):
    try:
        import docker
    except ImportError:
        LOGGER.warning("Docker library is not importable")
        return None

    try:
        client = docker.from_env()
        return client.containers.get(name)
    except docker.errors.NotFound:
        LOGGER.warning("Did not find container <%s>", name)

    return None


def _docker_start(name):
    LOGGER.info("Starting container <%s>", name)

    container = _docker_container(name)

    if container is not None:
        try:
            container.start()
            LOGGER.info("Started via Docker library call")
            return True
        except Exception:
            pass

    try:
        subprocess.run(["docker-compose", "start", name], check=True)
        LOGGER.info("Started via docker-compose CLI call")
        return True
    except Exception:
        pass

    LOGGER.warning("Unable to start container <%s>", name)
    return False


def _docker_stop(name, timeout=None):
    LOGGER.info("Stopping container <%s>", name)
    if timeout is not None:
        LOGGER.info("Allowing a timeout of %.1f seconds", timeout)

    container = _docker_container(name)

    if container is not None:
        try:
            if timeout is None:
                container.stop()
            else:
                container.stop(timeout=timeout)
            LOGGER.info("Stopped via Docker library call")
            return True
        except Exception:
            pass

    try:
        args = (
            ["docker-compose", "stop", name]
            if timeout is None
            else ["docker-compose", "stop", "--timeout", str(timeout), name]
        )
        subprocess.run(args=args, check=True)
        LOGGER.info("Stopped via docker-compose CLI call")
        return True
    except Exception:
        pass

    LOGGER.warning("Unable to stop container <%s>", name)
    return False


def _docker_compose(path, service):
    path = Path(path).resolve()
    LOGGER.info("Loading service <%s> from file <%s>", service, path)
    try:
        with open(path) as compose_file:
            config = safe_load(compose_file)
        return config["services"][service]
    except Exception:
        LOGGER.exception("Unable to load service <%s> from file <%s>", service, path)
    return {}


def _stop_merge_start(spider, compose_file, full=True, timeout=None, cool_down=None):
    scraper_name = spider.translate({ord("-"): "_"})
    docker_name = spider.translate({ord("_"): "-"})
    LOGGER.info(
        "Stopping, merging, and restarting spider <%s> / <%s>",
        scraper_name,
        docker_name,
    )

    docker_config = _docker_compose(path=compose_file, service=docker_name)
    container = docker_config.get("container_name")

    if not container:
        LOGGER.error("Unable to find container name for spider <%s>, aborting", spider)
        return False

    timeout = _parse_timeout(timeout)
    if timeout is None:
        timeout = _parse_timeout(docker_config.get("stop_grace_period"))

    # TODO add force option
    # if not _docker_stop(name=container, timeout=timeout):
    #     LOGGER.error("Unable to stop <%s>, aborting", container)
    #     return False

    _docker_stop(name=container, timeout=timeout)

    if cool_down:
        LOGGER.info("Cooling down for %d seconds...", cool_down)
        sleep(cool_down)

    for config in merge_configs(spider=scraper_name, full=full):
        LOGGER.info("Running merge with config %r", config)
        merge_files(**config)

    if cool_down:
        LOGGER.info("Cooling down for %d seconds...", cool_down)
        sleep(cool_down)

    return _docker_start(name=container)


def _parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("spiders", nargs="+", help="")
    parser.add_argument(
        "--compose-file", "-c", default=BASE_DIR / "docker-compose.yaml"
    )
    parser.add_argument("--timeout", "-t", help="")
    parser.add_argument("--cool-down", "-d", type=int, default=60, help="")
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

    for spider in args.spiders:
        _stop_merge_start(
            spider=spider,
            compose_file=args.compose_file,
            timeout=args.timeout,
            cool_down=args.cool_down,
        )


if __name__ == "__main__":
    main()
