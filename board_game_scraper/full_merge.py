# -*- coding: utf-8 -*-

"""Stop scraper, merge files, delete old files, and resume scraping."""

import argparse
import logging
import subprocess
import sys

from pathlib import Path
from time import sleep

from pytility import parse_float
from scrapy.utils.project import get_project_settings
from yaml import safe_load

LOGGER = logging.getLogger(__name__)
SETTINGS = get_project_settings()
BASE_DIR = Path(SETTINGS.get("BASE_DIR") or ".").resolve()


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


def _stop_merge_start(spider, compose_file, timeout=None, cool_down=None):
    LOGGER.info("Stopping, merging, and restarting spider <%s>", spider)

    config = _docker_compose(path=compose_file, service=spider)
    container = config.get("container_name")

    timeout = _parse_timeout(timeout)
    if timeout is None:
        timeout = _parse_timeout(config.get("stop_grace_period"))

    if not _docker_stop(name=container, timeout=timeout):
        LOGGER.error("Unable to stop <%s>, aborting", container)
        return False

    if cool_down:
        LOGGER.info("Cooling down for %d seconds...", cool_down)
        sleep(cool_down)

    # TODO merge

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
