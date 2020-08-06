# -*- coding: utf-8 -*-

"""Stop scraper, merge files, delete old files, and resume scraping."""

import logging
import subprocess

from pathlib import Path
from time import sleep

from yaml import safe_load

LOGGER = logging.getLogger(__name__)


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


def _docker_stop(name, timeout=120):
    LOGGER.info("Stopping container <%s>", name)

    container = _docker_container(name)

    if container is not None:
        try:
            container.stop(timeout=timeout)
            LOGGER.info("Stopped via Docker library call")
            return True
        except Exception:
            pass

    try:
        subprocess.run(
            ["docker-compose", "stop", "--timeout", str(timeout), name], check=True
        )
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


def _stop_merge_start(spider, cool_down=None):
    config = _docker_compose(path=Path() / "docker-compose.yaml", service=spider)
    container = config.get("container_name")

    _docker_stop(name=container)

    if cool_down:
        LOGGER.info("Cooling down for %d seconds...", cool_down)
        sleep(cool_down)

    # TODO merge

    if cool_down:
        LOGGER.info("Cooling down for %d seconds...", cool_down)
        sleep(cool_down)

    _docker_start(name=container)


def main():
    """Command line entry point."""


if __name__ == "__main__":
    main()
