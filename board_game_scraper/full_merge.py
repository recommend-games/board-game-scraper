# -*- coding: utf-8 -*-

"""Stop scraper, merge files, delete old files, and resume scraping."""

import logging
import subprocess

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


def main():
    """Command line entry point."""


if __name__ == "__main__":
    main()
