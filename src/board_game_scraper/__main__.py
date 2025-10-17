from __future__ import annotations

import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import TYPE_CHECKING

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

if TYPE_CHECKING:
    from argparse import Namespace
    from collections.abc import Iterable
    from typing import Any

LOGGER = logging.getLogger(__name__)
PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_DIR.parent / "board-game-data" / "scraped"
RG_CONFIG_DIR = PROJECT_DIR.parent / "recommend-games-config"


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Scrape board game data from various sources")
    parser.add_argument(
        "spiders",
        nargs="*",
        help="Spider name(s) to crawl in this process (default: all)",
    )
    parser.add_argument(
        "--job-dir",
        "-j",
        type=Path,
        default=PROJECT_DIR / "jobs",
        help="Job directory",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all available spiders, then exit",
    )
    return parser.parse_args()


def spider_kwargs(spider: str, /, **kwargs: Any) -> dict[str, Any]:
    if spider == "bgg":
        kwargs.setdefault("scrape_ratings", True)
        kwargs.setdefault("scrape_collections", True)
        kwargs.setdefault("scrape_users", True)
        kwargs.setdefault("game_files", (DATA_DIR / "bgg_GameItem.jl",))
        kwargs.setdefault("user_files", (DATA_DIR / "bgg_UserItem.jl",))
        kwargs.setdefault("premium_users_dir", RG_CONFIG_DIR / "users" / "premium")

    elif spider == "bgg_hotness":
        kwargs.setdefault(
            "local_files_dir",
            PROJECT_DIR / "feeds" / "bgg_hotness" / "raw",
        )
        kwargs.setdefault("always_scrape_url", True)

    else:
        msg = f"Unknown spider: {spider}"
        raise ValueError(msg)

    return kwargs


def main() -> None:
    args = parse_args()

    settings = get_project_settings()
    settings.set(
        name="LOG_LEVEL",
        value="DEBUG" if args.verbose > 1 else "INFO" if args.verbose else "WARNING",
        priority="cmdline",
    )

    process = CrawlerProcess(settings=settings, install_root_handler=True)

    LOGGER.info("Scraping board game data with args: %s", args)

    if args.list:
        print("Available spiders:")
        for spider in process.spider_loader.list():
            print(f"\t- {spider}")
        return

    job_dir = Path(args.job_dir).resolve() if args.job_dir else None
    spiders: Iterable[str] = args.spiders or process.spider_loader.list()

    for spider in spiders:
        if spider not in process.spider_loader.list():
            LOGGER.error("Unknown spider: %s", spider)
            continue

        LOGGER.info("Crawling spider: %s", spider)
        kwargs = spider_kwargs(spider)
        LOGGER.info("Spider kwargs: %s", kwargs)

        crawler = process.create_crawler(spider)

        if job_dir:
            spider_job_dir = job_dir / spider
            LOGGER.info("Setting job directory: <%s>", spider_job_dir)
            crawler.settings.set("JOBDIR", spider_job_dir, priority="cmdline")

        process.crawl(crawler, **kwargs)

    process.start()


if __name__ == "__main__":
    main()
