import os
from pathlib import Path

BOT_NAME = "board-game-scraper"

SPIDER_MODULES = ["board_game_scraper.spiders"]
NEWSPIDER_MODULE = "board_game_scraper.spiders"

LOG_LEVEL = os.getenv("LOG_LEVEL") or "INFO"
LOG_FORMATTER = "scrapy_extensions.QuietLogFormatter"
LOG_SCRAPED_ITEMS = os.getenv("LOG_SCRAPED_ITEMS")

BASE_DIR = Path(__file__).resolve().parent.parent.parent

FEED_EXPORT_FILE_SUFFIX = os.getenv("FEED_EXPORT_FILE_SUFFIX") or ""
FEEDS_DIR = f"{BASE_DIR}/feeds_v3/%(name)s"
FEEDS_FILE_NAME = f"%(time)s-%(batch_id)05d{FEED_EXPORT_FILE_SUFFIX}.jl"
COLLECTION_ITEM_URI = f"{FEEDS_DIR}/CollectionItem/{FEEDS_FILE_NAME}"
GAME_ITEM_URI = f"{FEEDS_DIR}/GameItem/{FEEDS_FILE_NAME}"
RANKING_ITEM_URI = f"{FEEDS_DIR}/RankingItem/{FEEDS_FILE_NAME}"
USER_ITEM_URI = f"{FEEDS_DIR}/UserItem/{FEEDS_FILE_NAME}"

FEED_EXPORTERS = {
    "sparsejsonlines": "board_game_scraper.exporters.SparseJsonLinesItemExporter",
}
FEED_EXPORT_BATCH_ITEM_COUNT = 10_000
FEEDS = {
    COLLECTION_ITEM_URI: {
        "item_classes": ["board_game_scraper.items.CollectionItem"],
        "format": "sparsejsonlines",
        "overwrite": False,
        "store_empty": False,
    },
    GAME_ITEM_URI: {
        "item_classes": ["board_game_scraper.items.GameItem"],
        "format": "sparsejsonlines",
        "overwrite": False,
        "store_empty": False,
    },
    RANKING_ITEM_URI: {
        "item_classes": ["board_game_scraper.items.RankingItem"],
        "format": "sparsejsonlines",
        "overwrite": False,
        "store_empty": False,
    },
    USER_ITEM_URI: {
        "item_classes": ["board_game_scraper.items.UserItem"],
        "format": "sparsejsonlines",
        "overwrite": False,
        "store_empty": False,
    },
}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "board-game-scraper (+https://recommend.games)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True
ROBOTSTXT_PARSER = "scrapy.robotstxt.PythonRobotParser"

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 8

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0  # Use custom setting for each spider
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 8
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = True

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    "scrapy.spidermiddlewares.httperror.HttpErrorMiddleware": 50,
    "scrapy.spidermiddlewares.referer.RefererMiddleware": 700,
    "scrapy.spidermiddlewares.urllength.UrlLengthMiddleware": 800,
    "scrapy.spidermiddlewares.depth.DepthMiddleware": 900,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.offsite.OffsiteMiddleware": 50,
    "scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware": 100,
    "scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware": 300,
    "scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware": 350,
    "scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware": 400,
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 500,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
    "scrapy_extensions.DelayedRetryMiddleware": 555,
    "scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware": 560,
    "scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware": 580,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 590,
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": 600,
    "scrapy.downloadermiddlewares.cookies.CookiesMiddleware": 700,
    "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 750,
    "scrapy.downloadermiddlewares.stats.DownloaderStats": 850,
    "scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware": 900,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    "scrapy.extensions.corestats.CoreStats": 0,
    "scrapy.extensions.telnet.TelnetConsole": 0,
    "scrapy.extensions.memusage.MemoryUsage": 0,
    "scrapy.extensions.memdebug.MemoryDebugger": 0,
    "scrapy.extensions.closespider.CloseSpider": 0,
    "scrapy.extensions.feedexport.FeedExporter": 0,
    "scrapy.extensions.logstats.LogStats": 0,
    "scrapy.extensions.spiderstate.SpiderState": 0,
    "scrapy.extensions.throttle.AutoThrottle": None,
    "scrapy_extensions.NicerAutoThrottle": 0,
    "board_game_scraper.extensions.ScrapePremiumUsersExtension": 500,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "board_game_scraper.pipelines.LimitImagesPipeline": 500,
    "scrapy.pipelines.images.ImagesPipeline": 600,
    "scrapy_extensions.BlurHashPipeline": 700,
}

# See https://doc.scrapy.org/en/latest/topics/extensions.html#module-scrapy.extensions.closespider
CLOSESPIDER_TIMEOUT = os.getenv("CLOSESPIDER_TIMEOUT") or 60 * 60 * 3  # 3 hours

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = max(DOWNLOAD_DELAY * 2, 5)
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = CONCURRENT_REQUESTS_PER_DOMAIN
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False
AUTOTHROTTLE_HTTP_CODES = (429, 503, 504)

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 60 * 60 * 24 * 7  # 1 week
# HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = (202, 408, 429, 500, 502, 503, 504)
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"
HTTPCACHE_POLICY = "scrapy.extensions.httpcache.RFC2616Policy"

# Retry settings
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#retrymiddleware-settings
RETRY_ENABLED = True
RETRY_TIMES = 2
RETRY_HTTP_CODES = (408, 429, 500, 502, 503, 504, 522, 524)
RETRY_PRIORITY_ADJUST = -1

# Delayed retry settings
DELAYED_RETRY_HTTP_CODES = (202,)
DELAYED_RETRY_TIMES = -1
DELAYED_RETRY_PRIORITY_ADJUST = 1
DELAYED_RETRY_DELAY = 10.0
DELAYED_RETRY_BACKOFF = True
DELAYED_RETRY_BACKOFF_MAX_DELAY = 100.0

MEDIA_ALLOW_REDIRECTS = True

# Image processing
# https://docs.scrapy.org/en/latest/topics/media-pipeline.html#using-the-images-pipeline
IMAGES_STORE = BASE_DIR / "images"
IMAGES_URLS_FIELD = "image_url_download"
IMAGES_RESULT_FIELD = "image_file"
IMAGES_EXPIRES = 360
# IMAGES_THUMBS = {"thumb": (1024, 1024)}

# Limit images to download
LIMIT_IMAGES_TO_DOWNLOAD = 0
LIMIT_IMAGES_URLS_FIELD = "image_url"

# BlurHash
BLURHASH_FIELD = "image_blurhash"
BLURHASH_X_COMPONENTS = 4
BLURHASH_Y_COMPONENTS = 4

# Scrape premium users
SCRAPE_PREMIUM_USERS_ENABLED = False
SCRAPE_PREMIUM_USERS_LIST = os.getenv("SCRAPE_PREMIUM_USERS_LIST")
SCRAPE_PREMIUM_USERS_CONFIG_DIR = os.getenv("SCRAPE_PREMIUM_USERS_CONFIG_DIR")
SCRAPE_PREMIUM_USERS_INTERVAL = (
    os.getenv("SCRAPE_PREMIUM_USERS_INTERVAL") or 60 * 60  # 1 hour
)
SCRAPE_PREMIUM_USERS_PREVENT_RESCRAPE_FOR = (
    os.getenv("SCRAPE_PREMIUM_USERS_PREVENT_RESCRAPE_FOR") or 3 * 60 * 60  # 3 hours
)

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
