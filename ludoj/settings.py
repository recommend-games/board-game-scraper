# -*- coding: utf-8 -*-

BOT_NAME = 'ludoj'

SPIDER_MODULES = ['ludoj.spiders']
NEWSPIDER_MODULE = 'ludoj.spiders'

FEED_EXPORT_FIELDS = ('name', 'alt_name', 'year',
                      'game_type', 'description',
                      'designer', 'artist', 'publisher',
                      'url', 'image_url',
                      'video_url', 'external_link', 'list_price',
                      'min_players', 'max_players',
                      'min_age', 'max_age',
                      'min_time', 'max_time',
                      'rank', 'num_votes', 'avg_rating',
                      'stddev_rating', 'bayes_rating',
                      'worst_rating', 'best_rating',
                      'complexity', 'easiest_complexity', 'hardest_complexity',
                      'bgg_id', 'freebase_id', 'wikidata_id',
                      'wikipedia_id', 'dbpedia_id', 'luding_id')

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'ludoj (+http://www.yourdomain.com)'
# USER_AGENT = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
#               'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36')

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 2.0
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 1
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'ludoj.middlewares.MyCustomSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'ludoj.middlewares.MyCustomDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'ludoj.pipelines.ValidatePipeline': 100,
    'ludoj.pipelines.DataTypePipeline': 200,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 0.5
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600
#HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [500, 502, 503, 504, 408, 429]
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
HTTPCACHE_POLICY = 'scrapy.extensions.httpcache.RFC2616Policy'

RETRY_ENABLED = True
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]
