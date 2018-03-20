# -*- coding: utf-8 -*-

''' BoardGameGeek spider '''

import re

from urllib.parse import unquote_plus, urlencode

from scrapy import Request, Spider

from ..items import GameItem, RatingItem
from ..loaders import GameLoader, RatingLoader
from ..utils import extract_query_param, now, parse_int


URL_REGEX_BOARD_GAME = re.compile(r'^.*/boardgame/(\d+).*$')
URL_REGEX_USER = re.compile(r'^.*/user/([^/]+).*$')


def extract_bgg_id(url):
    ''' extract BGG ID from URL '''

    match = URL_REGEX_BOARD_GAME.match(url)
    bgg_id = parse_int(match.group(1)) if match else None
    return bgg_id if bgg_id is not None else parse_int(extract_query_param(url, 'id'))


def extract_user_name(url):
    ''' extract user name from BGG url '''

    match = URL_REGEX_USER.match(url)
    return unquote_plus(match.group(1)) if match else extract_query_param(url, 'username')


class BggSpider(Spider):
    ''' BoardGameGeek spider '''

    name = 'bgg'
    allowed_domains = ['boardgamegeek.com']
    start_urls = (
        'https://boardgamegeek.com/browse/boardgame/',
        'https://boardgamegeek.com/browse/user/numreviews',
        'https://boardgamegeek.com/browse/user/numsessions')
    item_classes = (GameItem, RatingItem)

    # https://www.boardgamegeek.com/wiki/page/BGG_XML_API2
    xml_api_url = 'https://www.boardgamegeek.com/xmlapi2'
    page_size = 100

    custom_settings = {
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 4,
        'DELAYED_RETRY_ENABLED': True,
        'DELAYED_RETRY_HTTP_CODES': (202,),
        'DELAYED_RETRY_DELAY': 5.0,
        'AUTOTHROTTLE_HTTP_CODES': (429, 503),
    }

    def _api_url(self, action, **kwargs):
        kwargs['pagesize'] = self.page_size
        return '{}/{}?{}'.format(self.xml_api_url, action, urlencode(kwargs))

    def _game_request(self, bgg_id, *, page=1, priority=0, **kwargs):
        url = self._api_url(
            action='thing', id=bgg_id, stats=1, versions=1, videos=1, ratingcomments=1, page=1
        ) if page == 1 else self._api_url(
            action='thing', id=bgg_id, ratingcomments=1, page=page)

        request = Request(url, callback=self.parse_game, priority=priority)
        request.meta['bgg_id'] = bgg_id
        request.meta['page'] = page
        request.meta.update(kwargs)

        return request

    def _collection_request(self, user_name, *, priority=0, **kwargs):
        url = self._api_url(
            action='collection', username=user_name, subtype='boardgame',
            excludesubtype='boardgameexpansion', rated=1, brief=1, stats=1, version=0)

        request = Request(url, callback=self.parse_collection, priority=priority)
        request.meta['bgg_user_name'] = user_name
        request.meta.update(kwargs)

        return request

    def parse(self, response):
        '''
        @url https://boardgamegeek.com/browse/boardgame/
        @returns items 0 0
        @returns requests 200
        '''

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

        for url in response.xpath('//@href').extract():
            bgg_id = extract_bgg_id(url)
            if bgg_id is not None:
                yield self._game_request(bgg_id, profile_url=response.urljoin(url))

            user_name = extract_user_name(url)
            if user_name:
                yield self._collection_request(user_name)

    def parse_game(self, response):
        # pylint: disable=line-too-long
        '''
        @url https://www.boardgamegeek.com/xmlapi2/thing?id=13&stats=1&versions=1&videos=1&ratingcomments=1&page=1&pagesize=100
        @returns items 101 101
        @returns requests 101 101
        @scrapes bgg_id avg_rating
        '''

        profile_url = response.meta.get('profile_url')
        scraped_at = now()

        for game in response.xpath('/items/item'):
            bgg_id = game.xpath('@id').extract_first() or response.meta.get('bgg_id')
            page = parse_int(
                game.xpath('comments/@page').extract_first() or response.meta.get('page'))
            total_items = parse_int(
                game.xpath('comments/@totalitems').extract_first()
                or response.meta.get('total_items'))
            comments = game.xpath('comments/comment')

            if (page is not None and total_items is not None
                    and comments and page * self.page_size < total_items):
                # pylint: disable=invalid-unary-operand-type
                request = self._game_request(
                    bgg_id, page=page + 1, priority=-page,
                    skip_game_item=True, profile_url=profile_url)
                self.logger.debug('scraping more ratings from page %d: %r', page + 1, request)
                yield request

            for comment in comments:
                user_name = comment.xpath('@username').extract_first()

                if not user_name:
                    self.logger.warning('no user name found, cannot process rating')
                    continue

                yield self._collection_request(user_name)

                ldr = RatingLoader(
                    item=RatingItem(bgg_id=bgg_id, bgg_user_name=user_name, scraped_at=scraped_at),
                    selector=comment, response=response)
                ldr.add_xpath('avg_rating', '@rating')
                yield ldr.load_item()

            if response.meta.get('skip_game_item'):
                continue

            ldr = GameLoader(
                item=GameItem(
                    bgg_id=bgg_id, scraped_at=scraped_at,
                    worst_rating=1, best_rating=10,
                    easiest_complexity=1, hardest_complexity=5),
                selector=game, response=response)

            ldr.add_xpath('name', 'name[@type = "primary"]/@value')
            ldr.add_xpath('alt_name', 'name/@value')
            ldr.add_xpath('year', 'yearpublished/@value')
            ldr.add_xpath('description', 'description')

            ldr.add_xpath('designer', 'link[@type = "boardgamedesigner"]/@value')
            ldr.add_xpath('artist', 'link[@type = "boardgameartist"]/@value')
            ldr.add_xpath('publisher', 'link[@type = "boardgamepublisher"]/@value')

            ldr.add_value('url', profile_url)
            ldr.add_value('url', 'https://boardgamegeek.com/boardgame/{}'.format(bgg_id))
            images = game.xpath('image/text()').extract()
            ldr.add_value('image_url', (response.urljoin(i) for i in images))
            images = game.xpath('thumbnail/text()').extract()
            ldr.add_value('image_url', (response.urljoin(i) for i in images))
            videos = game.xpath('videos/video/@link').extract()
            ldr.add_value('video_url', (response.urljoin(v) for v in videos))

            ldr.add_xpath('min_players', 'minplayers/@value')
            ldr.add_xpath('max_players', 'maxplayers/@value')
            ldr.add_xpath('min_age', 'minage/@value')
            ldr.add_xpath('max_age', 'maxage/@value')
            ldr.add_xpath('min_time', 'minplaytime/@value')
            ldr.add_xpath('max_time', 'maxplaytime/@value')

            ldr.add_xpath('rank', 'statistics/ratings/ranks/rank[@name = "boardgame"]/@value')
            ldr.add_xpath('num_votes', 'statistics/ratings/usersrated/@value')
            ldr.add_xpath('avg_rating', 'statistics/ratings/average/@value')
            ldr.add_xpath('stddev_rating', 'statistics/ratings/stddev/@value')
            ldr.add_xpath('bayes_rating', 'statistics/ratings/bayesaverage/@value')
            ldr.add_xpath('complexity', 'statistics/ratings/averageweight/@value')

            yield ldr.load_item()

    def parse_collection(self, response):
        # pylint: disable=line-too-long
        '''
        @url https://www.boardgamegeek.com/xmlapi2/collection?username=Markus+Shepherd&subtype=boardgame&excludesubtype=boardgameexpansion&rated=1&brief=1&stats=1&version=0
        @returns items 130
        @returns requests 130
        @scrapes bgg_id bgg_user_name avg_rating
        '''

        user_name = response.meta.get('bgg_user_name') or extract_user_name(response.url)
        scraped_at = now()

        if not user_name:
            self.logger.warning('no user name found, cannot process collection')
            return

        for game in response.xpath('/items/item'):
            bgg_id = game.xpath('@objectid').extract_first()

            if not bgg_id:
                self.logger.warning('no BGG ID found, cannot process rating')
                continue

            yield self._game_request(bgg_id)

            ldr = RatingLoader(
                item=RatingItem(bgg_id=bgg_id, bgg_user_name=user_name, scraped_at=scraped_at),
                selector=game, response=response)
            ldr.add_xpath('avg_rating', 'stats/rating/@value')
            yield ldr.load_item()
