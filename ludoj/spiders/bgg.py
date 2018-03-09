# -*- coding: utf-8 -*-

''' BoardGameGeek spider '''

import re

from urllib.parse import unquote_plus, urlencode

from scrapy import Request, Spider

from ..items import GameItem, RatingItem
from ..loaders import GameLoader, RatingLoader


URL_REGEX_BOARD_GAME = re.compile(r'^.*/boardgame/(\d+).*$')
URL_REGEX_USER = re.compile(r'^.*/user/([^/]+).*$')


def _extract_bgg_id(url):
    match = URL_REGEX_BOARD_GAME.match(url)
    return int(match.group(1)) if match else None


def _extract_user_name(url):
    match = URL_REGEX_USER.match(url)
    return unquote_plus(match.group(1)) if match else None


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
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1,
    }

    def _api_url(self, action, **kwargs):
        kwargs['pagesize'] = self.page_size
        return '{}/{}?{}'.format(self.xml_api_url, action, urlencode(kwargs))

    def _game_request(self, bgg_id, profile_url=None):
        url = self._api_url(
            action='thing', id=bgg_id, stats=1, versions=1, videos=1, ratingcomments=1)
        request = Request(url, callback=self.parse_game)
        request.meta['bgg_id'] = bgg_id
        request.meta['profile_url'] = profile_url
        return request

    def _collection_request(self, user_name):
        url = self._api_url(
            action='collection', username=user_name, subtype='boardgame',
            excludesubtype='boardgameexpansion', rated=1, brief=1, stats=1, version=0)
        request = Request(url, callback=self.parse_collection)
        request.meta['bgg_user_name'] = user_name
        return request

    def parse(self, response):
        '''
        @url https://boardgamegeek.com/browse/boardgame/
        @returns items 0 0
        @returns requests 101 101
        '''

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

        for url in response.xpath('//@href').extract():
            bgg_id = _extract_bgg_id(url)
            if bgg_id is not None:
                yield self._game_request(bgg_id, response.urljoin(url))

            user_name = _extract_user_name(url)
            if user_name:
                yield self._collection_request(user_name)

    def parse_game(self, response):
        '''
        @url https://www.boardgamegeek.com/xmlapi2/thing?id=13&stats=1&versions=1&videos=1
        @returns items 1 1
        @returns requests 0 0
        @scrapes name year description designer artist publisher \
                 image_url video_url \
                 min_players max_players min_age min_time max_time \
                 rank num_votes avg_rating stddev_rating bayes_rating \
                 worst_rating best_rating complexity \
                 easiest_complexity hardest_complexity bgg_id
        '''

        for game in response.xpath('/items/item'):
            bgg_id = game.xpath('@id').extract_first() or response.meta.get('bgg_id')

            # TODO yield requests for other pages
            for comment in game.xpath('comments/comment'):
                user_name = comment.xpath('@username').extract_first()

                if not user_name:
                    self.logger.warning('no user name found, cannot process rating')
                    continue

                yield self._collection_request(user_name)

                ldr = RatingLoader(
                    item=RatingItem(bgg_id=bgg_id, bgg_user_name=user_name),
                    selector=comment, response=response)
                ldr.add_xpath('avg_rating', '@rating')
                yield ldr.load_item()

            if response.meta.get('skip_game_item'):
                continue

            ldr = GameLoader(item=GameItem(), selector=game, response=response)

            ldr.add_xpath('name', 'name[@type = "primary"]/@value')
            ldr.add_xpath('alt_name', 'name/@value')
            ldr.add_xpath('year', 'yearpublished/@value')
            ldr.add_xpath('description', 'description')

            ldr.add_xpath('designer', 'link[@type = "boardgamedesigner"]/@value')
            ldr.add_xpath('artist', 'link[@type = "boardgameartist"]/@value')
            ldr.add_xpath('publisher', 'link[@type = "boardgamepublisher"]/@value')

            ldr.add_value('url', response.meta.get('profile_url'))
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
            ldr.add_value('worst_rating', '1')
            ldr.add_value('best_rating', '10')

            ldr.add_xpath('complexity', 'statistics/ratings/averageweight/@value')
            ldr.add_value('easiest_complexity', '1')
            ldr.add_value('hardest_complexity', '5')

            ldr.add_value('bgg_id', bgg_id)

            yield ldr.load_item()

    def parse_collection(self, response):
        '''
        TODO contract
        '''

        user_name = response.meta.get('bgg_user_name')
        # TODO parse from URL if necessary

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
                item=RatingItem(bgg_id=bgg_id, bgg_user_name=user_name),
                selector=game, response=response)
            ldr.add_xpath('avg_rating', 'stats/rating/@value')
            yield ldr.load_item()
