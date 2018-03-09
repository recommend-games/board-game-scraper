# -*- coding: utf-8 -*-

''' BoardGameGeek spider '''

from urllib.parse import urlencode

from scrapy import Request, Spider

from ..items import GameItem, RatingItem
from ..loaders import GameLoader, RatingLoader

def _extract_bgg_id(url):
    return int(url.split('/')[2]) if url else None

class BggSpider(Spider):
    ''' BoardGameGeek spider '''

    name = 'bgg'
    allowed_domains = ['boardgamegeek.com']
    start_urls = ['https://boardgamegeek.com/browse/boardgame/']
    item_classes = (GameItem, RatingItem)

    # https://www.boardgamegeek.com/wiki/page/BGG_XML_API2
    xml_api_url = 'https://www.boardgamegeek.com/xmlapi2/thing'
    page_size = 100

    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': .5,
    }

    def _api_url(self, bgg_id, **kwargs):
        kwargs['id'] = bgg_id
        kwargs['pagesize'] = self.page_size
        return '{}?{}'.format(self.xml_api_url, urlencode(kwargs))

    def parse(self, response):
        '''
        @url https://boardgamegeek.com/browse/boardgame/
        @returns items 0 0
        @returns requests 101 101
        '''

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

        for game in response.css('tr#row_'):
            url = game.css('td.collection_objectname a::attr(href)').extract_first()
            bgg_id = _extract_bgg_id(url)

            if bgg_id is not None:
                req_url = self._api_url(bgg_id, stats=1, versions=1, videos=1, ratingcomments=1)
                request = Request(req_url, callback=self.parse_game)
                request.meta['bgg_id'] = bgg_id
                request.meta['profile_url'] = response.urljoin(url) if url else None
                yield request

    # pylint: disable=no-self-use
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
                ldr = RatingLoader(
                    item=RatingItem(bgg_id=bgg_id), selector=comment, response=response)
                ldr.add_xpath('bgg_user_name', '@username')
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
