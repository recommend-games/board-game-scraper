# -*- coding: utf-8 -*-

''' Board Game Atlas spider '''

from scrapy import Spider

from ..items import GameItem
from ..loaders import GameJsonLoader
from ..utils import now, parse_json


class BgaSpider(Spider):
    ''' Board Game Atlas spider '''

    name = 'bga'
    allowed_domains = ('boardgameatlas.com',)
    item_classes = (GameItem,)
    start_urls = tuple(
        'https://www.boardgameatlas.com/api/search?order-by=popularity'
        f'&limit=100&skip={page * 100}'
        for page in range(225))

    def parse(self, response):
        '''
        @url https://www.boardgameatlas.com/api/search?ids=OIXt3DmJU0
        @returns items 1 1
        @returns requests 0 0
        @scrapes name year description designer publisher url image_url \
            list_price min_players max_players min_age min_time max_time \
            worst_rating best_rating bga_id scraped_at
        '''

        result = parse_json(response.text) if hasattr(response, 'text') else None
        games = result.get('games', ()) if result else ()
        scraped_at = now()

        for game in games:
            ldr = GameJsonLoader(
                item=GameItem(
                    scraped_at=scraped_at,
                    worst_rating=1,
                    best_rating=5,
                ),
                json_obj=game,
                response=response,
            )

            ldr.add_jmes('name', 'name')
            ldr.add_jmes('alt_name', 'names')
            ldr.add_jmes('year', 'year_published')
            ldr.add_jmes('description', 'description_preview')
            ldr.add_jmes('description', 'description')

            ldr.add_jmes('designer', 'designers')
            ldr.add_jmes('artist', 'artists')
            ldr.add_jmes('publisher', 'primary_publisher')
            ldr.add_jmes('publisher', 'publishers')

            ldr.add_jmes('url', 'url')
            ldr.add_jmes('image_url', 'image_url')
            ldr.add_jmes('image_url', 'thumb_url')

            ldr.add_jmes('list_price', 'msrp')

            ldr.add_jmes('min_players', 'min_players')
            ldr.add_jmes('max_players', 'max_players')
            ldr.add_jmes('min_age', 'min_age')
            ldr.add_jmes('min_time', 'min_playtime')
            ldr.add_jmes('max_time', 'max_playtime')

            ldr.add_jmes('bga_id', 'id')

            # TODO make further requests for prices, images, videos, and reviews (external links)

            yield ldr.load_item()
