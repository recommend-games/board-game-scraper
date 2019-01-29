# -*- coding: utf-8 -*-

''' Board Game Atlas spider '''

from functools import partial

from scrapy import Request, Spider

from ..items import GameItem
from ..loaders import GameJsonLoader
from ..utils import extract_bga_id, now, parse_json

API_URL = 'https://www.boardgameatlas.com/api'


def _json_from_response(response):
    result = parse_json(response.text) if hasattr(response, 'text') else None
    return result or {}


def _extract_meta(response=None):
    if hasattr(response, 'meta') and response.meta:
        return response.meta
    if hasattr(response, 'request') and hasattr(response.request, 'meta'):
        return response.request.meta or {}
    return {}


def _extract_item(item=None, response=None, item_cls=GameItem):
    if item:
        return item
    meta = _extract_meta(response)
    return meta.get('item') or item_cls()


def _extract_url(item=None, response=None, default=None):
    if item and item.get('url'):
        return item['url']
    meta = _extract_meta(response)
    if meta.get('url'):
        return meta['url']
    if hasattr(response, 'url') and response.url:
        return response.url
    if hasattr(response, 'request') and hasattr(response.request, 'url'):
        return response.request.url
    return default


def _extract_bga_id(item=None, response=None):
    if item and item.get('bga_id'):
        return item['bga_id']
    meta = _extract_meta(response)
    if meta.get('bga_id'):
        return meta['bga_id']
    url = _extract_url(item, response)
    return extract_bga_id(url)


class BgaSpider(Spider):
    ''' Board Game Atlas spider '''

    name = 'bga'
    allowed_domains = ('boardgameatlas.com',)
    item_classes = (GameItem,)
    api_url = API_URL
    start_urls = tuple(
        f'{API_URL}/search?order-by=popularity&limit=100&skip={page * 100}'
        for page in range(225))

    def parse(self, response):
        '''
        @url https://www.boardgameatlas.com/api/search?order-by=popularity&limit=100
        @returns items 0 0
        @returns requests 100 100
        '''

        result = _json_from_response(response)
        games = result.get('games') or ()
        scraped_at = now()

        for game in games:
            bga_id = game.get('id') or extract_bga_id(game.get('url'))
            ldr = GameJsonLoader(
                item=GameItem(
                    bga_id=bga_id,
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

            item = ldr.load_item()
            callback = partial(self.parse_images, item=item)

            yield Request(
                url=f'{self.api_url}/game/images?game-id={bga_id}&limit=100',
                callback=callback,
                errback=callback,
                meta={'item': item, 'bga_id': bga_id},
                priority=1,
            )

    def parse_images(self, response, item=None):
        '''
        @url https://www.boardgameatlas.com/api/game/images?game-id=OIXt3DmJU0&limit=100
        @returns items 0 0
        @returns requests 1 1
        '''

        item = _extract_item(item, response)
        result = _json_from_response(response)

        ldr = GameJsonLoader(item=item, json_obj=result, response=response)
        ldr.add_value('image_url', item.get('image_url'))
        ldr.add_jmes('image_url', 'images[].url')
        ldr.add_jmes('image_url', 'images[].thumb')

        item = ldr.load_item()
        bga_id = _extract_bga_id(item, response)
        callback = partial(self.parse_videos, item=item)

        yield Request(
            url=f'{self.api_url}/game/videos?game-id={bga_id}&limit=100',
            callback=callback,
            errback=callback,
            meta={'item': item, 'bga_id': bga_id},
            priority=1,
        ) if bga_id else item

    def parse_videos(self, response, item=None):
        '''
        @url https://www.boardgameatlas.com/api/game/videos?game-id=OIXt3DmJU0&limit=100
        @returns items 0 0
        @returns requests 1 1
        '''

        item = _extract_item(item, response)
        result = _json_from_response(response)

        ldr = GameJsonLoader(item=item, json_obj=result, response=response)
        ldr.add_value('video_url', item.get('video_url'))
        ldr.add_jmes('video_url', 'videos[].url')

        item = ldr.load_item()
        bga_id = _extract_bga_id(item, response)
        callback = partial(self.parse_reviews, item=item)

        yield Request(
            url=f'{self.api_url}/game/reviews?game-id={bga_id}&limit=100',
            callback=callback,
            errback=callback,
            meta={'item': item, 'bga_id': bga_id},
            priority=1,
        ) if bga_id else item

    # pylint: disable=no-self-use
    def parse_reviews(self, response, item=None):
        '''
        @url https://www.boardgameatlas.com/api/game/reviews?game-id=OIXt3DmJU0&limit=100
        @returns items 1 1
        @returns requests 0 0
        @scrapes external_link
        '''

        item = _extract_item(item, response)
        result = _json_from_response(response)

        ldr = GameJsonLoader(item=item, json_obj=result, response=response)
        ldr.add_value('external_link', item.get('external_link'))
        ldr.add_jmes('external_link', 'reviews[].url')

        return ldr.load_item()
