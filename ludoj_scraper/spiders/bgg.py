# -*- coding: utf-8 -*-

''' BoardGameGeek spider '''

import re
import statistics

from itertools import repeat
from urllib.parse import urlencode

from scrapy import signals
from scrapy import Request, Spider
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.project import get_project_settings

from ..items import GameItem, RatingItem, UserItem
from ..loaders import GameLoader, RatingLoader, UserLoader
from ..utils import (
    batchify, clear_list, extract_bgg_id, extract_bgg_user_name,
    normalize_space, now, parse_int)

DIGITS_REGEX = re.compile(r'^\D*(\d+).*$')


def _parse_int(element, xpath, default=None, lenient=False):
    if not element or not xpath:
        return default

    string = normalize_space(element.xpath(xpath).extract_first())

    if not string:
        return default

    result = parse_int(string)

    if result is None and lenient:
        match = DIGITS_REGEX.match(string)
        result = parse_int(match.group(1)) if match else None

    return result if result is not None else default


def _parse_player_count(poll):
    for result in poll.xpath('results'):
        numplayers = normalize_space(result.xpath('@numplayers').extract_first())
        players = parse_int(numplayers)

        if not players and numplayers.endswith('+'):
            players = parse_int(numplayers[:-1]) or -1
            players += 1

        if not players:
            continue

        votes_best = _parse_int(result, 'result[@value = "Best"]/@numvotes', 0)
        votes_rec = _parse_int(result, 'result[@value = "Recommended"]/@numvotes', 0)
        votes_not = _parse_int(result, 'result[@value = "Not Recommended"]/@numvotes', 0)

        yield players, votes_best, votes_rec, votes_not


def _parse_votes(poll, attr='value', enum=False):
    if not poll:
        return

    for i, result in enumerate(poll.xpath('results/result'), start=1):
        value = i if enum else _parse_int(result, '@' + attr, lenient=True)
        numvotes = _parse_int(result, '@numvotes', 0)

        if value is not None:
            yield from repeat(value, numvotes)


def _value_id(items, sep=':'):
    for item in arg_to_iter(items):
        value = item.xpath('@value').extract_first() or ''
        id_ = item.xpath('@id').extract_first() or ''
        yield f'{value}{sep}{id_}' if id_ else value


def _value_id_rank(items, sep=':'):
    for item in arg_to_iter(items):
        value = item.xpath('@friendlyname').extract_first() or ''
        value = value[:-5] if value and value.lower().endswith(' rank') else value
        id_ = item.xpath('@id').extract_first() or ''
        yield f'{value}{sep}{id_}' if id_ else value


class BggSpider(Spider):
    ''' BoardGameGeek spider '''

    name = 'bgg'
    allowed_domains = ['boardgamegeek.com']
    start_urls = (
        'https://boardgamegeek.com/browse/boardgame/',
        'https://boardgamegeek.com/browse/user/numreviews',
        'https://boardgamegeek.com/browse/user/numsessions')
    item_classes = (GameItem, UserItem, RatingItem)
    state = None

    # https://www.boardgamegeek.com/wiki/page/BGG_XML_API2
    xml_api_url = 'https://www.boardgamegeek.com/xmlapi2'
    page_size = 100

    custom_settings = {
        'DOWNLOAD_DELAY': .5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 4,
        'DELAYED_RETRY_ENABLED': True,
        'DELAYED_RETRY_HTTP_CODES': (202,),
        'DELAYED_RETRY_DELAY': 5.0,
        'AUTOTHROTTLE_HTTP_CODES': (429, 503, 504),
        'PULL_QUEUE_ENABLED': True,
    }

    scrape_ratings = False
    scrape_collections = False
    min_votes = 20

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        ''' initialise spider from crawler '''

        kwargs.pop('settings', None)
        spider = cls(*args, settings=crawler.settings, **kwargs)
        spider._set_crawler(crawler)

        crawler.signals.connect(spider._spider_opened, signal=signals.spider_opened)

        return spider

    def __init__(self, *args, settings=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._ids_seen = set()

        settings = settings or get_project_settings()

        self.scrape_ratings = settings.getbool('SCRAPE_BGG_RATINGS')
        self.scrape_collections = (
            self.scrape_ratings and settings.getbool('SCRAPE_BGG_COLLECTIONS'))
        self.min_votes = settings.getint('MIN_VOTES', self.min_votes)

        self.logger.info('scrape ratings: %r', self.scrape_ratings)
        self.logger.info('scrape collections: %r', self.scrape_collections)

    def _spider_opened(self):
        state = getattr(self, 'state', None)

        if state is None:
            self.logger.warning('no spider state found')
            state = {}
            self.state = state

        ids_seen = state.get('ids_seen') or frozenset()
        self.logger.info('%d ID(s) seen in previous state', len(ids_seen))

        self._ids_seen |= ids_seen

        self.state['ids_seen'] = self._ids_seen

    def _api_url(self, action, **kwargs):
        kwargs['pagesize'] = self.page_size
        return '{}/{}?{}'.format(
            self.xml_api_url, action, urlencode(sorted(kwargs.items(), key=lambda x: x[0])))

    def _game_requests(self, *bgg_ids, batch_size=10, page=1, priority=0, **kwargs):
        bgg_ids = clear_list(map(parse_int, bgg_ids))

        if not bgg_ids:
            return

        for batch in batchify(bgg_ids, batch_size, skip=self._ids_seen if page == 1 else None):
            batch = tuple(batch)

            ids = ','.join(map(str, batch))

            url = self._api_url(
                action='thing',
                id=ids,
                stats=1,
                videos=1,
                versions=int(self.scrape_ratings),
                ratingcomments=int(self.scrape_ratings),
                page=1,
            ) if page == 1 else self._api_url(
                action='thing',
                id=ids,
                versions=1,
                ratingcomments=1,
                page=page,
            )

            request = Request(url, callback=self.parse_game, priority=priority)

            if len(batch) == 1:
                request.meta['bgg_id'] = batch[0]
            request.meta['page'] = page
            request.meta.update(kwargs)

            yield request

            if page == 1:
                self._ids_seen.update(batch)

        self.logger.debug('seen %d games in total', len(self._ids_seen))

    def _game_request(self, bgg_id, default=None, **kwargs):
        return next(self._game_requests(bgg_id, **kwargs), default)

    def collection_request(self, user_name, *, meta=None, **kwargs):
        ''' make a collection request for that user '''

        user_name = user_name.lower()
        url = self._api_url(
            action='collection',
            username=user_name,
            subtype='boardgame',
            excludesubtype='boardgameexpansion',
            stats=1,
            version=0,
        )

        request = Request(url, callback=self.parse_collection, **kwargs)
        if meta:
            request.meta.update(meta)
        request.meta['bgg_user_name'] = user_name

        return request

    def _filter_votes(self, votes_best, votes_rec, votes_not, best=False):
        if votes_best + votes_rec + votes_not < self.min_votes / 2:
            return False

        votes_true = votes_best
        votes_false = votes_not
        if best:
            votes_false += votes_rec
        else:
            votes_true += votes_rec

        return votes_true > votes_false

    def _player_count_votes(self, game):
        min_players = _parse_int(game, 'minplayers/@value')
        max_players = _parse_int(game, 'maxplayers/@value')

        polls = game.xpath('poll[@name = "suggested_numplayers"]')
        poll = polls[0] if polls else None

        if not poll or _parse_int(poll, '@totalvotes', 0) < self.min_votes:
            return min_players, max_players, min_players, max_players

        votes = sorted(_parse_player_count(poll), key=lambda x: x[0])
        recommended = [vote[0] for vote in votes if self._filter_votes(*vote[1:], best=False)]
        best = [vote[0] for vote in votes if self._filter_votes(*vote[1:], best=True)]

        return (
            min(recommended, default=min_players),
            max(recommended, default=max_players),
            min(best, default=min_players),
            max(best, default=max_players),
        )

    def _poll(self, game, name, attr='value', enum=False, func=statistics.mean, default=None):
        polls = game.xpath('poll[@name = "{}"]'.format(name))
        poll = polls[0] if polls else None

        if not poll or _parse_int(poll, '@totalvotes', 0) < self.min_votes:
            return default

        try:
            return func(_parse_votes(poll, attr, enum))
        except Exception as exc:
            self.logger.exception(exc)

        return default

    def parse(self, response):
        '''
        @url https://boardgamegeek.com/browse/boardgame/
        @returns items 0 0
        @returns requests 11
        '''

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                priority=1,
                meta={'max_retry_times': 10})

        urls = response.xpath('//@href').extract()
        bgg_ids = filter(None, map(extract_bgg_id, map(response.urljoin, urls)))
        yield from self._game_requests(*bgg_ids)

        if not self.scrape_collections:
            return

        user_names = filter(None, map(extract_bgg_user_name, urls))
        for user_name in clear_list(user_names):
            yield self.collection_request(user_name)

    def parse_game(self, response):
        # pylint: disable=line-too-long
        '''
        @url https://www.boardgamegeek.com/xmlapi2/thing?id=13,822,36218&stats=1&versions=1&videos=1&ratingcomments=1&page=1&pagesize=100
        @returns items 3 3
        @returns requests 303 303
        @scrapes name alt_name year description \
            designer artist publisher \
            url image_url video_url \
            min_players max_players min_players_rec max_players_rec \
            min_players_best max_players_best \
            min_age min_age_rec min_time max_time \
            game_type category mechanic cooperative compilation family expansion \
            rank num_votes avg_rating stddev_rating \
            bayes_rating worst_rating best_rating \
            complexity easiest_complexity hardest_complexity \
            language_dependency lowest_language_dependency highest_language_dependency \
            bgg_id scraped_at
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
            comments = game.xpath('comments/comment') if self.scrape_ratings else ()

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

                if self.scrape_collections:
                    yield self.collection_request(user_name)

                else:
                    ldr = RatingLoader(
                        item=RatingItem(
                            bgg_id=bgg_id,
                            bgg_user_name=user_name.lower(),
                            scraped_at=scraped_at,
                        ),
                        selector=comment,
                        response=response,
                    )
                    ldr.add_xpath('bgg_user_rating', '@rating')
                    ldr.add_xpath('comment', '@value')
                    yield ldr.load_item()

            if response.meta.get('skip_game_item'):
                continue

            ldr = GameLoader(
                item=GameItem(
                    bgg_id=bgg_id,
                    scraped_at=scraped_at,
                    worst_rating=1,
                    best_rating=10,
                    easiest_complexity=1,
                    hardest_complexity=5,
                    lowest_language_dependency=1,
                    highest_language_dependency=5,
                ),
                selector=game,
                response=response,
            )

            ldr.add_xpath('name', 'name[@type = "primary"]/@value')
            ldr.add_xpath('alt_name', 'name/@value')
            ldr.add_xpath('year', 'yearpublished/@value')
            ldr.add_xpath('description', 'description')

            ldr.add_value('designer', _value_id(game.xpath('link[@type = "boardgamedesigner"]')))
            ldr.add_value('artist', _value_id(game.xpath('link[@type = "boardgameartist"]')))
            ldr.add_value('publisher', _value_id(game.xpath('link[@type = "boardgamepublisher"]')))

            ldr.add_value('url', profile_url)
            ldr.add_value('url', 'https://boardgamegeek.com/boardgame/{}'.format(bgg_id))
            images = game.xpath('image/text()').extract()
            ldr.add_value('image_url', (response.urljoin(i) for i in images))
            images = game.xpath('thumbnail/text()').extract()
            ldr.add_value('image_url', (response.urljoin(i) for i in images))
            videos = game.xpath('videos/video/@link').extract()
            ldr.add_value('video_url', (response.urljoin(v) for v in videos))

            (min_players_rec, max_players_rec,
             min_players_best, max_players_best) = self._player_count_votes(game)

            ldr.add_xpath('min_players', 'minplayers/@value')
            ldr.add_xpath('max_players', 'maxplayers/@value')
            ldr.add_value('min_players_rec', min_players_rec)
            ldr.add_value('max_players_rec', max_players_rec)
            ldr.add_value('min_players_best', min_players_best)
            ldr.add_value('max_players_best', max_players_best)

            ldr.add_xpath('min_age', 'minage/@value')
            ldr.add_xpath('max_age', 'maxage/@value')
            ldr.add_value(
                'min_age_rec',
                self._poll(game, 'suggested_playerage', func=statistics.median_grouped))
            ldr.add_xpath('min_time', 'minplaytime/@value')
            ldr.add_xpath('min_time', 'playingtime/@value')
            ldr.add_xpath('max_time', 'maxplaytime/@value')
            ldr.add_xpath('max_time', 'playingtime/@value')
            ldr.add_xpath('max_time', 'minplaytime/@value')

            ldr.add_value(
                'game_type',
                _value_id_rank(game.xpath('statistics/ratings/ranks/rank[@type = "family"]')))
            ldr.add_value('category', _value_id(game.xpath('link[@type = "boardgamecategory"]')))
            ldr.add_value('mechanic', _value_id(game.xpath('link[@type = "boardgamemechanic"]')))
            # look for <link type="boardgamemechanic" id="2023" value="Co-operative Play" />
            ldr.add_value(
                'cooperative',
                bool(game.xpath('link[@type = "boardgamemechanic" and @id = "2023"]')))
            ldr.add_value(
                'compilation',
                bool(game.xpath('link[@type = "boardgamecompilation" and @inbound = "true"]')))
            ldr.add_xpath(
                'compilation_of',
                'link[@type = "boardgamecompilation" and @inbound = "true"]/@id')
            ldr.add_value('family', _value_id(game.xpath('link[@type = "boardgamefamily"]')))
            ldr.add_value('expansion', _value_id(game.xpath('link[@type = "boardgameexpansion"]')))
            ldr.add_xpath(
                'implementation',
                'link[@type = "boardgameimplementation" and @inbound = "true"]/@id')
            ldr.add_xpath('integration', 'link[@type = "boardgameintegration"]/@id')

            ldr.add_xpath('rank', 'statistics/ratings/ranks/rank[@name = "boardgame"]/@value')
            ldr.add_xpath('num_votes', 'statistics/ratings/usersrated/@value')
            ldr.add_xpath('avg_rating', 'statistics/ratings/average/@value')
            ldr.add_xpath('stddev_rating', 'statistics/ratings/stddev/@value')
            ldr.add_xpath('bayes_rating', 'statistics/ratings/bayesaverage/@value')
            ldr.add_xpath('complexity', 'statistics/ratings/averageweight/@value')
            ldr.add_value(
                'language_dependency',
                self._poll(
                    game, 'language_dependence',
                    attr='level', enum=True, func=statistics.median_grouped))

            yield ldr.load_item()

    def parse_collection(self, response):
        # pylint: disable=line-too-long
        '''
        @url https://www.boardgamegeek.com/xmlapi2/collection?username=Markus+Shepherd&subtype=boardgame&excludesubtype=boardgameexpansion&stats=1&version=0
        @returns items 950
        @returns requests 90
        @scrapes bgg_user_name scraped_at
        '''

        user_name = response.meta.get('bgg_user_name') or extract_bgg_user_name(response.url)
        scraped_at = now()

        if not user_name:
            self.logger.warning('no user name found, cannot process collection')
            return

        user_name = user_name.lower()

        ldr = UserLoader(
            item=UserItem(bgg_user_name=user_name, scraped_at=scraped_at),
            response=response,
        )
        ldr.add_xpath('updated_at', '/items/@pubdate')
        yield ldr.load_item()

        games = response.xpath('/items/item')
        bgg_ids = games.xpath('@objectid').extract()
        yield from self._game_requests(*bgg_ids)

        for game in games:
            bgg_id = game.xpath('@objectid').extract_first()

            if not bgg_id:
                self.logger.warning('no BGG ID found, cannot process rating')
                continue

            ldr = RatingLoader(
                item=RatingItem(
                    bgg_id=bgg_id,
                    bgg_user_name=user_name,
                    scraped_at=scraped_at,
                ),
                selector=game,
                response=response,
            )

            ldr.add_xpath('bgg_user_rating', 'stats/rating/@value')
            ldr.add_xpath('bgg_user_owned', 'status/@own')
            ldr.add_xpath('bgg_user_prev_owned', 'status/@prevowned')
            ldr.add_xpath('bgg_user_for_trade', 'status/@fortrade')
            ldr.add_xpath('bgg_user_want_in_trade', 'status/@want')
            ldr.add_xpath('bgg_user_want_to_play', 'status/@wanttoplay')
            ldr.add_xpath('bgg_user_want_to_buy', 'status/@wanttobuy')
            ldr.add_xpath('bgg_user_preordered', 'status/@preordered')
            ldr.add_xpath('bgg_user_wishlist', 'status[@wishlist = "1"]/@wishlistpriority')
            ldr.add_xpath('bgg_user_play_count', 'numplays/text()')

            ldr.add_xpath('comment', 'comment/text()')

            ldr.add_xpath('updated_at', 'status/@lastmodified')

            yield ldr.load_item()
