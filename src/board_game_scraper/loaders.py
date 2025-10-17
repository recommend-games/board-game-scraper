from __future__ import annotations

from functools import partial
from typing import Any

from itemloaders.processors import Identity, MapCompose, TakeFirst
from scrapy.http import Response
from scrapy.loader import ItemLoader
from w3lib.html import replace_entities

from board_game_scraper.items import (
    CollectionItem,
    GameItem,
    LegacyRankingItem,
    RankingItem,
    UserItem,
)
from board_game_scraper.utils.parsers import (
    parse_bool,
    parse_date,
    parse_float,
    parse_int,
)
from board_game_scraper.utils.strings import normalize_space

normalize_space_with_newline = partial(normalize_space, preserve_newline=True)


def response_urljoin(url: str, loader_context: dict[str, Any]) -> str | None:
    if not url or not isinstance(url, str):
        return None
    assert isinstance(url, str)
    response = loader_context.get("response")
    if not response or not isinstance(response, Response):
        return url
    assert isinstance(response, Response)
    return response.urljoin(url)


class GameLoader(ItemLoader):
    default_item_class = GameItem
    # default_input_processor = MapCompose(...)
    default_output_processor = TakeFirst()

    alt_name_out = Identity()
    year_in = MapCompose(parse_int)
    game_type_out = Identity()
    description_in = MapCompose(normalize_space_with_newline)

    designer_out = Identity()
    artist_out = Identity()
    publisher_out = Identity()

    url_in = MapCompose(response_urljoin)
    official_url_in = MapCompose(response_urljoin)
    official_url_out = Identity()
    image_url_in = MapCompose(response_urljoin)
    image_url_out = Identity()
    video_url_in = MapCompose(response_urljoin)
    video_url_out = Identity()
    rules_url_in = MapCompose(response_urljoin)
    rules_url_out = Identity()
    review_url_in = MapCompose(response_urljoin)
    review_url_out = Identity()
    external_link_in = MapCompose(response_urljoin)
    external_link_out = Identity()

    min_players_in = MapCompose(parse_int)
    max_players_in = MapCompose(parse_int)
    min_players_rec_in = MapCompose(parse_int)
    max_players_rec_in = MapCompose(parse_int)
    min_players_best_in = MapCompose(parse_int)
    max_players_best_in = MapCompose(parse_int)
    min_age_in = MapCompose(parse_int)
    max_age_in = MapCompose(parse_int)
    min_age_rec_in = MapCompose(parse_float)
    max_age_rec_in = MapCompose(parse_float)
    min_time_in = MapCompose(parse_int)
    max_time_in = MapCompose(parse_int)

    category_out = Identity()
    mechanic_out = Identity()
    compilation_of_in = MapCompose(parse_int)
    compilation_of_out = Identity()
    family_out = Identity()
    expansion_out = Identity()
    implementation_in = MapCompose(parse_int)
    implementation_out = Identity()
    integration_in = MapCompose(parse_int)
    integration_out = Identity()

    rank_in = MapCompose(parse_int)
    add_rank_out = Identity()
    num_votes_in = MapCompose(parse_int)
    avg_rating_in = MapCompose(parse_float)
    stddev_rating_in = MapCompose(parse_float)
    bayes_rating_in = MapCompose(parse_float)
    complexity_in = MapCompose(parse_float)
    language_dependency_in = MapCompose(parse_float)

    num_owned_in = MapCompose(parse_int)
    num_trading_in = MapCompose(parse_int)
    num_wanting_in = MapCompose(parse_int)
    num_wishlist_in = MapCompose(parse_int)
    num_comments_in = MapCompose(parse_int)
    num_complexity_votes_in = MapCompose(parse_int)

    bgg_id_in = MapCompose(parse_int)
    luding_id_in = MapCompose(parse_int)

    published_at_in = MapCompose(parse_date)
    updated_at_in = MapCompose(parse_date)
    scraped_at_in = MapCompose(parse_date)


class BggGameLoader(GameLoader):
    description_in = MapCompose(replace_entities, normalize_space_with_newline)


class RankingLoader(ItemLoader):
    default_item_class = RankingItem
    # default_input_processor = MapCompose(...)
    default_output_processor = TakeFirst()

    ranking_id_in = MapCompose(parse_int)
    bgg_id_in = MapCompose(parse_int)
    year_in = MapCompose(parse_int)

    rank_in = MapCompose(parse_int)
    num_votes_in = MapCompose(parse_int)
    avg_rating_in = MapCompose(parse_float)
    stddev_rating_in = MapCompose(parse_float)
    bayes_rating_in = MapCompose(parse_float)

    image_url_in = MapCompose(response_urljoin)
    image_url_out = Identity()

    published_at_in = MapCompose(parse_date)
    updated_at_in = MapCompose(parse_date)
    scraped_at_in = MapCompose(parse_date)


class LegacyRankingLoader(ItemLoader):
    default_item_class = LegacyRankingItem
    # default_input_processor = MapCompose(...)
    default_output_processor = TakeFirst()

    game_type_id_in = MapCompose(parse_int)
    rank_in = MapCompose(parse_int)
    bayes_rating_in = MapCompose(parse_float)


class UserLoader(ItemLoader):
    default_item_class = UserItem
    # default_input_processor = MapCompose(...)
    default_output_processor = TakeFirst()

    item_id_in = MapCompose(parse_int)

    registered_in = MapCompose(parse_int)
    last_login_in = MapCompose(parse_date)

    external_link_in = MapCompose(response_urljoin)
    external_link_out = Identity()
    image_url_in = MapCompose(response_urljoin)
    image_url_out = Identity()

    published_at_in = MapCompose(parse_date)
    updated_at_in = MapCompose(parse_date)
    scraped_at_in = MapCompose(parse_date)


class CollectionLoader(ItemLoader):
    default_item_class = CollectionItem
    # default_input_processor = MapCompose(...)
    default_output_processor = TakeFirst()

    bgg_id_in = MapCompose(parse_int)

    bgg_user_rating_in = MapCompose(parse_float)
    bgg_user_owned_in = MapCompose(parse_bool)
    bgg_user_prev_owned_in = MapCompose(parse_bool)
    bgg_user_for_trade_in = MapCompose(parse_bool)
    bgg_user_want_in_trade_in = MapCompose(parse_bool)
    bgg_user_want_to_play_in = MapCompose(parse_bool)
    bgg_user_want_to_buy_in = MapCompose(parse_bool)
    bgg_user_preordered_in = MapCompose(parse_bool)
    bgg_user_wishlist_in = MapCompose(parse_int)
    bgg_user_play_count_in = MapCompose(parse_int)

    comment_in = MapCompose(normalize_space_with_newline)

    published_at_in = MapCompose(parse_date)
    updated_at_in = MapCompose(parse_date)
    scraped_at_in = MapCompose(parse_date)
