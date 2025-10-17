from __future__ import annotations

from typing import TYPE_CHECKING

from attrs import define, field

from board_game_scraper.utils.dates import now
from board_game_scraper.utils.strings import lower_or_none

if TYPE_CHECKING:
    from datetime import datetime


@define(kw_only=True)
class GameItem:
    name: str | None = field(
        default=None,
        metadata={"required": True},
    )
    alt_name: list[str] | None = None
    year: int | None = None
    game_type: list[str] | None = None
    description: str | None = None

    designer: list[str] | None = None
    artist: list[str] | None = None
    publisher: list[str] | None = None

    url: str | None = None
    official_url: list[str] | None = None
    image_url: list[str] | None = None
    image_url_download: list[str] | None = None
    image_file: list[dict[str, str]] | None = None
    image_blurhash: list[dict[str, str]] | None = None
    video_url: list[str] | None = None
    rules_url: list[str] | None = None
    rules_file: list[dict[str, str]] | None = None
    review_url: list[str] | None = None
    external_link: list[str] | None = None
    list_price: str | None = None

    min_players: int | None = None
    max_players: int | None = None
    min_players_rec: int | None = None
    max_players_rec: int | None = None
    min_players_best: int | None = None
    max_players_best: int | None = None
    min_age: int | None = None
    max_age: int | None = None
    min_age_rec: float | None = None
    max_age_rec: float | None = None
    min_time: int | None = None
    max_time: int | None = None

    category: list[str] | None = None
    mechanic: list[str] | None = None
    cooperative: bool | None = None
    compilation: bool | None = None
    compilation_of: list[int] | None = None
    family: list[str] | None = None
    expansion: list[str] | None = None
    implementation: list[int] | None = None
    integration: list[int] | None = None

    rank: int | None = None
    add_rank: list[RankingItem] | None = None
    num_votes: int | None = None
    avg_rating: float | None = None
    stddev_rating: float | None = None
    bayes_rating: float | None = None
    complexity: float | None = None
    language_dependency: float | None = None

    num_owned: int | None = None
    num_trading: int | None = None
    num_wanting: int | None = None
    num_wishlist: int | None = None
    num_comments: int | None = None
    num_complexity_votes: int | None = None

    bgg_id: int | None = None
    freebase_id: str | None = None
    wikidata_id: str | None = None
    wikipedia_id: str | None = None
    dbpedia_id: str | None = None
    luding_id: int | None = None
    spielen_id: str | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None
    scraped_at: datetime = field(
        factory=now,
        metadata={"required": True},
    )


@define(kw_only=True)
class RankingItem:
    ranking_id: int | None = None
    ranking_type: str | None = None
    ranking_name: str | None = None

    bgg_id: int | None = field(
        default=None,
        metadata={"required": True},
    )
    name: str | None = None
    year: int | None = None

    rank: int | None = None
    num_votes: int | None = None
    avg_rating: float | None = None
    stddev_rating: float | None = None
    bayes_rating: float | None = None

    image_url: list[str] | None = None
    image_url_download: list[str] | None = None
    image_file: list[dict[str, str]] | None = None
    image_blurhash: list[dict[str, str]] | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None
    scraped_at: datetime = field(factory=now)


@define(kw_only=True)
class LegacyRankingItem:
    game_type: str | None = None
    game_type_id: int | None = None
    name: str | None = None
    rank: int | None = None
    bayes_rating: float | None = None


@define(kw_only=True)
class UserItem:
    item_id: int | None = None
    bgg_user_name: str | None = field(
        converter=lower_or_none,
        default=None,
        metadata={"required": True},
    )
    first_name: str | None = None
    last_name: str | None = None

    registered: int | None = None
    last_login: datetime | None = None

    country: str | None = None
    region: str | None = None
    city: str | None = None

    external_link: list[str] | None = None
    image_url: list[str] | None = None
    image_url_download: list[str] | None = None
    image_file: list[dict[str, str]] | None = None
    image_blurhash: list[dict[str, str]] | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None
    scraped_at: datetime = field(
        factory=now,
        metadata={"required": True},
    )


@define(kw_only=True)
class CollectionItem:
    # TODO: Default to f"{self.bgg_user_name}:{self.bgg_id}"
    item_id: str | int | None = field(
        default=None,
        metadata={"required": True},
    )
    bgg_id: int | None = field(
        default=None,
        metadata={"required": True},
    )
    bgg_user_name: str | None = field(
        converter=lower_or_none,
        default=None,
        metadata={"required": True},
    )

    bgg_user_rating: float | None = None
    bgg_user_owned: bool | None = None
    bgg_user_prev_owned: bool | None = None
    bgg_user_for_trade: bool | None = None
    bgg_user_want_in_trade: bool | None = None
    bgg_user_want_to_play: bool | None = None
    bgg_user_want_to_buy: bool | None = None
    bgg_user_preordered: bool | None = None
    bgg_user_wishlist: int | None = None
    bgg_user_play_count: int | None = None

    comment: str | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None
    scraped_at: datetime = field(
        factory=now,
        metadata={"required": True},
    )
