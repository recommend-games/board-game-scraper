"""Scraped item schema.

Parsing and type coercion live in the ItemLoaders, not here — these classes
just hold fields. The ``metadata`` markers only document intent (``identity`` =
part of the natural key, ``required`` = must be set for the row to mean
anything); nothing enforces them here, that is a loader/pipeline job.

Migration notes (not done in this file):
  * loaders.py — link fields become ID lists; add ``LinkLoader``,
    ``GameStatsLoader``; ``RankingLoader`` gains ``ranking_type``.
  * bgg.py — ``extract_game_item`` splits into game + stats + ranking rows;
    ``LegacyRankingItem`` / ``extract_ranking_item`` deleted.
  * settings.py — a feed per item class.
  * board-game-merger — new schemas for LinkItem / GameStatsItem / RankingItem;
    GameItem loses stats/rank columns, link columns become ``List(Int64)``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from attrs import define, field

from board_game_scraper.utils.dates import now
from board_game_scraper.utils.strings import lower_or_none

if TYPE_CHECKING:
    from datetime import date, datetime

__all__ = [
    "CollectionItem",
    "GameItem",
    "GameStatsItem",
    "LinkItem",
    "LinkType",
    "RankingItem",
    "RankingType",
    "Source",
    "UserItem",
]

IDENTITY = {"identity": True}
REQUIRED = {"required": True}
IDENTITY_REQUIRED = {"identity": True, "required": True}


class Source(StrEnum):
    """Where a record came from. Present on every item."""

    bgg = "bgg"
    dbpedia = "dbpedia"
    luding = "luding"
    spielen = "spielen"
    wikidata = "wikidata"


class LinkType(StrEnum):
    """The kind of thing a ``LinkItem`` describes — mirrors BGG ``<link>`` types."""

    designer = "designer"
    artist = "artist"
    publisher = "publisher"
    category = "category"
    mechanic = "mechanic"
    family = "family"
    game_type = "game_type"


class RankingType(StrEnum):
    """The kind of ranking a ``RankingItem`` records."""

    overall = "overall"  # BGG type="subtype" name="boardgame" — "Board Game Rank"
    game_type = "game_type"  # BGG type="family" — Strategy / Family / Party / War / …
    hotness = "hotness"  # BGG Hotness — the daily trending list
    award = "award"  # Spiel des Jahres & co.; rank 1 = winner, >1 = nominee
    charts = "charts"  # sales / play charts, if ever scraped


@define(kw_only=True)
class ScrapedItem:
    """Fields shared by every scraped record."""

    source: Source | None = field(default=None, metadata=REQUIRED)
    # When we fetched this. Semantic time for observations is ``observed_at``.
    scraped_at: datetime = field(factory=now, metadata=REQUIRED)


# Entities — slowly-changing, identity-keyed. One row per thing per source;
# a later scrape replaces the earlier one. Cross-references are ID lists.


@define(kw_only=True)
class GameItem(ScrapedItem):
    """A board game — description only.

    Identity: the source-native game ID (``bgg_id`` for BGG, ``luding_id`` for
    luding, …). One row per game per source; the merger resolves cross-source
    identity into a single game.

    Deliberately absent: community stats and rankings. Those change every scrape
    and are kept as observations — see ``GameStatsItem`` and ``RankingItem``.
    """

    name: str | None = field(default=None, metadata=REQUIRED)
    alt_name: list[str] | None = None
    year: int | None = None
    description: str | None = None

    # Cross-references, by source-native ID (see ``LinkItem``). Never names.
    designer: list[int] | None = None
    artist: list[int] | None = None
    publisher: list[int] | None = None
    category: list[int] | None = None
    mechanic: list[int] | None = None
    family: list[int] | None = None
    game_type: list[int] | None = None

    # Related games, by ``bgg_id``.
    compilation_of: list[int] | None = None
    expansion: list[int] | None = None
    implementation: list[int] | None = None
    integration: list[int] | None = None
    cooperative: bool | None = None
    compilation: bool | None = None

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

    # External identifiers — the join keys for cross-source merging.
    bgg_id: int | None = field(default=None, metadata=IDENTITY)
    freebase_id: str | None = field(default=None, metadata=IDENTITY)
    wikidata_id: str | None = field(default=None, metadata=IDENTITY)
    wikipedia_id: str | None = field(default=None, metadata=IDENTITY)
    dbpedia_id: str | None = field(default=None, metadata=IDENTITY)
    luding_id: int | None = field(default=None, metadata=IDENTITY)
    spielen_id: str | None = field(default=None, metadata=IDENTITY)

    # Entity timestamps: when the source says the game was published / last edited.
    published_at: datetime | None = None
    updated_at: datetime | None = None


@define(kw_only=True)
class LinkItem(ScrapedItem):
    """A person, company or taxonomy term referenced by a game.

    Maps to BGG ``<link>`` elements. Replaces the old ``"name:id"`` strings in
    ``GameItem.designer`` etc.: the game now carries ``link_id`` and the name
    lives here.

    Identity: ``(source, link_type, link_id)``.
    """

    link_id: int | str | None = field(default=None, metadata=IDENTITY_REQUIRED)
    link_type: LinkType | None = field(default=None, metadata=IDENTITY_REQUIRED)
    name: str | None = field(default=None, metadata=REQUIRED)

    # Same term's ID on BGG, when a non-BGG source names it and we can tell.
    bgg_id: int | None = field(default=None, metadata=IDENTITY)

    url: str | None = None
    image_url: list[str] | None = None
    image_url_download: list[str] | None = None
    image_file: list[dict[str, str]] | None = None
    image_blurhash: list[dict[str, str]] | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None


@define(kw_only=True)
class UserItem(ScrapedItem):
    """A BoardGameGeek user.

    Identity: ``bgg_user_name`` (lower-cased).
    """

    bgg_user_name: str | None = field(
        converter=lower_or_none,
        default=None,
        metadata=IDENTITY_REQUIRED,
    )
    item_id: int | None = None  # BGG's numeric user ID, when available
    first_name: str | None = None
    last_name: str | None = None

    registered_at: date | None = None  # was ``registered`` (a bare year int)
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


# Observations — append-only, timestamped. Each row is "at ``observed_at``, from
# ``source``, subject X had payload Y". Nothing is overwritten; history is the
# point. ``scraped_at`` (from ``ScrapedItem``) is when *we* fetched the row;
# ``observed_at`` is when the fact held — equal for a live scrape, but years
# apart for a historical dump.


@define(kw_only=True)
class GameStatsItem(ScrapedItem):
    """A snapshot of BGG community statistics for one game.

    Identity: ``(bgg_id, observed_at)``. Everything here changes on every scrape
    — that is exactly why it is no longer part of ``GameItem``.
    """

    bgg_id: int | None = field(default=None, metadata=IDENTITY_REQUIRED)
    observed_at: datetime = field(factory=now, metadata=IDENTITY_REQUIRED)

    num_votes: int | None = None
    avg_rating: float | None = None
    stddev_rating: float | None = None
    bayes_rating: float | None = None

    complexity: float | None = None
    num_complexity_votes: int | None = None
    language_dependency: float | None = None

    num_owned: int | None = None
    num_trading: int | None = None
    num_wanting: int | None = None
    num_wishlist: int | None = None
    num_comments: int | None = None


@define(kw_only=True)
class RankingItem(ScrapedItem):
    """One game's position in one ranking at one point in time.

    Identity: ``(ranking_type, ranking_id, bgg_id, observed_at)``.

    Covers every "chart" concept with a single shape:
      * ``overall``    — BGG's boardgame rank        (``ranking_id`` None)
      * ``game_type``  — Strategy / Family / War / …  (``ranking_id`` = BGG game-type id,
                         same int as the ``game_type`` ``LinkItem.link_id``)
      * ``hotness``    — the daily Hotness list       (``ranking_id`` None)
      * ``award``      — Spiel des Jahres & co.       (``ranking_id`` = award id)
    """

    ranking_type: RankingType | None = field(default=None, metadata=IDENTITY_REQUIRED)
    ranking_id: int | str | None = field(default=None, metadata=IDENTITY)
    ranking_name: str | None = None  # "Strategy Game Rank", "Spiel des Jahres 2023"

    bgg_id: int | None = field(default=None, metadata=IDENTITY_REQUIRED)
    observed_at: datetime = field(factory=now, metadata=IDENTITY_REQUIRED)

    # Denormalised for convenience / auditing; the entity of record is GameItem.
    name: str | None = None
    year: int | None = None

    rank: int | None = field(default=None, metadata=REQUIRED)
    # Some rankings ship their inputs alongside the position (historical dumps).
    bayes_rating: float | None = None
    avg_rating: float | None = None
    num_votes: int | None = None


@define(kw_only=True)
class CollectionItem(ScrapedItem):
    """One user's relationship to one game, as of one scrape.

    Identity: ``(bgg_user_name, bgg_id, observed_at)``. Rating, ownership and
    play count all drift over time, so this is an observation, not an edge.
    """

    bgg_user_name: str | None = field(
        converter=lower_or_none,
        default=None,
        metadata=IDENTITY_REQUIRED,
    )
    bgg_id: int | None = field(default=None, metadata=IDENTITY_REQUIRED)
    observed_at: datetime = field(factory=now, metadata=IDENTITY_REQUIRED)

    collection_id: int | None = None  # BGG's @collid — source-native, not the key

    bgg_user_rating: float | None = None
    bgg_user_owned: bool | None = None
    bgg_user_prev_owned: bool | None = None
    bgg_user_for_trade: bool | None = None
    bgg_user_want_in_trade: bool | None = None
    bgg_user_want_to_play: bool | None = None
    bgg_user_want_to_buy: bool | None = None
    bgg_user_preordered: bool | None = None
    bgg_user_wishlist: int | None = None  # wishlist priority 1-5
    bgg_user_play_count: int | None = None

    comment: str | None = None

    # BGG's own "last modified" on the collection entry — distinct from observed_at.
    updated_at: datetime | None = None
