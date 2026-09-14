"""Scraped item schema.

Parsing and type coercion live in the ItemLoaders, not here — these classes
just hold fields. The ``metadata`` markers only document intent (``identity`` =
part of the natural key, ``required`` = must be set for the row to mean
anything); nothing enforces them here, that is a loader/pipeline job.

Two groups of items:

* Entities (``GameItem``, ``LinkItem``, ``BggUserItem``) — slowly-changing,
  identity-keyed. One row per thing per source; a later scrape replaces the
  earlier one. Cross-references between entities are ID lists, never names.
* Observations (``BggStatsItem``, ``BggRankingItem``, ``BggCollectionItem``) —
  each row is "at ``observed_at``, subject X had payload Y", rather than one
  row per subject overwritten in place. ``scraped_at`` is when *we* fetched
  the row; ``observed_at`` is when the fact held — equal for a live scrape,
  but years apart for a historical dump. Whether every scrape's row is kept
  or only changes are (deduping consecutive-identical observations, keeping
  latest-only, …) is a merger/storage retention policy, not part of this
  schema — and it should differ by item: ``BggStatsItem``/``BggRankingItem``
  are bounded by game count and cheap to keep in full; ``BggCollectionItem``
  is bounded by users times games and is not.

``GameItem`` and ``LinkItem`` are source-generic (BGG, luding, wikidata, …).
The ``Bgg*`` classes are BoardGameGeek-shaped and always carry ``source=bgg``.
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
    "BggCollectionItem",
    "BggPollItem",
    "BggRankingItem",
    "BggStatsItem",
    "BggUserItem",
    "GameItem",
    "Image",
    "LinkItem",
    "LinkKind",
    "PollKind",
    "RankingType",
    "Source",
]

IDENTITY = {"identity": True}
REQUIRED = {"required": True}
IDENTITY_REQUIRED = {"identity": True, "required": True}


class Source(StrEnum):
    """Where a record came from. Present on every item."""

    bga = "bga"
    bgg = "bgg"
    dbpedia = "dbpedia"
    luding = "luding"
    spielen = "spielen"
    wikidata = "wikidata"


class LinkKind(StrEnum):
    """What kind of thing a ``LinkItem`` is — *not* how it relates to a game.

    A person can be both a game's designer and its artist (BGG gives them one
    shared id); the role is carried by which ``GameItem`` field references the
    id, so ``designer`` and ``artist`` both point at the same ``person`` row.
    """

    person = "person"  # designer, artist — BGG shares the id namespace
    company = "company"  # publisher
    category = "category"
    mechanic = "mechanic"
    family = "family"
    game_type = "game_type"


class RankingType(StrEnum):
    """The kind of BGG ranking a ``BggRankingItem`` records."""

    overall = "overall"  # BGG type="subtype" name="boardgame" — "Board Game Rank"
    game_type = "game_type"  # BGG type="family" — Strategy / Family / Party / War / …
    hotness = "hotness"  # BGG Hotness — the daily trending list


class PollKind(StrEnum):
    """Which BGG community poll a ``BggPollItem`` row comes from."""

    numplayers = "numplayers"  # per player count: Best / Recommended / Not Recommended
    playerage = "playerage"  # per suggested age
    language_dependence = "language_dependence"  # per dependency level, 1-5
    weight = "weight"  # per complexity rating, 1-5 — raw poll unconfirmed, see BggPollItem


@define(kw_only=True)
class ScrapedItem:
    """Fields shared by every scraped record."""

    source: Source | None = field(default=None, metadata=REQUIRED)
    # When we fetched this. Semantic time for observations is ``observed_at``.
    scraped_at: datetime = field(factory=now, metadata=REQUIRED)


@define(kw_only=True)
class Image:
    """One image found for an entity, and whether/how it was archived."""

    url: str | None = None
    fetch: bool = False  # request: should the pipeline download this one
    file: str | None = None  # result: local path, once fetched
    checksum: str | None = None
    blurhash: str | None = None


# --- Entities --------------------------------------------------------------- #


@define(kw_only=True)
class GameItem(ScrapedItem):
    """A board game — description only.

    Identity: the source-native game ID (``bgg_id`` for BGG, ``luding_id`` for
    luding, …). One row per game per source; the merger resolves cross-source
    identity into a single game.

    Deliberately absent: community stats and rankings. Those change every scrape
    and are kept as observations — see ``BggStatsItem`` and ``BggRankingItem``.
    """

    name: str | None = field(default=None, metadata=REQUIRED)
    alt_name: list[str] | None = None
    year: int | None = None
    description: str | None = None

    # Cross-references, by source-native ``LinkItem.link_id`` (a string). The
    # *role* is the field name: ``designer`` and ``artist`` may hold the same id.
    designer: list[str] | None = None  # -> LinkItem kind=person
    artist: list[str] | None = None  # -> LinkItem kind=person
    publisher: list[str] | None = None  # -> LinkItem kind=company
    category: list[str] | None = None  # -> LinkItem kind=category
    mechanic: list[str] | None = None  # -> LinkItem kind=mechanic
    family: list[str] | None = None  # -> LinkItem kind=family
    game_type: list[str] | None = None  # -> LinkItem kind=game_type

    # Related games, by this same source's game id (like the link fields above).
    compilation_of: list[str] | None = None
    expansion: list[str] | None = None
    implementation: list[str] | None = None
    integration: list[str] | None = None
    cooperative: bool | None = None
    compilation: bool | None = None

    url: str | None = None
    official_url: list[str] | None = None
    images: list[Image] | None = None
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
    bga_id: int | None = field(default=None, metadata=IDENTITY)
    bga_slug: str | None = field(default=None, metadata=IDENTITY)
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

    Replaces the old ``"name:id"`` strings in ``GameItem.designer`` etc.: the
    game carries ``link_id``, the name lives here. One row per thing, not per
    role — a designer-and-artist is a single ``person`` row.

    Identity: ``(source, kind, link_id)``. ``link_id`` is always a string;
    BGG ids are just numeric strings.
    """

    link_id: str | None = field(default=None, metadata=IDENTITY_REQUIRED)
    kind: LinkKind | None = field(default=None, metadata=IDENTITY_REQUIRED)
    name: str | None = field(default=None, metadata=REQUIRED)

    url: str | None = None
    images: list[Image] | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None


@define(kw_only=True)
class BggUserItem(ScrapedItem):
    """A BoardGameGeek user.

    Identity: ``user_name`` (lower-cased).
    """

    user_name: str | None = field(
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
    images: list[Image] | None = None

    published_at: datetime | None = None
    updated_at: datetime | None = None


# --- Observations (BoardGameGeek) ------------------------------------------- #


@define(kw_only=True)
class BggStatsItem(ScrapedItem):
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
class BggPollItem(ScrapedItem):
    """One bucket's vote count from a BGG community poll, for one game.

    Identity: ``(bgg_id, poll, bucket, label, observed_at)``. ``bucket`` is the
    poll's primary axis (player count, suggested age, dependency level, …).
    ``label`` is only load-bearing for ``numplayers``, where the same player
    count has three rows — Best / Recommended / Not Recommended — that would
    otherwise collide; for other polls it's a denormalised description of
    ``bucket`` and safe to leave unset.
    """

    bgg_id: int | None = field(default=None, metadata=IDENTITY_REQUIRED)
    poll: PollKind | None = field(default=None, metadata=IDENTITY_REQUIRED)
    bucket: str | None = field(default=None, metadata=IDENTITY_REQUIRED)
    label: str | None = field(default=None, metadata=IDENTITY)
    observed_at: datetime = field(factory=now, metadata=IDENTITY_REQUIRED)

    num_votes: int | None = field(default=None, metadata=REQUIRED)


@define(kw_only=True)
class BggRankingItem(ScrapedItem):
    """One game's position in one BGG ranking at one point in time.

    Identity: ``(ranking_type, ranking_id, bgg_id, observed_at)``.

      * ``overall``    — BGG's boardgame rank        (``ranking_id`` None)
      * ``game_type``  — Strategy / Family / War / …  (``ranking_id`` = BGG
                         game-type id, the ``game_type`` ``LinkItem.link_id``)
      * ``hotness``    — the daily Hotness list       (``ranking_id`` None)

    Awards (Spiel des Jahres & co.) are *not* rankings — see the ``AwardItem``
    note in the module docstring.
    """

    ranking_type: RankingType | None = field(default=None, metadata=IDENTITY_REQUIRED)
    ranking_id: str | None = field(default=None, metadata=IDENTITY)
    ranking_name: str | None = None  # "Strategy Game Rank"

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
class BggCollectionItem(ScrapedItem):
    """One entry in one user's BGG collection, as of one scrape.

    Identity: ``(user_name, bgg_id, collection_id, observed_at)``.
    ``collection_id`` (BGG's ``@collid``) is in the key but optional: a real
    collection entry has one, and a user can have several for the same game
    (different editions, a separate "previously owned" entry, …) — those
    coexist because each has its own ``collection_id``. A play-count-only
    aggregate (a game played but never added to the collection) has none, so
    it sets ``collection_id`` to ``None``; that's a distinct, singular slot
    per ``(user_name, bgg_id, observed_at)``. Rating, ownership and play
    count drift over time, so this is an observation, not an edge.
    """

    user_name: str | None = field(
        converter=lower_or_none,
        default=None,
        metadata=IDENTITY_REQUIRED,
    )
    bgg_id: int | None = field(default=None, metadata=IDENTITY_REQUIRED)
    collection_id: int | None = field(default=None, metadata=IDENTITY)
    observed_at: datetime = field(factory=now, metadata=IDENTITY_REQUIRED)

    version_id: int | None = None  # BGG @versionid — the edition, when set

    rating: float | None = None
    owned: bool | None = None
    prev_owned: bool | None = None
    for_trade: bool | None = None
    want_in_trade: bool | None = None
    want_to_play: bool | None = None
    want_to_buy: bool | None = None
    preordered: bool | None = None
    wishlist: int | None = None  # wishlist priority 1-5
    play_count: int | None = None

    comment: str | None = None

    # BGG's own "last modified" on the collection entry — distinct from observed_at.
    updated_at: datetime | None = None
