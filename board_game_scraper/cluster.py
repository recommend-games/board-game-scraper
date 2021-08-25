# -*- coding: utf-8 -*-

""" merge different sources """

import argparse
import logging
import math
import os
import re
import sys

from collections import defaultdict
from functools import lru_cache
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse
from pkg_resources import resource_stream

import dedupe
import jmespath
import yaml

from pytility import clear_list, parse_float, parse_int
from scrapy.utils.misc import arg_to_iter, load_object

try:
    # pylint: disable=redefined-builtin
    from smart_open import open
except ImportError:
    pass

from .items import GameItem
from .utils import parse_json, serialize_json

LOGGER = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def abs_comp(field_1, field_2):
    """returns absolute value of difference if both arguments are valid, else inf"""
    field_1 = parse_float(field_1)
    field_2 = parse_float(field_2)
    return math.inf if field_1 is None or field_2 is None else abs(field_1 - field_2)


@lru_cache(8)
def _fields(resource="fields.yaml"):
    LOGGER.info("loading dedupe fields from <%s>", resource)
    fields = yaml.safe_load(resource_stream(__name__, resource))

    for field in fields:
        if field.get("comparator"):
            field["comparator"] = load_object(field["comparator"])

    return fields


DEDUPE_FIELDS = tuple(_fields())

VALUE_ID_REGEX = re.compile(r"^(.*?)(:(\d+))?$")
VALUE_ID_FIELDS = ("designer", "artist", "publisher")


def smart_exists(path, raise_exc=False):
    """returns True if given path exists"""

    url = urlparse(path)

    if url.scheme == "s3":
        try:
            import boto
        except ImportError as exc:
            LOGGER.error("<boto> library must be importable: %s", exc)
            if raise_exc:
                raise exc
            return False

        try:
            bucket = boto.connect_s3().get_bucket(url.hostname, validate=True)
            key = bucket.new_key(url.path[1:])
            return key.exists()

        except Exception as exc:
            LOGGER.error(exc)
            if raise_exc:
                raise exc

        return False

    try:
        return os.path.exists(url.path)

    except Exception as exc:
        LOGGER.error(exc)
        if raise_exc:
            raise exc

    return False


def _parse_value_id(string, regex=VALUE_ID_REGEX):
    match = regex.match(string) if string else None
    if not match or parse_int(match.group(3)) == 3:  # filter out '(Uncredited):3'
        return None
    return match.group(1) or None


def _parse_game(game):
    for field in DEDUPE_FIELDS:
        game.setdefault(field["field"], None)
        if field["type"] == "Set":
            game[field["field"]] = tuple(arg_to_iter(game[field["field"]])) or None
    game["names"] = tuple(
        clear_list(
            chain(arg_to_iter(game.get("name")), arg_to_iter(game.get("alt_name")))
        )
    )
    for field in VALUE_ID_FIELDS:
        game[field] = tuple(
            clear_list(map(_parse_value_id, arg_to_iter(game.get(field))))
        )
    return game


def _load_games(*args):
    for files in args:
        for file in arg_to_iter(files):
            if not file:
                continue

            LOGGER.info("reading from file <%s>", file)

            try:
                with open(file) as file_obj:
                    games = map(parse_json, file_obj)
                    games = filter(None, games)
                    games = map(GameItem.parse, games)
                    games = map(dict, games)
                    games = map(_parse_game, games)
                    yield from games

            except Exception:
                LOGGER.exception("there was an error reading from file <%s>", file)


def _make_id(game, id_field="id", id_prefix=None):
    id_value = game.get(id_field)
    return (
        None if not id_value else f"{id_prefix}:{id_value}" if id_prefix else id_value
    )


def _make_data(games, id_field="id", id_prefix=None):
    return {_make_id(game, id_field, id_prefix): game for game in games}


def _remove_from_training(training, key, keep):
    # training = {"distinct": [(item_1, item_2), ...], "match": [(item_1, item_2), ...]}
    path = jmespath.compile(f"[].{key}")
    keep = frozenset(arg_to_iter(keep))
    LOGGER.info("Keeping %d distinct values in field <%s>", len(keep), key)
    return {
        kind: [
            pair
            for pair in pairs
            if pair and (frozenset(path.search(pair) or ()) <= keep)
        ]
        for kind, pairs in training.items()
    }


def _train_gazetteer(
    data_1,
    data_2,
    fields=DEDUPE_FIELDS,
    training_file=None,
    add_bgg_labels=False,
    manual_labelling=False,
    pretty_print=False,
):
    LOGGER.info("training gazetteer with fields: %r", fields)

    gazetteer = dedupe.Gazetteer(fields)

    if training_file and smart_exists(training_file):
        LOGGER.info("reading existing training from <%s>", training_file)
        with open(training_file) as file_obj:
            gazetteer.prepare_training(
                data_1=data_1,
                data_2=data_2,
                training_file=file_obj,
                sample_size=100_000,
            )
    else:
        gazetteer.prepare_training(data_1=data_1, data_2=data_2, sample_size=100_000)

    if add_bgg_labels:
        labeled_pairs = dedupe.training_data_link(data_1, data_2, common_key="bgg_id")
        labeled_pairs["distinct"] = []
        LOGGER.info(
            "Found %d matches obtained via their BGG ID in total",
            len(labeled_pairs["match"]),
        )
        labeled_pairs = _remove_from_training(
            training=labeled_pairs,
            key="bgg_id",
            keep=(game["bgg_id"] for game in data_1.values() if game.get("bgg_id")),
        )
        LOGGER.info(
            "Adding %d matches obtained via their BGG ID after removing non-existing games",
            len(labeled_pairs["match"]),
        )
        gazetteer.mark_pairs(labeled_pairs)

    if manual_labelling:
        LOGGER.info("start interactive labelling")
        dedupe.console_label(gazetteer)

    if training_file:
        LOGGER.info("write training data back to <%s>", training_file)

        with open(training_file, "w") as file_obj:
            gazetteer.write_training(file_obj)

        if pretty_print:
            with open(training_file) as file_obj:
                training = parse_json(file_obj)
            with open(training_file, "w") as file_obj:
                serialize_json(obj=training, file=file_obj, sort_keys=True, indent=4)

    LOGGER.info("done labelling, begin training")
    gazetteer.index(data_1)
    gazetteer.train(recall=0.9, index_predicates=True)
    gazetteer.cleanup_training()

    return gazetteer


def _extract_site(path):
    path = Path(path)
    return path.stem.split("_")[0] or None


def link_games(
    gazetteer,
    paths,
    id_prefixes=None,
    id_fields=None,
    training_file=None,
    add_bgg_labels=False,
    manual_labelling=False,
    threshold=None,
    output=None,
    pretty_print=True,
):
    """find links for games"""

    paths = tuple(arg_to_iter(paths))
    if len(paths) < 2:
        raise ValueError(f"need at least 2 files to link games, but received {paths}")

    id_prefixes = tuple(arg_to_iter(id_prefixes))
    id_prefixes = id_prefixes + tuple(map(_extract_site, paths[len(id_prefixes) :]))
    id_fields = tuple(arg_to_iter(id_fields))
    id_fields = id_fields + tuple(
        f"{prefix}_id" for prefix in id_prefixes[len(id_fields) :]
    )

    games_canonical = _load_games(paths[0])
    data_canonical = _make_data(games_canonical, id_fields[0], id_prefixes[0])
    del games_canonical

    LOGGER.info(
        "loaded %d games in the canonical dataset <%s>", len(data_canonical), paths[0]
    )

    data_link = {}
    for path, id_field, id_prefix in zip(paths[1:], id_fields[1:], id_prefixes[1:]):
        games = _load_games(path)
        data_link.update(_make_data(games, id_field, id_prefix))
        del games

    LOGGER.info("loaded %d games to link in the datasets %s", len(data_link), paths[1:])

    if training_file:
        gazetteer_trained = _train_gazetteer(
            data_canonical,
            data_link,
            training_file=training_file,
            add_bgg_labels=add_bgg_labels,
            manual_labelling=manual_labelling,
            pretty_print=pretty_print,
        )

        if isinstance(gazetteer, str):
            LOGGER.info("saving gazetteer model to <%s>", gazetteer)
            with open(gazetteer, "wb") as file_obj:
                gazetteer_trained.write_settings(file_obj)

        gazetteer = gazetteer_trained
        del gazetteer_trained

    elif isinstance(gazetteer, str):
        LOGGER.info("reading gazetteer model from <%s>", gazetteer)
        with open(gazetteer, "rb") as file_obj:
            gazetteer = dedupe.StaticGazetteer(file_obj)

    LOGGER.info(gazetteer)

    gazetteer.index(data_canonical)

    LOGGER.info("using gazetteer model %r", gazetteer)

    threshold = threshold or 0.5  # TODO estimate threshold based on recall

    LOGGER.info("using threshold %.3f", threshold)

    clusters = gazetteer.search(
        data=data_link, threshold=threshold, n_matches=None, generator=True
    )
    del gazetteer

    links = defaultdict(set)
    for link_id, canonical_ids in clusters:
        for canonical_id, _ in canonical_ids:
            links[canonical_id].add(link_id)
    del clusters

    LOGGER.info("found links for %d items", len(links))

    if output == "-":
        for id_canonical, linked in links.items():
            LOGGER.info("%s <-> %s", id_canonical, linked)

    elif output:
        LOGGER.info("saving clusters as JSON to <%s>", output)
        links_sorted = {
            key: sorted(value) for key, value in links.items() if key and value
        }
        json_formats = {"sort_keys": True, "indent": 4} if pretty_print else {}
        with open(output, "w") as file_obj:
            serialize_json(obj=links_sorted, file=file_obj, **json_formats)
        del links_sorted

    return links


def _parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "file_canonical", help="input JSON Lines files with canonical dataset"
    )
    parser.add_argument("files_link", nargs="+", help="input JSON Lines files to link")
    parser.add_argument("--id-fields", "-i", nargs="+", help="ID fields")
    parser.add_argument("--id-prefixes", "-I", nargs="+", help="ID prefixes")
    parser.add_argument("--train", "-t", action="store_true", help="train deduper")
    parser.add_argument(
        "--training-file",
        "-T",
        default=os.path.join(BASE_DIR, "cluster", "training.json"),
        help="training JSON file",
    )
    parser.add_argument(
        "--gazetteer-file",
        "-G",
        default=os.path.join(BASE_DIR, "cluster", "gazetteer.pickle"),
        help="gazetteer model file",
    )
    parser.add_argument(
        "--bgg-labels",
        "-b",
        action="store_true",
        help="add labels based on matching BGG IDs",
    )
    parser.add_argument("--threshold", "-r", type=float, help="clustering threshold")
    parser.add_argument("--output", "-o", help="output location")
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    return parser.parse_args()


def _main():
    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)
    LOGGER.info(DEDUPE_FIELDS)

    link_games(
        gazetteer=args.gazetteer_file,
        paths=[args.file_canonical] + args.files_link,
        id_prefixes=args.id_prefixes,
        id_fields=args.id_fields,
        training_file=args.training_file if args.train else None,
        add_bgg_labels=args.bgg_labels,
        manual_labelling=args.train,
        threshold=args.threshold,
        output=args.output or "-",
    )


if __name__ == "__main__":
    _main()
