# -*- coding: utf-8 -*-

"""TODO."""

import argparse
import csv
import logging
import os
import sys

from pathlib import Path

from pytility import normalize_space

try:
    # pylint: disable=redefined-builtin
    from smart_open import open
except ImportError:
    pass

from .utils import pubsub_client

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def _process_messages(messages, output, encoding="utf-8"):
    writer = csv.writer(output)
    writer.writerow(("date", "user"))
    for message in messages:
        date = message.message.publish_time.replace(nanosecond=0).isoformat()
        user = normalize_space(message.message.data.decode(encoding)).lower()
        writer.writerow((date, user))
        yield message.ack_id


def _parse_args():
    parser = argparse.ArgumentParser(description="TODO.")
    parser.add_argument(
        "--project",
        "-p",
        default=os.getenv("PULL_QUEUE_PROJECT"),
        help="Google Cloud project",
    )
    parser.add_argument(
        "--subscription",
        "-s",
        default=os.getenv("PULL_QUEUE_SUBSCRIPTION"),
        help="Google Cloud PubSub subscription",
    )
    parser.add_argument(
        "--out-file",
        "-o",
        help="Output location",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=1000,
        help="maximum batch size (max messages per pull and file)",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=60,
        help="timeout for a pull in seconds",
    )
    parser.add_argument(
        "--no-ack",
        "-n",
        action="store_true",
        help="do not acknowledge messages to subscription",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    return parser.parse_args()


def main():
    """TODO."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    client = pubsub_client()

    # pylint: disable=no-member
    subscription_path = client.subscription_path(args.project, args.subscription)

    while True:
        response = client.pull(
            subscription=subscription_path,
            max_messages=args.max_messages,
            return_immediately=False,
            timeout=args.timeout,
        )

        if not response or not response.received_messages:
            LOGGER.info(
                "nothing to be pulled from subscription <%s>", subscription_path
            )
            break

        if not args.out_file or args.out_file == "-":
            ack_ids = tuple(_process_messages(response.received_messages, sys.stdout))
        else:
            with open(args.out_file, "w", newline="") as out_file:
                ack_ids = tuple(_process_messages(response.received_messages, out_file))

        LOGGER.info("%d message(s) succesfully processed", len(ack_ids))

        if ack_ids and not args.no_ack:
            client.acknowledge(subscription=subscription_path, ack_ids=ack_ids)

    LOGGER.info("Done.")


if __name__ == "__main__":
    main()
