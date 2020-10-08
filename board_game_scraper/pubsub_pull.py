# -*- coding: utf-8 -*-

"""Pull logs from Google Cloud PubSub queue."""

import argparse
import csv
import logging
import os
import sys

from itertools import count
from pathlib import Path
from time import sleep

from pytility import normalize_space

try:
    # pylint: disable=redefined-builtin
    from smart_open import open
except ImportError:
    pass

from .utils import now, pubsub_client

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def _process_messages(messages, output, header=False, encoding="utf-8"):
    writer = csv.writer(output)

    if header:
        writer.writerow(("date", "user"))

    for message in messages:
        try:
            date = message.message.publish_time.replace(nanosecond=0).isoformat()
            user = normalize_space(message.message.data.decode(encoding)).lower()
            if date and user:
                writer.writerow((date, user))
                yield message.ack_id
            else:
                LOGGER.error("there was a problem processing message %r", message)

        except Exception:
            LOGGER.exception("unable to process message %r", message)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Pull logs from Google Cloud PubSub queue."
    )
    parser.add_argument(
        "--project",
        "-p",
        default=os.getenv("PULL_QUEUE_PROJECT"),
        help="Google Cloud project",
    )
    parser.add_argument(
        "--subscription",
        "-s",
        default=os.getenv("PULL_QUEUE_SUBSCRIPTION_LOGS"),
        help="Google Cloud PubSub subscription",
    )
    parser.add_argument(
        "--out-path",
        "-o",
        help="Output location",
    )
    parser.add_argument(
        "--header",
        "-H",
        action="store_true",
        help="include CSV header",
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
        "--sleep",
        "-S",
        type=float,
        help="sleep for that many seconds before start pulling",
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
    """Pull logs from Google Cloud PubSub queue."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    if not args.project or not args.subscription:
        LOGGER.error("Google Cloud PubSub project and subscription are required")
        sys.exit(1)

    if args.sleep:
        LOGGER.info("going to sleep for %.1f seconds", args.sleep)
        sleep(args.sleep)

    client = pubsub_client()
    # pylint: disable=no-member
    subscription_path = client.subscription_path(args.project, args.subscription)

    for i in count():
        LOGGER.info("#%d: pulling subscription <%s>", i, subscription_path)

        try:
            response = client.pull(
                subscription=subscription_path,
                max_messages=args.batch_size,
                return_immediately=False,
                timeout=args.timeout,
            )
        except Exception:
            LOGGER.info("subscription <%s> timed out", subscription_path)
            break

        if not response or not response.received_messages:
            LOGGER.info(
                "nothing to be pulled from subscription <%s>", subscription_path
            )
            break

        if not args.out_path or args.out_path == "-":
            ack_ids = tuple(
                _process_messages(
                    messages=response.received_messages,
                    output=sys.stdout,
                    header=args.header and (i == 0),
                )
            )
        else:
            out_path = args.out_path.format(
                number=i, time=now().strftime("%Y-%m-%dT%H-%M-%S")
            )
            LOGGER.info("writing results to <%s>", out_path)
            with open(out_path, "w", newline="") as out_file:
                ack_ids = tuple(
                    _process_messages(
                        messages=response.received_messages,
                        output=out_file,
                        header=args.header,
                    )
                )

        LOGGER.info("%d message(s) succesfully processed", len(ack_ids))

        if ack_ids and not args.no_ack:
            LOGGER.info(
                "acknowledge %d message(s) in subscription <%s>",
                len(ack_ids),
                subscription_path,
            )
            client.acknowledge(subscription=subscription_path, ack_ids=ack_ids)

    LOGGER.info("Done.")


if __name__ == "__main__":
    main()
