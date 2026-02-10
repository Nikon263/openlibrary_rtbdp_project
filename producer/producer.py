#!/usr/bin/env python

import os
import sys
import logging
# import re
import time
import requests
from argparse import ArgumentParser
# from websocket import WebSocketApp
from confluent_kafka import Producer
from common import setup_topic
from json import loads, dumps


def del_rep(err, msg):
    if err is not None:
        logging.error(f"Delivery has failed: {err}")


def main():

    # Parse command line options (call program with --help to see supported options)
    parser = ArgumentParser(description='Kafka producer for JSON messages')
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="bootstrap servers", type=str)
    parser.add_argument('--url', help="URL", required=True, type=str)
    parser.add_argument('--topic', default='ol.recentchanges', help='topic name', type=str)
    parser.add_argument('--num-partitions', default=1, help='# partitions (default 1)', type=int)
    parser.add_argument('--replication-factor', default=1, help='replication factor (default 1)', type=int)
    parser.add_argument("--dry-run", help="print to stdout instead of writing to Kafka", action="store_true")
    parser.add_argument("--log-level", dest="log_level", default="debug", type=str, help="log level (debug*|info|warn|error|fatal|critical, *=default")
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--offset-file", default="/tmp/ol_last_ts.txt")
    args = parser.parse_args()

    # Configure logging (log to stdout if it is a terminal, otherwise stderr so to allow feeding messages to downstream program with --dry-run)
    logging.basicConfig(
        level=logging.getLevelName(args.log_level.upper()),
        format="%(asctime)s (%(levelname).1s) %(message)s [%(threadName)s]",
        stream=sys.stdout if sys.stdout.isatty() else sys.stderr,
    )

    # Setup topic(s) and create Kafka Producer (if not in dry_run mode)
    producer = None
    if not args.dry_run:
        # Non-dry-run mode: emit to Kafka topic using a Kafka producer (both initialized here)
        setup_topic(
            args.topic,
            bootstrap_servers=args.bootstrap_servers,
            retention_ms=86400000,
            num_partitions=args.num_partitions,
            replication_factor=args.replication_factor,
        )

        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "compression.type": "gzip",
            "acks": "1",
            "linger.ms": "100", # allow some batching
            "on_delivery": del_rep,
        })

    try:
        last_ts = None
        if os.path.exists(args.offset_file):
            with open(args.offset_file, "r") as f:
                last_ts = int(f.read().strip())
        logging.info(f"Starting producer at {args.url}, last_ts={last_ts}")

        while True:
            params = {"since": last_ts} if last_ts else {}

            r = requests.get(args.url, params=params, timeout=20)
            r.raise_for_status()
            changes = r.json()

            sent = 0

            for change in changes:
                if not isinstance(change, dict):
                    continue

                changes_list = change.get("changes", [])

                entities = set()
                for c in changes_list:
                    key = c.get("key", "")
                    if key.startswith("/authors/"):
                        entities.add("author")
                    elif key.startswith("/works/"):
                        entities.add("work")
                    elif key.startswith("/books/"):
                        entities.add("book")

                event = {
                    "event_id": change.get("id"),
                    "timestamp": change.get("timestamp"),
                    "kind": change.get("kind"),
                    "actor_type": "user" if change.get("author") else "anonymous" if change.get("ip") else "system",
                    "entity_count": len(changes),
                    "entity_types": list(entities),
                    "has_comment": bool(change.get("comment")),
                    "raw": change,
                }

                ts = event.get("timestamp")
                if ts:
                    last_ts = ts

                if args.dry_run:
                    print(dumps(event))
                else:
                    producer.produce(args.topic, key=event.get("kind"), value=dumps(change))
                sent += 1

            if sent > 0 and not args.dry_run:
                producer.flush()
                with open(args.offset_file, "w") as f:
                    f.write(last_ts)
                logging.info(f"Sent {sent} changes (last_ts={last_ts})")

            time.sleep(args.poll_interval)

    except Exception as e:
        logging.error(f"Error: {e}")


    finally:
        # Flush producer on non-dry-run mode
        if not args.dry_run: producer.flush(3.0)
        logging.info("Stopped")


if __name__ == "__main__":
    main()
