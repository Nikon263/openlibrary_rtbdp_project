#!/usr/bin/env python

from time import sleep
from confluent_kafka.admin import AdminClient


def wait_for_kafka_topics(bootstrap_servers, *topics):
    client = AdminClient({ "bootstrap.servers": bootstrap_servers })
    while True:
        existing_topics = client.list_topics().topics.keys()
        if all(topic in existing_topics for topic in topics):
            print(f"Topic(s) <{', '.join(topics)}> are present, continue", flush=True)
            return
        print(f"Topic(s) <{', '.join(topics)}> not present, waiting ...", flush=True)
        sleep(5)
