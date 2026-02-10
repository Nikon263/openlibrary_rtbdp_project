#!/usr/bin/env python

import logging
from time import sleep
from typing import Dict
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType


def setup_topic(
        topic: str,
        bootstrap_servers: str = "localhost:29092",
        num_partitions: int = 1,
        replication_factor: int = 1,
        retention_ms: int = None,
        compaction: bool = None,
        config: Dict[str, str] = None,
        num_attempts: int = 10
    ) -> bool:

    # Create the Admin Client
    client = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Prepare the topic configuration, injecting retention_ms if specified separately
    # Note: if a retention time is given, we initialize segment.ms to the same time
    # (if missing) as otherwise the default of 7 days applies so we would have to wait
    # that time before seeing compaction in action
    if not config: config = {}
    if compaction: config["cleanup.policy"] = "delete,compact"
    if retention_ms: config["retention.ms"] = str(retention_ms)
    if retention_ms and "segment.ms" not in config: config["segment.ms"] = str(retention_ms)

    # Perform multiple attempts to deal with eventual consistency and concurrent topic creation attempts
    last_ex = None
    for _ in range(num_attempts):
        # The topic may already exist: check if its configuration matches the desired one
        t = client.list_topics().topics.get(topic)
        if t is not None:
            np = len(t.partitions)
            rf = len(t.partitions[0].replicas) if np > 0 else 0
            if num_partitions == np and replication_factor == rf:
                logging.info(f"Found existing topic '{topic}' with {np} partitions and replication factor {rf}")
                c = ConfigResource(ResourceType.TOPIC, topic, config)
                client.alter_configs([c]).get(c).result() # enforce config / async operation, wait for completion
                return False # signal topic already exists
            else:
                client.delete_topics([topic]).get(topic).result()
                logging.info(f"Deleted existing topic '{topic}' with {np} partitions and replication factor {rf}")

        # Create the topic if it was not there or we deleted a conflicting one
        try:
            n = NewTopic(topic=topic, num_partitions=num_partitions, replication_factor=replication_factor, config=config)
            client.create_topics(new_topics=[n]).get(topic).result() # async operation, wait for completion
            logging.info(f"Created new topic '{topic}' with {num_partitions} partitions, replication factor {replication_factor}, config {config}")
            return True
        except KafkaException as ex:
            last_ex = ex
            sleep(1.0) # ignore, wait and retry, topic may be marked for deletion

    # Fail
    raise Exception(f"Cound not create topic {topic} after {num_attempts}: {last_ex}")


def produce_or_block(producer: Producer, **kwargs):
    while True:
        try:
            producer.produce(**kwargs)
            break
        except BufferError:
            producer.flush(1.0)
