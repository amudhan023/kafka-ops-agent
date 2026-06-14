from __future__ import annotations
import json
import logging
import os
from typing import Any, Callable

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
    })


def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe(topics)
    return consumer


def make_admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def publish(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    def delivery_report(err, msg):
        if err:
            logger.error("Delivery failed for topic %s: %s", topic, err)

    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, default=str).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


def consume_loop(consumer: Consumer, handler: Callable[[dict], None], poll_timeout: float = 1.0) -> None:
    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                handler(payload)
            except Exception:
                logger.exception("Error handling message from %s", msg.topic())
    finally:
        consumer.close()
