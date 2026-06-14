"""
Traffic simulator — produces realistic message loads to Kafka topics.
Simulates business event streams for orders, payments, inventory, and user activity.
"""
from __future__ import annotations
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = os.getenv("SIMULATION_TOPICS", "orders,payments,inventory,user-events,notifications").split(",")
TARGET_RATE = int(os.getenv("PRODUCE_RATE_PER_SECOND", "200"))

messages_produced = Counter("simulator_messages_produced_total", "Messages produced", ["topic"])

_running = True


def handle_signal(sig, frame):
    global _running
    logger.info("Signal %s received, shutting down", sig)
    _running = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def make_event(topic: str) -> dict:
    base = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": topic,
    }
    if topic == "orders":
        base.update({
            "order_id": f"ORD-{random.randint(100000, 999999)}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "total_amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(["PLACED", "CONFIRMED", "SHIPPED"]),
            "items": random.randint(1, 8),
        })
    elif topic == "payments":
        base.update({
            "payment_id": f"PAY-{random.randint(100000, 999999)}",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "currency": "USD",
            "method": random.choice(["card", "bank_transfer", "wallet"]),
            "status": random.choice(["PROCESSING", "COMPLETED", "FAILED"]),
        })
    elif topic == "inventory":
        base.update({
            "product_id": f"PROD-{random.randint(1000, 4999)}",
            "quantity_delta": random.randint(-10, 50),
            "warehouse": random.choice(["US-EAST", "US-WEST", "EU-CENTRAL"]),
        })
    elif topic == "user-events":
        base.update({
            "user_id": f"USR-{random.randint(1, 50000)}",
            "action": random.choice(["login", "view_product", "add_to_cart", "checkout", "logout"]),
            "session_id": str(uuid.uuid4()),
        })
    elif topic == "notifications":
        base.update({
            "notification_id": str(uuid.uuid4()),
            "type": random.choice(["ORDER_CONFIRMATION", "PAYMENT_RECEIPT", "SHIPPING_UPDATE"]),
            "channel": random.choice(["email", "sms", "push"]),
        })
    return base


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)


def main():
    start_http_server(8100)
    logger.info("Metrics available at :8100/metrics")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": 1,
        "linger.ms": 5,
        "batch.num.messages": 1000,
    })

    interval = 1.0 / TARGET_RATE
    logger.info("Starting traffic simulator: %d msg/s across topics %s", TARGET_RATE, TOPICS)

    while _running:
        tick_start = time.monotonic()
        topic = random.choice(TOPICS)
        event = make_event(topic)

        producer.produce(
            topic,
            key=event.get("customer_id", event.get("user_id", str(uuid.uuid4()))).encode(),
            value=json.dumps(event).encode(),
            callback=delivery_report,
        )
        producer.poll(0)
        messages_produced.labels(topic=topic).inc()

        elapsed = time.monotonic() - tick_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    producer.flush(30)
    logger.info("Traffic simulator stopped")


if __name__ == "__main__":
    main()
