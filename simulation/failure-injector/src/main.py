from __future__ import annotations
import logging
import os
import random
import signal
import time

from confluent_kafka import Producer

from failure_scenarios import ALL_SCENARIOS

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INTERVAL = int(os.getenv("FAILURE_INTERVAL_SECONDS", "120"))
TOPICS = ["orders", "payments", "inventory", "user-events", "notifications"]

_running = True


def handle_signal(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": 1})

    logger.info("Failure injector started. First failure in %ds", INTERVAL)
    time.sleep(INTERVAL)

    while _running:
        scenario_factory = random.choice(ALL_SCENARIOS)
        try:
            scenario = scenario_factory(producer, TOPICS)
        except TypeError:
            scenario = scenario_factory(producer)

        logger.info("Injecting failure: %s — %s", scenario.name, scenario.description)
        try:
            scenario.apply()
        except Exception:
            logger.exception("Failed to apply scenario %s", scenario.name)

        next_in = INTERVAL + random.randint(-30, 60)
        logger.info("Next failure in %ds", next_in)

        for _ in range(next_in):
            if not _running:
                break
            time.sleep(1)


if __name__ == "__main__":
    main()
