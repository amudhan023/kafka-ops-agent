"""
Failure scenarios for the Kafka Operations Agent demo.

Each scenario simulates a different class of consumer lag or partition problem
by manipulating consumer groups and message volumes.
"""
from __future__ import annotations
import logging
import random
import time
import uuid
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class FailureScenario:
    name: str
    description: str
    duration_seconds: int
    apply: Callable
    teardown: Callable | None = None


def scenario_consumer_lag_spike(producer, topics: list[str]) -> FailureScenario:
    """Flood a topic with 10× normal volume to create growing lag."""
    def apply():
        import json
        target_topic = random.choice(["orders", "payments"])
        logger.info("[FAILURE] Injecting consumer lag spike on topic=%s", target_topic)
        for _ in range(5000):
            producer.produce(
                target_topic,
                key=f"flood-{uuid.uuid4()}".encode(),
                value=json.dumps({
                    "event_id": str(uuid.uuid4()),
                    "injected": True,
                    "scenario": "LAG_SPIKE",
                }).encode(),
            )
        producer.flush(10)

    return FailureScenario(
        name="CONSUMER_LAG_SPIKE",
        description="Floods orders/payments with 5000 messages to create growing lag",
        duration_seconds=120,
        apply=apply,
    )


def scenario_single_partition_stall(producer) -> FailureScenario:
    """Produce poison-pill style messages to partition 0 of orders."""
    def apply():
        import json
        logger.info("[FAILURE] Injecting single partition stall on orders-p0")
        for i in range(200):
            producer.produce(
                "orders",
                key=b"stall-key-0",
                value=json.dumps({
                    "event_id": str(uuid.uuid4()),
                    "injected": True,
                    "scenario": "SINGLE_PARTITION_STALL",
                    "poison": True,
                }).encode(),
                partition=0,
            )
        producer.flush(10)

    return FailureScenario(
        name="SINGLE_PARTITION_STALL",
        description="Floods partition 0 of orders with messages from a single key",
        duration_seconds=90,
        apply=apply,
    )


def scenario_thundering_herd(producer) -> FailureScenario:
    """Simulate a batch job producing a large burst across all topics."""
    def apply():
        import json
        logger.info("[FAILURE] Injecting thundering herd across all topics")
        for topic in ["orders", "payments", "inventory", "user-events", "notifications"]:
            for _ in range(2000):
                producer.produce(
                    topic,
                    key=f"batch-{uuid.uuid4()}".encode(),
                    value=json.dumps({
                        "event_id": str(uuid.uuid4()),
                        "injected": True,
                        "scenario": "THUNDERING_HERD",
                        "batch_id": "BATCH-001",
                    }).encode(),
                )
        producer.flush(30)

    return FailureScenario(
        name="THUNDERING_HERD",
        description="Batch job floods all topics simultaneously",
        duration_seconds=180,
        apply=apply,
    )


ALL_SCENARIOS = [
    scenario_consumer_lag_spike,
    scenario_single_partition_stall,
    scenario_thundering_herd,
]
