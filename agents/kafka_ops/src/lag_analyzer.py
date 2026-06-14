from __future__ import annotations
import logging
import math
import os
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition

from agents.shared.models import (
    ConsumerGroupAnalysis,
    LagClassification,
    PartitionLag,
    Severity,
    TopicLagDetail,
)
from agents.shared.postgres_client import fetch_recent_lag_history

logger = logging.getLogger(__name__)

LAG_WARNING_SCORE = float(os.getenv("LAG_WARNING_SCORE", "0.4"))
LAG_CRITICAL_SCORE = float(os.getenv("LAG_CRITICAL_SCORE", "0.7"))
STALL_THRESHOLD_SECONDS = 600
MAX_LAG_REFERENCE = 1_000_000  # normalisation ceiling


class LagAnalyzer:
    def __init__(self, bootstrap_servers: str):
        self._bootstrap = bootstrap_servers
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def list_consumer_groups(self) -> list[str]:
        metadata = self._admin.list_groups(timeout=10)
        return [g.id for g in metadata if g.state != "Dead"]

    def analyze_group(self, group_id: str) -> ConsumerGroupAnalysis:
        offsets = self._fetch_offsets(group_id)
        if not offsets:
            return ConsumerGroupAnalysis(group_id=group_id)

        analysis = ConsumerGroupAnalysis(group_id=group_id)
        all_scores: list[float] = []

        for topic, partitions in offsets.items():
            detail = self._analyze_topic(group_id, topic, partitions)
            analysis.topics.append(detail)
            all_scores.append(detail.severity_score)

        if all_scores:
            analysis.severity_score = max(all_scores)

        return analysis

    def _fetch_offsets(self, group_id: str) -> dict[str, list[PartitionLag]]:
        consumer = Consumer({
            "bootstrap.servers": self._bootstrap,
            "group.id": f"__kafka_ops_inspector_{group_id}",
            "enable.auto.commit": False,
        })
        try:
            cluster_meta = self._admin.list_topics(timeout=10)
            result: dict[str, list[PartitionLag]] = {}

            for topic_name, topic_meta in cluster_meta.topics.items():
                if topic_name.startswith("__") or topic_name.startswith("kafka.ops"):
                    continue

                tps = [TopicPartition(topic_name, p) for p in topic_meta.partitions]

                committed = consumer.committed(tps, timeout=10)
                high_watermarks = {}
                for tp in tps:
                    _, high = consumer.get_watermark_offsets(tp, timeout=5)
                    high_watermarks[tp.partition] = high

                partitions: list[PartitionLag] = []
                for tp in committed:
                    committed_offset = max(tp.offset, 0)
                    log_end = high_watermarks.get(tp.partition, committed_offset)
                    partitions.append(PartitionLag(
                        partition=tp.partition,
                        committed_offset=committed_offset,
                        log_end_offset=log_end,
                        lag=max(0, log_end - committed_offset),
                    ))

                if any(p.lag > 0 for p in partitions):
                    result[topic_name] = partitions

            return result
        except Exception:
            logger.exception("Failed to fetch offsets for group %s", group_id)
            return {}
        finally:
            consumer.close()

    def _analyze_topic(
        self,
        group_id: str,
        topic: str,
        partitions: list[PartitionLag],
    ) -> TopicLagDetail:
        total_lag = sum(p.lag for p in partitions)
        detail = TopicLagDetail(topic=topic, partitions=partitions, total_lag=total_lag)

        if total_lag == 0:
            detail.lag_classification = LagClassification.HEALTHY
            return detail

        history = fetch_recent_lag_history(group_id, topic, minutes=10)
        velocity = self._compute_velocity(total_lag, history)
        detail.lag_velocity_per_min = velocity

        detail.lag_classification = self._classify(partitions, velocity, total_lag)
        detail.problematic_partitions = self._find_problematic_partitions(partitions, total_lag)
        detail.severity_score = self._score(total_lag, velocity, detail.lag_classification)

        return detail

    def _compute_velocity(self, current_lag: int, history: list[dict]) -> float:
        if len(history) < 2:
            return 0.0
        oldest = history[0]
        elapsed_min = (datetime.utcnow().replace(tzinfo=timezone.utc) - oldest["snapshotted_at"]).total_seconds() / 60
        if elapsed_min < 0.5:
            return 0.0
        oldest_total = sum(
            r["lag"] for r in history
            if r["snapshotted_at"] == oldest["snapshotted_at"]
        )
        return (current_lag - oldest_total) / elapsed_min

    def _classify(
        self,
        partitions: list[PartitionLag],
        velocity: float,
        total_lag: int,
    ) -> LagClassification:
        stalled = [p for p in partitions if p.is_stalled]
        if stalled:
            all_stalled = len(stalled) == len(partitions)
            return LagClassification.STALLED if all_stalled else LagClassification.SINGLE_PARTITION_STALL

        non_zero = [p for p in partitions if p.lag > 0]
        if non_zero:
            max_lag = max(p.lag for p in non_zero)
            avg_lag = total_lag / len(non_zero)
            if max_lag > avg_lag * 5 and len(non_zero) < len(partitions):
                return LagClassification.SINGLE_PARTITION_STALL

        if velocity < -100:
            return LagClassification.CATCHUP
        if velocity > 500:
            return LagClassification.GROWING
        if total_lag > 50_000 and abs(velocity) < 50:
            return LagClassification.THUNDERING_HERD

        return LagClassification.GROWING if velocity > 0 else LagClassification.CATCHUP

    def _find_problematic_partitions(self, partitions: list[PartitionLag], total_lag: int) -> list[int]:
        if total_lag == 0:
            return []
        avg_lag = total_lag / max(len(partitions), 1)
        return [p.partition for p in partitions if p.lag > avg_lag * 3]

    def _score(self, total_lag: int, velocity: float, classification: LagClassification) -> float:
        if total_lag == 0:
            return 0.0

        base = math.log10(max(total_lag, 1)) / math.log10(max(MAX_LAG_REFERENCE, 2))

        multipliers = {
            LagClassification.GROWING: 1.5,
            LagClassification.STALLED: 2.0,
            LagClassification.SINGLE_PARTITION_STALL: 1.8,
            LagClassification.THUNDERING_HERD: 1.2,
            LagClassification.CATCHUP: 0.5,
            LagClassification.HEALTHY: 0.0,
        }
        score = base * multipliers.get(classification, 1.0)
        return min(max(score, 0.0), 1.0)

    @staticmethod
    def severity_from_score(score: float) -> Severity:
        if score >= LAG_CRITICAL_SCORE:
            return Severity.CRITICAL
        if score >= LAG_WARNING_SCORE:
            return Severity.HIGH
        if score >= 0.2:
            return Severity.MEDIUM
        return Severity.LOW
