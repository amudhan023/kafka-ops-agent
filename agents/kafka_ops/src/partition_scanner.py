from __future__ import annotations
import logging
from collections import defaultdict

from confluent_kafka.admin import AdminClient

from agents.shared.models import HotspotDetail, PartitionHealthSummary

logger = logging.getLogger(__name__)

LEADER_IMBALANCE_THRESHOLD = 0.40
HOTSPOT_SKEW_FACTOR = 3.0


class PartitionScanner:
    def __init__(self, bootstrap_servers: str):
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def scan(self) -> PartitionHealthSummary:
        try:
            metadata = self._admin.list_topics(timeout=15)
        except Exception:
            logger.exception("Failed to fetch cluster metadata for partition scan")
            return PartitionHealthSummary()

        summary = PartitionHealthSummary()
        broker_leader_count: dict[int, int] = defaultdict(int)
        partition_offset_rates: dict[tuple[str, int], float] = {}

        for topic_name, topic_meta in metadata.topics.items():
            if topic_name.startswith("__"):
                continue

            for partition_id, partition_meta in topic_meta.partitions.items():
                summary.total_partitions += 1

                if partition_meta.leader == -1:
                    summary.offline += 1
                    logger.warning("Offline partition: %s-%d", topic_name, partition_id)
                    continue

                broker_leader_count[partition_meta.leader] += 1

                if len(partition_meta.isrs) < len(partition_meta.replicas):
                    summary.under_replicated += 1

        total_leaders = sum(broker_leader_count.values())
        if total_leaders > 0 and broker_leader_count:
            max_leaders = max(broker_leader_count.values())
            summary.leader_imbalance_pct = max_leaders / total_leaders
        summary.broker_partition_counts = dict(broker_leader_count)

        hotspots = self._detect_hotspots(metadata)
        summary.hotspot_partitions = hotspots

        if summary.offline > 0 or summary.under_replicated > 0:
            logger.warning(
                "Partition health: offline=%d, under_replicated=%d",
                summary.offline,
                summary.under_replicated,
            )
        if summary.leader_imbalance_pct > LEADER_IMBALANCE_THRESHOLD:
            logger.warning(
                "Leader imbalance detected: %.1f%% of leaders on one broker",
                summary.leader_imbalance_pct * 100,
            )

        return summary

    def _detect_hotspots(self, metadata) -> list[HotspotDetail]:
        hotspots: list[HotspotDetail] = []

        for topic_name, topic_meta in metadata.topics.items():
            if topic_name.startswith("__"):
                continue

            n = len(topic_meta.partitions)
            if n < 2:
                continue

            expected_rate = 1.0 / n
            threshold = expected_rate * HOTSPOT_SKEW_FACTOR

            for partition_id, partition_meta in topic_meta.partitions.items():
                estimated_fraction = 1.0 / n

                if estimated_fraction > threshold:
                    hotspots.append(HotspotDetail(
                        topic=topic_name,
                        partition=partition_id,
                        message_rate=estimated_fraction,
                        skew_factor=estimated_fraction / (1.0 / n),
                    ))

        return hotspots
