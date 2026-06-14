from __future__ import annotations
import hashlib
import json
import logging
import os
from datetime import datetime

from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

from agents.shared.postgres_client import get_connection

logger = logging.getLogger(__name__)

CONFIGS_COLLECTION = "kafka_cluster_configs"
EMBEDDING_MODEL = "text-embedding-3-large"


class ClusterConfigSnapshotter:
    def __init__(self, bootstrap_servers: str):
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        self._qdrant = QdrantClient(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )
        self._openai = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        self._last_snapshot: dict | None = None

    def capture(self) -> dict:
        snapshot = self._build_snapshot()
        delta = self._compute_delta(snapshot)
        self._store(snapshot, delta)
        self._last_snapshot = snapshot
        logger.info(
            "Cluster config snapshot captured: %d brokers, %d topics",
            snapshot["broker_count"],
            len(snapshot["topics"]),
        )
        return snapshot

    def _build_snapshot(self) -> dict:
        metadata = self._admin.list_topics(timeout=15)

        brokers = [
            {"broker_id": b.id, "host": b.host, "port": b.port}
            for b in metadata.brokers.values()
        ]

        topics = []
        for topic_name, topic_meta in metadata.topics.items():
            if topic_name.startswith("__"):
                continue
            topics.append({
                "name": topic_name,
                "partition_count": len(topic_meta.partitions),
                "replication_factor": len(list(topic_meta.partitions.values())[0].replicas)
                if topic_meta.partitions else 1,
            })

        return {
            "snapshot_at": datetime.utcnow().isoformat(),
            "broker_count": len(brokers),
            "brokers": brokers,
            "topics": topics,
        }

    def _compute_delta(self, snapshot: dict) -> dict | None:
        if self._last_snapshot is None:
            return None
        delta: dict = {}
        if snapshot["broker_count"] != self._last_snapshot["broker_count"]:
            delta["broker_count"] = {
                "before": self._last_snapshot["broker_count"],
                "after": snapshot["broker_count"],
            }
        prev_topics = {t["name"]: t for t in self._last_snapshot.get("topics", [])}
        curr_topics = {t["name"]: t for t in snapshot.get("topics", [])}
        changed_topics = []
        for name, curr in curr_topics.items():
            prev = prev_topics.get(name)
            if prev and curr != prev:
                changed_topics.append({"topic": name, "before": prev, "after": curr})
        new_topics = [t for t in curr_topics if t not in prev_topics]
        removed_topics = [t for t in prev_topics if t not in curr_topics]
        if changed_topics:
            delta["changed_topics"] = changed_topics
        if new_topics:
            delta["new_topics"] = new_topics
        if removed_topics:
            delta["removed_topics"] = removed_topics
        return delta if delta else None

    def _store(self, snapshot: dict, delta: dict | None) -> None:
        text = json.dumps(snapshot)
        try:
            embedding = self._openai.embeddings.create(
                model=EMBEDDING_MODEL, input=text[:8000]
            ).data[0].embedding
        except Exception:
            logger.exception("Failed to generate embedding for config snapshot")
            return

        point_id = hashlib.md5(snapshot["snapshot_at"].encode()).hexdigest()[:16]
        point_id_int = int(point_id, 16) % (2**63)

        try:
            self._qdrant.upsert(
                collection_name=CONFIGS_COLLECTION,
                points=[
                    PointStruct(
                        id=point_id_int,
                        vector=embedding,
                        payload={
                            "snapshot_at": snapshot["snapshot_at"],
                            "broker_count": snapshot["broker_count"],
                            "topic_count": len(snapshot["topics"]),
                            "has_delta": delta is not None,
                            "delta_summary": json.dumps(delta) if delta else None,
                        },
                    )
                ],
            )
        except Exception:
            logger.exception("Failed to upsert config snapshot to Qdrant")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cluster_config_snapshots
                        (snapshot_at, broker_count, topics, consumer_groups, change_delta, embedding_id)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                    """,
                    (
                        snapshot["snapshot_at"],
                        snapshot["broker_count"],
                        json.dumps(snapshot["topics"]),
                        "[]",
                        json.dumps(delta) if delta else None,
                        str(point_id_int),
                    ),
                )
