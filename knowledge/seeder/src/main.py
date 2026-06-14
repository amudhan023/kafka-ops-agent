"""
Knowledge seeder — loads runbooks and historical incidents into Qdrant.
Runs once at startup via docker-compose depends_on.
"""
from __future__ import annotations
import glob
import hashlib
import json
import logging
import os
import sys
import time

from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    VectorParams,
)

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 3072

COLLECTIONS = {
    "kafka_incidents": EMBEDDING_DIM,
    "kafka_runbooks": EMBEDDING_DIM,
    "kafka_cluster_configs": EMBEDDING_DIM,
}


def wait_for_qdrant(client: QdrantClient, max_retries: int = 30) -> None:
    for i in range(max_retries):
        try:
            client.get_collections()
            logger.info("Qdrant is ready")
            return
        except Exception:
            logger.info("Waiting for Qdrant... (%d/%d)", i + 1, max_retries)
            time.sleep(3)
    raise RuntimeError("Qdrant not reachable after retries")


def ensure_collections(client: QdrantClient) -> None:
    existing = {c.name for c in client.get_collections().collections}
    for name, dim in COLLECTIONS.items():
        if name not in existing:
            client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )
            logger.info("Created collection: %s", name)
        else:
            logger.info("Collection already exists: %s", name)


def embed_batch(openai_client: OpenAI, texts: list[str]) -> list[list[float]]:
    response = openai_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=[t[:8000] for t in texts],
    )
    return [d.embedding for d in response.data]


def stable_id(text: str) -> int:
    h = hashlib.md5(text.encode()).hexdigest()[:15]
    return int(h, 16) % (2**62)


def seed_runbooks(qdrant: QdrantClient, openai_client: OpenAI, runbooks_path: str) -> None:
    files = glob.glob(os.path.join(runbooks_path, "*.md"))
    logger.info("Seeding %d runbooks", len(files))
    points: list[PointStruct] = []

    for filepath in files:
        with open(filepath) as f:
            content = f.read()

        runbook_id = os.path.basename(filepath).replace(".md", "")
        sections = content.split("\n### ")
        title = sections[0].strip().lstrip("# ")

        lag_classifications = []
        for cls in ["GROWING_LAG", "STALLED_CONSUMER", "SINGLE_PARTITION_STALL",
                    "THUNDERING_HERD", "CATCHUP_IN_PROGRESS"]:
            if cls.lower().replace("_", " ") in content.lower() or cls in content:
                lag_classifications.append(cls)

        for i, section in enumerate(sections):
            chunk_text = f"{title}\n### {section}" if i > 0 else section
            section_name = section.split("\n")[0].strip() if i > 0 else "overview"
            points.append(PointStruct(
                id=stable_id(f"{runbook_id}_{i}"),
                vector=[],  # filled below
                payload={
                    "runbook_id": runbook_id,
                    "title": title,
                    "section": section_name,
                    "content": chunk_text[:1200],
                    "lag_classifications": lag_classifications or ["GROWING_LAG"],
                    "filepath": filepath,
                },
            ))

    if not points:
        return

    texts = [p.payload["content"] for p in points]
    batch_size = 20
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        embeddings = embed_batch(openai_client, batch)
        for point, emb in zip(points[i:i + batch_size], embeddings):
            point.vector = emb

    qdrant.upsert(collection_name="kafka_runbooks", points=points)
    logger.info("Seeded %d runbook chunks", len(points))


def seed_incidents(qdrant: QdrantClient, openai_client: OpenAI, incidents_path: str) -> None:
    files = glob.glob(os.path.join(incidents_path, "*.json"))
    all_incidents = []
    for filepath in files:
        with open(filepath) as f:
            data = json.load(f)
        if isinstance(data, list):
            all_incidents.extend(data)
        else:
            all_incidents.append(data)

    logger.info("Seeding %d historical incidents", len(all_incidents))
    points: list[PointStruct] = []

    for incident in all_incidents:
        iid = incident.get("incident_id", str(len(points)))

        chunks = [
            ("cluster_state", f"Cluster state: {json.dumps(incident.get('cluster_state_at_onset', {}))}"),
            ("symptoms", incident.get("symptoms_description", "")),
            ("resolution", f"Resolution: {incident.get('resolution_type', '')}. {incident.get('resolution_notes', '')}"),
        ]
        for chunk_type, chunk_text in chunks:
            if not chunk_text.strip():
                continue
            points.append(PointStruct(
                id=stable_id(f"{iid}_{chunk_type}"),
                vector=[],
                payload={
                    "incident_id": iid,
                    "chunk_type": chunk_type,
                    "consumer_group": incident.get("consumer_group", ""),
                    "topic": incident.get("topic", ""),
                    "lag_classification": incident.get("lag_classification", ""),
                    "resolution_type": incident.get("resolution_type", ""),
                    "time_to_resolution_minutes": incident.get("time_to_resolution_minutes", 0),
                    "severity": incident.get("severity", ""),
                    "summary": incident.get("summary", ""),
                    "content": chunk_text[:1200],
                },
            ))

    if not points:
        return

    texts = [p.payload["content"] for p in points]
    batch_size = 20
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        embeddings = embed_batch(openai_client, batch)
        for point, emb in zip(points[i:i + batch_size], embeddings):
            point.vector = emb

    qdrant.upsert(collection_name="kafka_incidents", points=points)
    logger.info("Seeded %d incident chunks from %d incidents", len(points), len(all_incidents))


def main():
    openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    wait_for_qdrant(qdrant)
    ensure_collections(qdrant)

    runbooks_path = os.getenv("RUNBOOKS_PATH", "/knowledge/runbooks")
    incidents_path = os.getenv("INCIDENTS_PATH", "/knowledge/incidents")

    seed_runbooks(qdrant, openai_client, runbooks_path)
    seed_incidents(qdrant, openai_client, incidents_path)

    logger.info("Knowledge seeding complete.")


if __name__ == "__main__":
    main()
