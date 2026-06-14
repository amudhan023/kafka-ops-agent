from __future__ import annotations
import logging
import os

from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, ScoredPoint

from agents.shared.models import LagClassification, RunbookChunk, SimilarIncident

logger = logging.getLogger(__name__)

INCIDENTS_COLLECTION = "kafka_incidents"
RUNBOOKS_COLLECTION = "kafka_runbooks"
CONFIGS_COLLECTION = "kafka_cluster_configs"
EMBEDDING_MODEL = "text-embedding-3-large"


class KafkaKnowledgeRetriever:
    def __init__(self):
        self._qdrant = QdrantClient(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )
        self._openai = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    def search_similar_incidents(
        self,
        consumer_group: str,
        topic: str,
        lag_classification: LagClassification,
        symptom_description: str,
        top_k: int = 5,
    ) -> list[SimilarIncident]:
        query = (
            f"{lag_classification.value} consumer group {consumer_group} "
            f"topic {topic} {symptom_description}"
        )
        try:
            embedding = self._embed(query)
            results = self._qdrant.search(
                collection_name=INCIDENTS_COLLECTION,
                query_vector=embedding,
                limit=top_k * 4,
                score_threshold=0.5,
            )
        except Exception:
            logger.exception("Qdrant search failed for incidents")
            return []

        seen: set[str] = set()
        incidents: list[SimilarIncident] = []
        for point in results:
            p = point.payload or {}
            incident_id = p.get("incident_id", point.id)
            if incident_id in seen:
                continue
            seen.add(incident_id)
            incidents.append(SimilarIncident(
                incident_id=str(incident_id),
                similarity_score=point.score,
                lag_classification=p.get("lag_classification", "UNKNOWN"),
                consumer_group=p.get("consumer_group", ""),
                topic=p.get("topic", ""),
                resolution_type=p.get("resolution_type", "UNKNOWN"),
                time_to_resolution_minutes=p.get("time_to_resolution_minutes", 0),
                summary=p.get("summary", ""),
            ))
            if len(incidents) >= top_k:
                break
        return incidents

    def search_runbooks(
        self,
        lag_classification: LagClassification,
        context: str = "",
        top_k: int = 3,
    ) -> list[RunbookChunk]:
        query = f"{lag_classification.value} kafka consumer group runbook {context}"
        try:
            embedding = self._embed(query)

            filter_condition = Filter(
                must=[
                    FieldCondition(
                        key="lag_classifications",
                        match=MatchValue(value=lag_classification.value),
                    )
                ]
            )

            try:
                results = self._qdrant.search(
                    collection_name=RUNBOOKS_COLLECTION,
                    query_vector=embedding,
                    query_filter=filter_condition,
                    limit=top_k * 2,
                    score_threshold=0.4,
                )
            except Exception:
                results = self._qdrant.search(
                    collection_name=RUNBOOKS_COLLECTION,
                    query_vector=embedding,
                    limit=top_k * 2,
                    score_threshold=0.4,
                )
        except Exception:
            logger.exception("Qdrant search failed for runbooks")
            return []

        chunks: list[RunbookChunk] = []
        for point in results[:top_k]:
            p = point.payload or {}
            chunks.append(RunbookChunk(
                runbook_id=p.get("runbook_id", str(point.id)),
                title=p.get("title", ""),
                section=p.get("section", ""),
                content=p.get("content", ""),
                relevance_score=point.score,
            ))
        return chunks

    def _embed(self, text: str) -> list[float]:
        response = self._openai.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text[:8000],
        )
        return response.data[0].embedding
