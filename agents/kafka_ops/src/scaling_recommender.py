from __future__ import annotations
import json
import logging

from agents.shared import llm_client
from agents.shared.models import (
    ConsumerGroupAnalysis,
    Grounding,
    LagClassification,
    PartitionHealthSummary,
    Priority,
    RecommendationType,
    RiskLevel,
    RunbookChunk,
    ScalingRecommendation,
    SimilarIncident,
)

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a Kafka operations specialist SRE with deep expertise in consumer group \
lag management, partition tuning, and cluster scaling. You analyze cluster state, historical incidents, \
and runbooks to produce precise, risk-ordered scaling recommendations.

Rules:
- Every recommendation must cite its grounding: RUNBOOK, HISTORICAL_INCIDENT, or HEURISTIC_ONLY.
- Never recommend increasing partition count without flagging the consumer rebalance risk.
- Never recommend offset resets without flagging data loss risk.
- Order recommendations by priority: IMMEDIATE first, then WITHIN_15MIN, WITHIN_1HOUR, ADVISORY.
- Be concise. Each rationale must be one sentence.
"""

TOOLS = [
    {
        "name": "submit_recommendations",
        "description": "Submit the final ranked list of scaling recommendations and root cause summary.",
        "input_schema": {
            "type": "object",
            "properties": {
                "root_cause_summary": {
                    "type": "string",
                    "description": "One paragraph summary of the root cause for the operations team.",
                },
                "recommendations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "recommendation_type": {"type": "string"},
                            "priority": {"type": "string", "enum": ["IMMEDIATE", "WITHIN_15MIN", "WITHIN_1HOUR", "ADVISORY"]},
                            "rationale": {"type": "string"},
                            "risk_level": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
                            "grounding": {"type": "string", "enum": ["RUNBOOK", "HISTORICAL_INCIDENT", "HEURISTIC_ONLY"]},
                            "consumer_group": {"type": "string"},
                            "topic": {"type": "string"},
                            "estimated_resolution_minutes": {"type": "integer"},
                            "cli_commands": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["recommendation_type", "priority", "rationale", "risk_level", "grounding"],
                    },
                },
                "confidence": {
                    "type": "number",
                    "description": "Overall confidence in the analysis (0.0 to 1.0).",
                },
            },
            "required": ["root_cause_summary", "recommendations", "confidence"],
        },
    }
]


def build_context(
    group_analyses: list[ConsumerGroupAnalysis],
    partition_health: PartitionHealthSummary,
    similar_incidents: list[SimilarIncident],
    runbook_chunks: list[RunbookChunk],
) -> str:
    parts: list[str] = []

    parts.append("## Consumer Group Lag Analysis")
    for ga in group_analyses:
        parts.append(f"\n### Group: {ga.group_id} (severity={ga.severity_score:.2f})")
        for td in ga.topics:
            parts.append(
                f"  - topic={td.topic} lag={td.total_lag:,} "
                f"velocity={td.lag_velocity_per_min:+.0f}/min "
                f"classification={td.lag_classification.value}"
            )
            if td.problematic_partitions:
                parts.append(f"    problematic partitions: {td.problematic_partitions}")

    parts.append("\n## Partition Health")
    parts.append(f"  total={partition_health.total_partitions}")
    parts.append(f"  under_replicated={partition_health.under_replicated}")
    parts.append(f"  offline={partition_health.offline}")
    parts.append(f"  leader_imbalance={partition_health.leader_imbalance_pct:.1%}")
    if partition_health.hotspot_partitions:
        parts.append(f"  hotspots: {[(h.topic, h.partition) for h in partition_health.hotspot_partitions]}")

    if similar_incidents:
        parts.append("\n## Similar Historical Incidents (RAG)")
        for si in similar_incidents:
            parts.append(
                f"  - [{si.incident_id}] {si.lag_classification} "
                f"resolved_by={si.resolution_type} in {si.time_to_resolution_minutes}min "
                f"(similarity={si.similarity_score:.2f})"
            )
            if si.summary:
                parts.append(f"    summary: {si.summary}")
    else:
        parts.append("\n## Similar Historical Incidents: none found")

    if runbook_chunks:
        parts.append("\n## Relevant Runbook Sections")
        for rc in runbook_chunks:
            parts.append(f"\n### {rc.title} — {rc.section}")
            parts.append(rc.content[:800])

    return "\n".join(parts)


class ScalingRecommender:
    def generate(
        self,
        group_analyses: list[ConsumerGroupAnalysis],
        partition_health: PartitionHealthSummary,
        similar_incidents: list[SimilarIncident],
        runbook_chunks: list[RunbookChunk],
    ) -> tuple[list[ScalingRecommendation], str, float]:
        context = build_context(group_analyses, partition_health, similar_incidents, runbook_chunks)

        messages = [
            {
                "role": "user",
                "content": (
                    "Analyze the following Kafka cluster state and produce ranked scaling recommendations. "
                    "Use the submit_recommendations tool to return your analysis.\n\n"
                    + context
                ),
            }
        ]

        try:
            response = llm_client.call_with_tools(
                system=SYSTEM_PROMPT,
                messages=messages,
                tools=TOOLS,
            )
        except Exception:
            logger.exception("LLM call failed — falling back to heuristic recommendations")
            return self._heuristic_fallback(group_analyses, partition_health), "", 0.0

        tool_calls = llm_client.extract_tool_use(response)
        if not tool_calls:
            logger.warning("LLM did not call submit_recommendations tool")
            return self._heuristic_fallback(group_analyses, partition_health), "", 0.0

        result = tool_calls[0]["input"]
        recs = []
        for r in result.get("recommendations", []):
            recs.append(ScalingRecommendation(
                recommendation_type=RecommendationType(r.get("recommendation_type", "CONFIG_CHANGE")),
                priority=Priority(r.get("priority", "ADVISORY")),
                rationale=r.get("rationale", ""),
                risk_level=RiskLevel(r.get("risk_level", "MEDIUM")),
                grounding=Grounding(r.get("grounding", "HEURISTIC_ONLY")),
                consumer_group=r.get("consumer_group"),
                topic=r.get("topic"),
                estimated_resolution_minutes=r.get("estimated_resolution_minutes", 15),
                cli_commands=r.get("cli_commands", []),
            ))

        return (
            recs,
            result.get("root_cause_summary", ""),
            float(result.get("confidence", 0.0)),
        )

    def _heuristic_fallback(
        self,
        group_analyses: list[ConsumerGroupAnalysis],
        partition_health: PartitionHealthSummary,
    ) -> list[ScalingRecommendation]:
        recs: list[ScalingRecommendation] = []

        for ga in group_analyses:
            for td in ga.topics:
                if td.lag_classification == LagClassification.STALLED:
                    recs.append(ScalingRecommendation(
                        recommendation_type=RecommendationType.CONSUMER_RESTART,
                        priority=Priority.IMMEDIATE,
                        rationale=f"Consumer group {ga.group_id} has stalled on {td.topic}.",
                        risk_level=RiskLevel.LOW,
                        grounding=Grounding.HEURISTIC_ONLY,
                        consumer_group=ga.group_id,
                        topic=td.topic,
                        estimated_resolution_minutes=5,
                    ))
                elif td.lag_classification == LagClassification.GROWING:
                    recs.append(ScalingRecommendation(
                        recommendation_type=RecommendationType.CONSUMER_SCALE_OUT,
                        priority=Priority.WITHIN_15MIN,
                        rationale=f"Growing lag on {td.topic}; increase consumer instances.",
                        risk_level=RiskLevel.MEDIUM,
                        grounding=Grounding.HEURISTIC_ONLY,
                        consumer_group=ga.group_id,
                        topic=td.topic,
                        estimated_resolution_minutes=15,
                    ))

        if partition_health.under_replicated > 0:
            recs.insert(0, ScalingRecommendation(
                recommendation_type=RecommendationType.BROKER_HEALTH_CHECK,
                priority=Priority.IMMEDIATE,
                rationale=f"{partition_health.under_replicated} under-replicated partitions; check broker disk/network.",
                risk_level=RiskLevel.CRITICAL,
                grounding=Grounding.HEURISTIC_ONLY,
                estimated_resolution_minutes=30,
            ))

        if partition_health.leader_imbalance_pct > 0.4:
            recs.append(ScalingRecommendation(
                recommendation_type=RecommendationType.PREFERRED_REPLICA_ELECTION,
                priority=Priority.WITHIN_1HOUR,
                rationale="Leader imbalance exceeds 40%; trigger preferred replica election.",
                risk_level=RiskLevel.LOW,
                grounding=Grounding.HEURISTIC_ONLY,
                estimated_resolution_minutes=5,
                cli_commands=[
                    "kafka-leader-election --bootstrap-server localhost:9092 "
                    "--election-type PREFERRED --all-topic-partitions"
                ],
            ))

        return recs
