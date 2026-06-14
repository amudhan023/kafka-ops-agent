"""
Kafka Operations Agent — entry point.

Runs two concurrent loops:
  1. Lag/partition polling loop (every LAG_POLL_INTERVAL_SECONDS)
  2. Config snapshot loop (every CONFIG_SNAPSHOT_INTERVAL_SECONDS)

Also exposes a FastAPI health + metrics endpoint.
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
import signal
import uuid
from datetime import datetime

import uvicorn
from fastapi import FastAPI
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from agents.shared import kafka_client, redis_client, postgres_client
from agents.shared.models import KafkaLagAnalysisEvent, Severity
from agents.kafka_ops.src.lag_analyzer import LagAnalyzer
from agents.kafka_ops.src.partition_scanner import PartitionScanner
from agents.kafka_ops.src.kafka_knowledge_retriever import KafkaKnowledgeRetriever
from agents.kafka_ops.src.scaling_recommender import ScalingRecommender
from agents.kafka_ops.src.cluster_config_snapshotter import ClusterConfigSnapshotter


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LAG_POLL_INTERVAL = int(os.getenv("LAG_POLL_INTERVAL_SECONDS", "60"))
PARTITION_SCAN_INTERVAL = int(os.getenv("PARTITION_SCAN_INTERVAL_SECONDS", "300"))
CONFIG_SNAPSHOT_INTERVAL = int(os.getenv("CONFIG_SNAPSHOT_INTERVAL_SECONDS", "21600"))

# Prometheus metrics
analyses_total = Counter("kafka_ops_analyses_total", "Total lag analyses run", ["severity"])
recommendations_total = Counter("kafka_ops_recommendations_total", "Recommendations generated", ["type", "priority"])
consumer_groups_monitored = Gauge("kafka_ops_consumer_groups_monitored", "Number of consumer groups being monitored")
partition_health_gauge = Gauge("kafka_ops_unhealthy_partitions", "Unhealthy partitions", ["kind"])

app = FastAPI(title="Kafka Operations Agent", version="1.0.0")
_shutdown = asyncio.Event()


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "kafka-ops-agent",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/analyses/latest")
async def latest_analyses():
    with postgres_client.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT analysis_id, triggered_by, analyzed_at, severity,
                       overall_confidence, llm_root_cause_summary
                FROM lag_analyses
                ORDER BY analyzed_at DESC
                LIMIT 20
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "analysis_id": str(r[0]),
                    "triggered_by": r[1],
                    "analyzed_at": r[2].isoformat() if r[2] else None,
                    "severity": r[3],
                    "confidence": r[4],
                    "summary": r[5],
                }
                for r in rows
            ]


async def run_lag_analysis_loop() -> None:
    lag_analyzer = LagAnalyzer(BOOTSTRAP)
    partition_scanner = PartitionScanner(BOOTSTRAP)
    retriever = KafkaKnowledgeRetriever()
    recommender = ScalingRecommender()
    producer = kafka_client.make_producer()
    partition_scan_tick = 0

    logger.info("Lag analysis loop started (interval=%ds)", LAG_POLL_INTERVAL)

    while not _shutdown.is_set():
        try:
            await asyncio.sleep(LAG_POLL_INTERVAL)
            if _shutdown.is_set():
                break

            logger.debug("Running lag analysis cycle")
            groups = lag_analyzer.list_consumer_groups()
            consumer_groups_monitored.set(len(groups))

            group_analyses = []
            for group_id in groups:
                try:
                    analysis = lag_analyzer.analyze_group(group_id)
                    group_analyses.append(analysis)
                    for topic in analysis.topics:
                        snapshot_rows = [
                            {
                                "group_id": group_id,
                                "topic": topic.topic,
                                "partition": p.partition,
                                "committed_offset": p.committed_offset,
                                "log_end_offset": p.log_end_offset,
                            }
                            for p in topic.partitions
                        ]
                        if snapshot_rows:
                            postgres_client.insert_consumer_group_snapshot(snapshot_rows)
                except Exception:
                    logger.exception("Failed to analyze group %s", group_id)

            partition_scan_tick += LAG_POLL_INTERVAL
            if partition_scan_tick >= PARTITION_SCAN_INTERVAL:
                partition_health = partition_scanner.scan()
                partition_scan_tick = 0
                partition_health_gauge.labels("under_replicated").set(partition_health.under_replicated)
                partition_health_gauge.labels("offline").set(partition_health.offline)
            else:
                from agents.shared.models import PartitionHealthSummary
                partition_health = PartitionHealthSummary()

            high_severity_groups = [
                ga for ga in group_analyses if ga.severity_score >= 0.4
            ]
            if not high_severity_groups and partition_health.under_replicated == 0 and partition_health.offline == 0:
                continue

            # Collect RAG context for the most critical group
            all_similar_incidents = []
            all_runbook_chunks = []
            if high_severity_groups:
                worst = max(high_severity_groups, key=lambda g: g.severity_score)
                worst_topic = max(worst.topics, key=lambda t: t.severity_score)

                dedup_key = redis_client.dedup_key(
                    worst.group_id,
                    worst_topic.lag_classification.value,
                    worst_topic.topic,
                )
                is_new = redis_client.dedup_check(dedup_key, ttl_seconds=600)
                if not is_new:
                    logger.debug("Skipping duplicate analysis for %s", dedup_key)
                    continue

                all_similar_incidents = retriever.search_similar_incidents(
                    consumer_group=worst.group_id,
                    topic=worst_topic.topic,
                    lag_classification=worst_topic.lag_classification,
                    symptom_description=(
                        f"lag={worst_topic.total_lag} "
                        f"velocity={worst_topic.lag_velocity_per_min:.0f}/min"
                    ),
                )
                all_runbook_chunks = retriever.search_runbooks(
                    lag_classification=worst_topic.lag_classification,
                    context=f"topic={worst_topic.topic}",
                )

            recs, summary, confidence = recommender.generate(
                group_analyses=high_severity_groups,
                partition_health=partition_health,
                similar_incidents=all_similar_incidents,
                runbook_chunks=all_runbook_chunks,
            )

            max_score = max((g.severity_score for g in high_severity_groups), default=0.0)
            severity = LagAnalyzer.severity_from_score(max_score)
            if partition_health.offline > 0 or partition_health.under_replicated > 0:
                severity = Severity.CRITICAL

            event = KafkaLagAnalysisEvent(
                triggered_by="SCHEDULED_POLL",
                consumer_groups=high_severity_groups,
                partition_health=partition_health,
                similar_incidents=all_similar_incidents,
                runbook_references=all_runbook_chunks,
                recommendations=recs,
                llm_root_cause_summary=summary,
                overall_confidence=confidence,
                severity=severity,
            )

            event_dict = event.to_dict()
            kafka_client.publish(
                producer,
                "kafka.ops.analysis.completed",
                key=event.analysis_id,
                value=event_dict,
            )
            if recs:
                kafka_client.publish(
                    producer,
                    "kafka.ops.scaling.recommended",
                    key=event.analysis_id,
                    value=event_dict,
                )

            postgres_client.insert_lag_analysis(event_dict)
            analyses_total.labels(severity=severity.value).inc()
            for rec in recs:
                recommendations_total.labels(
                    type=rec.recommendation_type.value,
                    priority=rec.priority.value,
                ).inc()

            postgres_client.audit(
                event_type="LAG_ANALYSIS_COMPLETED",
                payload={
                    "analysis_id": event.analysis_id,
                    "severity": severity.value,
                    "recommendation_count": len(recs),
                    "confidence": confidence,
                },
            )

            logger.info(
                "Analysis complete: severity=%s groups=%d recs=%d confidence=%.2f",
                severity.value, len(high_severity_groups), len(recs), confidence,
            )

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Unexpected error in lag analysis loop")


async def run_config_snapshot_loop() -> None:
    snapshotter = ClusterConfigSnapshotter(BOOTSTRAP)
    logger.info("Config snapshot loop started (interval=%ds)", CONFIG_SNAPSHOT_INTERVAL)
    while not _shutdown.is_set():
        try:
            snapshotter.capture()
        except Exception:
            logger.exception("Failed to capture cluster config snapshot")
        await asyncio.sleep(CONFIG_SNAPSHOT_INTERVAL)


async def main() -> None:
    loop = asyncio.get_event_loop()

    def handle_signal():
        logger.info("Shutdown signal received")
        _shutdown.set()

    loop.add_signal_handler(signal.SIGINT, handle_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_signal)

    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8207")),
        log_level="warning",
    )
    server = uvicorn.Server(config)

    await asyncio.gather(
        server.serve(),
        run_lag_analysis_loop(),
        run_config_snapshot_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
