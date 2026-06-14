from __future__ import annotations
import json
import logging
import os
from contextlib import contextmanager
from typing import Any, Generator

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _conn_params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "kafka_ops"),
        "user": os.getenv("POSTGRES_USER", "kafka_ops"),
        "password": os.getenv("POSTGRES_PASSWORD", "kafka_ops_secret"),
    }


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(**_conn_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_lag_analysis(analysis: dict[str, Any]) -> None:
    sql = """
        INSERT INTO lag_analyses
            (analysis_id, triggered_by, analyzed_at, consumer_groups,
             partition_health, similar_incidents, runbook_references,
             recommendations, llm_root_cause_summary, overall_confidence, severity)
        VALUES
            (%(analysis_id)s, %(triggered_by)s, %(analyzed_at)s,
             %(consumer_groups)s::jsonb, %(partition_health)s::jsonb,
             %(similar_incidents)s, %(runbook_references)s,
             %(recommendations)s::jsonb, %(llm_root_cause_summary)s,
             %(overall_confidence)s, %(severity)s)
        ON CONFLICT (analysis_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {
                "analysis_id": analysis["analysis_id"],
                "triggered_by": analysis.get("triggered_by", "SCHEDULED_POLL"),
                "analyzed_at": analysis.get("analyzed_at"),
                "consumer_groups": json.dumps(analysis.get("consumer_groups", []), default=str),
                "partition_health": json.dumps(analysis.get("partition_health") or {}, default=str),
                "similar_incidents": [i["incident_id"] for i in analysis.get("similar_incidents", [])],
                "runbook_references": [r["runbook_id"] for r in analysis.get("runbook_references", [])],
                "recommendations": json.dumps(analysis.get("recommendations", []), default=str),
                "llm_root_cause_summary": analysis.get("llm_root_cause_summary"),
                "overall_confidence": analysis.get("overall_confidence", 0.0),
                "severity": analysis.get("severity", "LOW"),
            })


def insert_consumer_group_snapshot(snapshots: list[dict]) -> None:
    sql = """
        INSERT INTO consumer_group_snapshots
            (group_id, topic, partition, committed_offset, log_end_offset)
        VALUES
            (%(group_id)s, %(topic)s, %(partition)s, %(committed_offset)s, %(log_end_offset)s)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, snapshots)


def audit(event_type: str, payload: dict, consumer_group: str = None, topic: str = None) -> None:
    sql = """
        INSERT INTO agent_audit_log (event_type, consumer_group, topic, payload)
        VALUES (%s, %s, %s, %s::jsonb)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (event_type, consumer_group, topic, json.dumps(payload, default=str)))


def fetch_recent_lag_history(group_id: str, topic: str, minutes: int = 30) -> list[dict]:
    sql = """
        SELECT partition, lag, snapshotted_at
        FROM consumer_group_snapshots
        WHERE group_id = %s AND topic = %s
          AND snapshotted_at > NOW() - INTERVAL '%s minutes'
        ORDER BY snapshotted_at ASC
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, (group_id, topic, minutes))
            return [dict(row) for row in cur.fetchall()]
