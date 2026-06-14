from __future__ import annotations
import logging
import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

app = FastAPI(title="Kafka Operations API", version="1.0.0", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "kafka_ops"),
        user=os.getenv("POSTGRES_USER", "kafka_ops"),
        password=os.getenv("POSTGRES_PASSWORD", "kafka_ops_secret"),
    )


@app.get("/health")
def health():
    return {"status": "healthy", "service": "kafka-ops-api"}


@app.get("/analyses")
def list_analyses(limit: int = 20, severity: str = None):
    """List recent lag analyses, optionally filtered by severity."""
    sql = """
        SELECT analysis_id, triggered_by, analyzed_at, severity,
               overall_confidence, llm_root_cause_summary
        FROM lag_analyses
        {where}
        ORDER BY analyzed_at DESC
        LIMIT %s
    """
    params = []
    where = ""
    if severity:
        where = "WHERE severity = %s"
        params.append(severity.upper())
    params.append(limit)

    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql.format(where=where), params)
            rows = cur.fetchall()
            return [
                {
                    "analysis_id": str(r["analysis_id"]),
                    "triggered_by": r["triggered_by"],
                    "analyzed_at": r["analyzed_at"].isoformat() if r["analyzed_at"] else None,
                    "severity": r["severity"],
                    "confidence": r["overall_confidence"],
                    "summary": r["llm_root_cause_summary"],
                }
                for r in rows
            ]
    finally:
        conn.close()


@app.get("/analyses/{analysis_id}")
def get_analysis(analysis_id: str):
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT * FROM lag_analyses WHERE analysis_id = %s",
                (analysis_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Analysis not found")
            return dict(row)
    finally:
        conn.close()


@app.get("/consumer-groups/lag")
def consumer_group_lag(group_id: str = None, limit: int = 100):
    """Return recent lag snapshots, optionally for a specific consumer group."""
    sql = """
        SELECT group_id, topic, partition, lag, snapshotted_at
        FROM consumer_group_snapshots
        {where}
        ORDER BY snapshotted_at DESC
        LIMIT %s
    """
    params = []
    where = ""
    if group_id:
        where = "WHERE group_id = %s"
        params.append(group_id)
    params.append(limit)

    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql.format(where=where), params)
            rows = cur.fetchall()
            return [
                {
                    "group_id": r["group_id"],
                    "topic": r["topic"],
                    "partition": r["partition"],
                    "lag": r["lag"],
                    "snapshotted_at": r["snapshotted_at"].isoformat(),
                }
                for r in rows
            ]
    finally:
        conn.close()


@app.get("/recommendations")
def list_recommendations(applied: bool = None, limit: int = 50):
    sql = """
        SELECT r.*, a.severity, a.analyzed_at
        FROM scaling_recommendations r
        JOIN lag_analyses a ON r.analysis_id = a.analysis_id
        {where}
        ORDER BY a.analyzed_at DESC
        LIMIT %s
    """
    params = []
    where = ""
    if applied is not None:
        where = "WHERE r.applied = %s"
        params.append(applied)
    params.append(limit)

    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql.format(where=where), params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@app.get("/cluster-configs")
def list_cluster_configs(limit: int = 10):
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT snapshot_at, broker_count,
                       jsonb_array_length(topics) as topic_count,
                       change_delta
                FROM cluster_config_snapshots
                ORDER BY snapshot_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "snapshot_at": r["snapshot_at"].isoformat(),
                    "broker_count": r["broker_count"],
                    "topic_count": r["topic_count"],
                    "has_changes": r["change_delta"] is not None,
                    "change_delta": r["change_delta"],
                }
                for r in cur.fetchall()
            ]
    finally:
        conn.close()


@app.get("/audit")
def audit_log(event_type: str = None, limit: int = 100):
    sql = """
        SELECT event_type, consumer_group, topic, payload, logged_at
        FROM agent_audit_log
        {where}
        ORDER BY logged_at DESC
        LIMIT %s
    """
    params = []
    where = ""
    if event_type:
        where = "WHERE event_type = %s"
        params.append(event_type.upper())
    params.append(limit)

    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql.format(where=where), params)
            return [
                {
                    "event_type": r["event_type"],
                    "consumer_group": r["consumer_group"],
                    "topic": r["topic"],
                    "payload": r["payload"],
                    "logged_at": r["logged_at"].isoformat(),
                }
                for r in cur.fetchall()
            ]
    finally:
        conn.close()
