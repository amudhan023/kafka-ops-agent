-- Kafka Operations Agent — Database Schema

CREATE TABLE IF NOT EXISTS lag_analyses (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analysis_id     UUID NOT NULL UNIQUE,
    triggered_by    VARCHAR(32) NOT NULL,  -- ANOMALY_DETECTED | SCHEDULED_POLL | THRESHOLD_BREACH
    analyzed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumer_groups JSONB NOT NULL DEFAULT '[]',
    partition_health JSONB NOT NULL DEFAULT '{}',
    similar_incidents TEXT[] DEFAULT '{}',
    runbook_references TEXT[] DEFAULT '{}',
    recommendations JSONB NOT NULL DEFAULT '[]',
    llm_root_cause_summary TEXT,
    overall_confidence FLOAT,
    severity        VARCHAR(16),           -- CRITICAL | HIGH | MEDIUM | LOW
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS consumer_group_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    group_id        VARCHAR(255) NOT NULL,
    topic           VARCHAR(255) NOT NULL,
    partition       INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    log_end_offset  BIGINT NOT NULL,
    lag             BIGINT GENERATED ALWAYS AS (log_end_offset - committed_offset) STORED,
    snapshotted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cgs_group_topic_time ON consumer_group_snapshots (group_id, topic, snapshotted_at DESC);
CREATE INDEX idx_cgs_lag ON consumer_group_snapshots (lag) WHERE lag > 0;

CREATE TABLE IF NOT EXISTS cluster_config_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    broker_count    INT NOT NULL,
    topics          JSONB NOT NULL DEFAULT '[]',
    consumer_groups JSONB NOT NULL DEFAULT '[]',
    change_delta    JSONB,
    embedding_id    VARCHAR(255)  -- Qdrant point ID for vector retrieval
);

CREATE TABLE IF NOT EXISTS scaling_recommendations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analysis_id     UUID NOT NULL REFERENCES lag_analyses(analysis_id),
    recommendation_type VARCHAR(64) NOT NULL,
    priority        VARCHAR(32) NOT NULL,
    rationale       TEXT NOT NULL,
    risk_level      VARCHAR(16) NOT NULL,
    grounding       VARCHAR(32) NOT NULL,
    consumer_group  VARCHAR(255),
    topic           VARCHAR(255),
    estimated_resolution_minutes INT,
    applied         BOOLEAN DEFAULT FALSE,
    applied_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(64) NOT NULL,
    consumer_group  VARCHAR(255),
    topic           VARCHAR(255),
    payload         JSONB,
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_event_type ON agent_audit_log (event_type, logged_at DESC);
CREATE INDEX idx_audit_group ON agent_audit_log (consumer_group, logged_at DESC);
