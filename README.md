# Kafka Operations Agent

An AI-powered autonomous agent for Kafka cluster operations — analyzing consumer lag, detecting problematic partitions, retrieving similar historical issues from a vector store, and generating grounded scaling recommendations.

## Quick Start

```bash
cp .env.example .env
# Add ANTHROPIC_API_KEY and OPENAI_API_KEY to .env

make demo
```

Demo completes setup in ~90 seconds, then begins monitoring immediately.

## What It Does

**Proactive (every 60s):** Polls all consumer groups for lag. Classifies each group's lag pattern. When thresholds are breached, retrieves similar historical incidents + relevant runbooks from Qdrant, calls Claude for synthesis, and publishes a grounded recommendation.

**Reactive:** Responds to failure injection scenarios (consumer lag spikes, single partition stalls, thundering herd) from the built-in simulator.

## Lag Classifications

| Classification | Meaning | Default Action |
|---|---|---|
| `GROWING_LAG` | Velocity positive and sustained | Consumer scale-out or partition increase |
| `STALLED_CONSUMER` | No offset commits for >10 minutes | Consumer restart or message skip |
| `SINGLE_PARTITION_STALL` | One partition lagging, others healthy | Inspect for poison pill message |
| `THUNDERING_HERD` | All groups lag simultaneously | Wait and monitor (transient) |
| `CATCHUP_IN_PROGRESS` | Lag decreasing, velocity negative | No action needed |

## Access Points

| Service | URL |
|---|---|
| **API (Swagger)** | http://localhost:8000/docs |
| **Agent health** | http://localhost:8207/health |
| **Kafka UI** | http://localhost:8080 |
| **Grafana** | http://localhost:3000 (admin/admin) |
| **Prometheus** | http://localhost:9090 |
| **Qdrant** | http://localhost:6333/dashboard |

## API Endpoints

```
GET /analyses              - Recent lag analyses (filter by severity)
GET /analyses/{id}         - Full analysis detail
GET /consumer-groups/lag   - Raw lag snapshots
GET /recommendations       - Generated scaling recommendations
GET /cluster-configs       - Cluster configuration history
GET /audit                 - Agent audit log
```

## Architecture

```
Simulation (traffic-simulator + failure-injector)
    ↓ produces to
Kafka (orders, payments, inventory, user-events, notifications)
    ↓ read by
Kafka Operations Agent
    ├── LagAnalyzer          — classifies consumer group lag
    ├── PartitionScanner     — detects partition health issues
    ├── KafkaKnowledgeRetriever — searches Qdrant (40 incidents + 12 runbooks)
    └── ScalingRecommender   — calls Claude claude-sonnet-4-6, returns grounded recs
    ↓ publishes to
kafka.ops.analysis.completed → Postgres + Kafka
```

## Vector Store Collections (Qdrant)

| Collection | Contents |
|---|---|
| `kafka_incidents` | 40 historical incidents, 3 chunks each (state, symptoms, resolution) |
| `kafka_runbooks` | 12 runbooks, chunked by section |
| `kafka_cluster_configs` | Runtime cluster snapshots (captured every 6h) |

## Configuration

All tuneable via environment variables in `.env`:

```
LAG_POLL_INTERVAL_SECONDS=60        # How often to poll consumer groups
PARTITION_SCAN_INTERVAL_SECONDS=300  # How often to scan partition health
CONFIG_SNAPSHOT_INTERVAL_SECONDS=21600  # How often to snapshot cluster config (6h)
LAG_WARNING_SCORE=0.4                # Score threshold for HIGH severity
LAG_CRITICAL_SCORE=0.7              # Score threshold for CRITICAL severity
```

## Project Structure

```
kafka-ops-agent/
├── agents/
│   ├── shared/              # Kafka, Postgres, Redis, LLM clients + Pydantic models
│   └── kafka-ops/           # Main agent: lag_analyzer, partition_scanner,
│                            #   kafka_knowledge_retriever, scaling_recommender,
│                            #   cluster_config_snapshotter
├── knowledge/
│   ├── runbooks/            # 12 Kafka operational runbooks
│   ├── incidents/           # 40 historical incident fixtures (seed data)
│   └── seeder/              # One-shot Qdrant population job
├── simulation/
│   ├── traffic-simulator/   # Produces realistic Kafka load
│   └── failure-injector/    # Injects lag spikes, partition stalls, thundering herds
├── application/
│   └── kafka-ops-api/       # FastAPI REST API for analysis results
└── infrastructure/
    ├── kafka/               # Topic definitions + creation script
    ├── prometheus/          # Scrape config + alert rules
    ├── grafana/             # Dashboards + datasources
    └── postgres/            # Schema migrations
```
