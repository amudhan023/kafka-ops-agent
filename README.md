# Kafka Operations Agent

> An autonomous AI-powered SRE agent that monitors Apache Kafka consumer groups, detects lag anomalies, retrieves grounded context from a vector knowledge base, and generates risk-ranked scaling recommendations using Claude LLM — without human intervention.

<br>

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        CAPABILITY HIGHLIGHTS                                     │
│                                                                                  │
│  Autonomous SRE    ·   RAG-Powered Reasoning   ·   Event-Driven Architecture   │
│  Vector Search     ·   LLM Tool Use (Claude)   ·   Full Observability Stack    │
│  5 Lag Patterns    ·   40 Historical Incidents  ·   Zero Human-in-the-Loop     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Solution Overview](#2-solution-overview)
3. [End-to-End Architecture](#3-end-to-end-architecture)
4. [Agent Workflow](#4-agent-workflow)
5. [Execution Sequence Diagram](#5-execution-sequence-diagram)
6. [AI Agent Internals](#6-ai-agent-internals)
7. [RAG Architecture](#7-rag-architecture)
8. [Vector Store Design](#8-vector-store-design)
9. [Data Flow](#9-data-flow)
10. [Knowledge Base Design](#10-knowledge-base-design)
11. [Reasoning Loop](#11-reasoning-loop)
12. [Agent State Machine](#12-agent-state-machine)
13. [Error Handling & Reliability](#13-error-handling--reliability)
14. [Scalability Architecture](#14-scalability-architecture)
15. [Security Architecture](#15-security-architecture)
16. [Observability Stack](#16-observability-stack)
17. [Project Structure](#17-project-structure)
18. [Deployment Architecture](#18-deployment-architecture)
19. [Example Walkthrough](#19-example-walkthrough)
20. [Technical Highlights](#20-technical-highlights)
21. [Quick Start](#21-quick-start)
22. [Future Roadmap](#22-future-roadmap)

---

## 1. Problem Statement

### The Challenge of Kafka Operations at Scale

Apache Kafka powers mission-critical pipelines at millions of messages per second. When a consumer group stalls, a partition becomes a hotspot, or lag begins growing unbounded, the window to intervene is measured in **minutes — not hours**.

```
Traditional On-Call Response                  AI Agent Response
─────────────────────────────                 ──────────────────
Alert fires at 2 AM          ──── vs ────     Detected at t+0s
Engineer paged               (8-15 min)       Context retrieved at t+2s
Engineer investigates        (10-30 min)      Root cause identified at t+5s
Runbook located              (5-15 min)       Recommendations generated at t+6s
Action taken                 (5-10 min)       Events published at t+7s
Total: 28–70 minutes                          Total: < 10 seconds
```

### Why Traditional Systems Fail

| Problem | Traditional Approach | Failure Mode |
|---|---|---|
| Consumer lag spike | Threshold alert → PagerDuty | Alert fatigue, false positives |
| Partition hotspots | Manual `kafka-topics.sh` inspection | Slow, error-prone |
| Root cause analysis | Engineer reads runbooks | Knowledge varies by engineer |
| Historical context | Tribal knowledge | Lost when engineers leave |
| Scaling decisions | Gut feel + experience | Inconsistent, risky |

### Why an AI Agent Solves This

- **Continuous observation** — polls every 60 seconds, never sleeps
- **Grounded recommendations** — every decision cites a runbook or historical incident
- **Velocity-aware** — detects *rate of change* in lag, not just threshold breaches
- **Pattern recognition** — classifies 5 distinct lag failure modes with distinct remediation paths
- **Confidence scoring** — communicates uncertainty so operators can prioritize review

---

## 2. Solution Overview

```mermaid
flowchart LR
    subgraph Producers["⚡ Event Sources"]
        TS[Traffic Simulator\n200 msg/s]
        FI[Failure Injector\nChaos Scenarios]
        REAL[Business Services\nOrders · Payments · Inventory]
    end

    subgraph Kafka["🗂 Apache Kafka"]
        BT[Business Topics\n5 topics]
        AT[Agent Topics\n6 topics]
    end

    subgraph Agent["🤖 Kafka Ops Agent"]
        LA[Lag Analyzer\nVelocity + Classification]
        PS[Partition Scanner\nHotspot Detection]
        KR[Knowledge Retriever\nRAG Engine]
        SR[Scaling Recommender\nLLM Reasoning]
        CS[Config Snapshotter\n6h Interval]
    end

    subgraph Knowledge["🧠 Knowledge Layer"]
        QD[(Qdrant\nVector Store)]
        INC[40 Historical\nIncidents]
        RB[12 Runbooks\n& SOPs]
    end

    subgraph LLM["✨ AI Layer"]
        CL[Claude Sonnet 4.6\nTool Use]
        OAI[OpenAI\nEmbeddings]
    end

    subgraph Storage["💾 Persistence"]
        PG[(PostgreSQL\n5 tables)]
        RD[(Redis\nDedup Cache)]
    end

    subgraph Observe["📊 Observability"]
        PROM[Prometheus]
        GRAF[Grafana]
        ALERTS[Alert Rules]
    end

    Producers --> Kafka
    Kafka --> Agent
    Agent --> Knowledge
    Agent --> LLM
    Agent --> Storage
    Agent --> Kafka
    PROM --> Agent
    PROM --> GRAF
    PROM --> ALERTS
```

---

## 3. End-to-End Architecture

```mermaid
flowchart TD
    subgraph Simulation["Simulation Layer"]
        TS["Traffic Simulator\n:8100\n200 msg/s across 5 topics"]
        FI["Failure Injector\n3 chaos scenarios\n120s interval"]
    end

    subgraph Kafka["Apache Kafka Cluster"]
        direction LR
        BT["Business Topics\norders · payments\ninventory · user-events\nnotifications"]
        AT["Agent Topics\nkafka.ops.analysis.completed\nkafka.ops.scaling.recommended\nkafka.ops.lag.detected\nagent.audit.log · dlq"]
    end

    subgraph AgentCore["Kafka Operations Agent  :8207"]
        direction TB
        LL["Lag Analysis Loop\nEvery 60 seconds"]
        CL["Config Snapshot Loop\nEvery 6 hours"]

        subgraph LL_Steps["Analysis Pipeline"]
            S1["① List Consumer Groups\nAdminClient.list_groups()"]
            S2["② Analyze Each Group\nVelocity · Classification · Score"]
            S3["③ Scan Partition Health\nUnder-replicated · Offline · Hotspots"]
            S4["④ Redis Dedup Check\n600s TTL per group+classification"]
            S5["⑤ Retrieve Similar Incidents\nQdrant similarity search · k=5"]
            S6["⑥ Retrieve Runbooks\nClassification-filtered · k=3"]
            S7["⑦ Generate Recommendations\nClaude LLM with tool use"]
            S8["⑧ Publish & Persist\nKafka events + PostgreSQL"]
        end

        S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8
    end

    subgraph KnowledgeLayer["Knowledge Layer"]
        QD[("Qdrant Vector DB\n3 collections\nkafka_incidents\nkafka_runbooks\nkafka_cluster_configs")]
        SEED["Knowledge Seeder\nOne-time initialization\n40 incidents · 12 runbooks"]
    end

    subgraph AILayer["AI Layer"]
        CLAUDE["Claude Sonnet 4.6\nTool Use Pattern\nsubmit_recommendations()"]
        OPENAI["OpenAI\ntext-embedding-3-large\nfor snapshots & seeding"]
    end

    subgraph DataLayer["Data Layer"]
        PG[("PostgreSQL\nlag_analyses\nconsumer_group_snapshots\ncluster_config_snapshots\nscaling_recommendations\nagent_audit_log")]
        REDIS[("Redis\nDeduplication\nSET nx=True · TTL 600s")]
    end

    subgraph APILayer["REST API  :8000"]
        EP1["GET /analyses\nGET /analyses/:id"]
        EP2["GET /consumer-groups/lag"]
        EP3["GET /recommendations"]
        EP4["GET /cluster-configs\nGET /audit"]
    end

    subgraph ObservabilityLayer["Observability"]
        PROM["Prometheus :9090\nScrapes agent :8207/metrics\nevery 15 seconds"]
        GRAF["Grafana :3000\nkafka-ops-overview\ndashboard"]
        KEXP["Kafka Exporter :9308\nBroker + topic metrics"]
        ALERTS["Alert Rules\nLagHigh · LagCritical\nUnderReplicated · Offline"]
        KUI["Kafka UI :8080"]
    end

    TS --> Kafka
    FI --> Kafka
    Kafka --> AgentCore
    SEED --> QD
    AgentCore --> QD
    AgentCore --> CLAUDE
    AgentCore --> OPENAI
    AgentCore --> PG
    AgentCore --> REDIS
    AgentCore --> AT
    PG --> APILayer
    PROM --> AgentCore
    PROM --> KEXP
    PROM --> GRAF
    PROM --> ALERTS
    KUI --> Kafka
```

---

## 4. Agent Workflow

```mermaid
flowchart TD
    START([Agent Starts\nScheduled Loop Every 60s]) --> LCG

    LCG["List Active\nConsumer Groups\nAdminClient.list_groups()"]

    LCG --> FEG{Groups\nFound?}
    FEG -- No --> SLEEP([Sleep 60s → Restart])
    FEG -- Yes --> FOREACH

    FOREACH["For Each Consumer Group\nParallel Analysis"]

    FOREACH --> FO["Fetch Partition Offsets\ncommitted vs log_end_offset\nper topic · per partition"]

    FO --> CV["Compute Lag Velocity\nlag_change / elapsed_minutes\nrolling 30-min window from PostgreSQL"]

    CV --> CL["Classify Lag Pattern"]

    CL --> C1["GROWING_LAG\nvelocity > 0 sustained"]
    CL --> C2["STALLED_CONSUMER\nno offset commits > 600s"]
    CL --> C3["CATCHUP_IN_PROGRESS\nvelocity < 0, lag shrinking"]
    CL --> C4["THUNDERING_HERD\nall partitions spiking simultaneously"]
    CL --> C5["SINGLE_PARTITION_STALL\none partition >> 3× avg lag"]
    CL --> C6["HEALTHY\nscore < 0.4"]

    C1 & C2 & C4 & C5 --> SCORE
    C3 & C6 --> SKIP([Skip — No Action Needed])

    SCORE["Compute Severity Score\n0.0 → 1.0\nlag + velocity + classification + partition_health"]

    SCORE --> SCHECK{Score\nThreshold}
    SCHECK -- "< 0.4 LOW" --> SKIP2([Publish LOW, no recommendations])
    SCHECK -- "0.4+ MEDIUM/HIGH" --> DEDUP
    SCHECK -- "0.7+ CRITICAL" --> DEDUP

    DEDUP["Redis Dedup Check\nSET nx=True · TTL 600s\nKey: group:classification:topic"]

    DEDUP -- Duplicate\nwithin 600s --> SKIP3([Skip — Already Processing])
    DEDUP -- New Event --> PS

    PS["Scan Partition Health\nEvery 300s\nunder_replicated · offline · hotspots\nleader_imbalance_pct"]

    PS --> RAG1["Vector Search: Similar Incidents\nEmbed query → Qdrant kafka_incidents\nscore_threshold=0.5 · top_k=5"]

    RAG1 --> RAG2["Vector Search: Runbooks\nFilter by lag_classification\nscore_threshold=0.4 · top_k=3"]

    RAG2 --> LLM["LLM Reasoning\nClaude Sonnet 4.6\nTool Use: submit_recommendations()"]

    LLM --> FB{LLM\nSuccess?}
    FB -- No --> HEURISTIC["Heuristic Fallback\nRule-based recommendations\nby classification + partition state"]
    FB -- Yes --> REC

    HEURISTIC --> REC

    REC["Risk-Ranked Recommendations\ntype · priority · risk_level\ngrounding · cli_commands\nconfidence score"]

    REC --> PUB["Publish Kafka Events\nkafka.ops.analysis.completed\nkafka.ops.scaling.recommended"]

    PUB --> PG["Persist to PostgreSQL\nlag_analyses · consumer_group_snapshots\nscaling_recommendations · agent_audit_log"]

    PG --> MET["Update Prometheus Metrics\nanalyses_total[severity]\nrecommendations_total[type,priority]\nunhealthy_partitions[kind]"]

    MET --> SLEEP2([Sleep Until Next Cycle])
```

---

## 5. Execution Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant TS as Traffic Simulator
    participant FI as Failure Injector
    participant KF as Apache Kafka
    participant AG as Kafka Ops Agent
    participant RD as Redis
    participant QD as Qdrant
    participant OAI as OpenAI Embeddings
    participant CL as Claude LLM
    participant PG as PostgreSQL
    participant PR as Prometheus
    participant AT as Agent Kafka Topics

    TS->>KF: Produce 200 msg/s to business topics
    FI->>KF: Inject failure: consumer_lag_spike (5000 msgs)

    loop Every 60 seconds
        AG->>KF: AdminClient.list_consumer_groups()
        KF-->>AG: [payment-processor, order-fulfillment, ...]

        AG->>KF: list_consumer_group_offsets(each group)
        KF-->>AG: {topic: {partition: {committed, log_end}}}

        AG->>PG: fetch_recent_lag_history(group, topic, 30min)
        PG-->>AG: Historical lag snapshots for velocity calc

        AG->>AG: classify_lag() → GROWING_LAG, score=0.82

        AG->>RD: SET nx=True payment-processor:GROWING_LAG:payments TTL=600
        RD-->>AG: True (new, proceed)

        AG->>KF: describe_topics() for partition health
        KF-->>AG: under_replicated=0, hotspots=[payments-p3]

        AG->>OAI: embed("GROWING_LAG payment-processor payments lag_velocity=450")
        OAI-->>AG: [0.023, -0.441, ...] 3072-dim vector

        AG->>QD: search(kafka_incidents, vector, score_threshold=0.5, top_k=5)
        QD-->>AG: [INC-007, INC-019, INC-034] similar incidents

        AG->>QD: search(kafka_runbooks, vector, filter=GROWING_LAG, top_k=3)
        QD-->>AG: consumer-lag-growing.md § Scaling Consumers

        AG->>CL: call_with_tools(system_prompt, context, [submit_recommendations])
        Note over AG,CL: Context includes: group analysis,<br/>partition health, 3 incidents,<br/>3 runbook sections
        CL-->>AG: tool_use: submit_recommendations({...})

        Note over AG,CL: Claude returns:<br/>root_cause_summary<br/>recommendations[CONSUMER_SCALE_OUT IMMEDIATE]<br/>confidence=0.87

        AG->>AT: publish(kafka.ops.analysis.completed, analysis_event)
        AG->>AT: publish(kafka.ops.scaling.recommended, recommendations)

        AG->>PG: insert_lag_analysis(analysis)
        AG->>PG: insert_consumer_group_snapshot(partitions)
        AG->>PG: insert_scaling_recommendations(recs)
        AG->>PG: audit(ANALYSIS_COMPLETED, payload)

        AG->>PR: kafka_ops_analyses_total{severity="CRITICAL"} += 1
        AG->>PR: kafka_ops_recommendations_total{type="CONSUMER_SCALE_OUT", priority="IMMEDIATE"} += 1
    end

    loop Every 6 hours
        AG->>KF: describe_cluster() + list_topics()
        KF-->>AG: Broker metadata, topic configs
        AG->>OAI: embed(cluster_snapshot_text)
        OAI-->>AG: Embedding vector
        AG->>QD: upsert(kafka_cluster_configs, snapshot + vector)
        AG->>PG: insert_cluster_config_snapshot(snapshot, delta)
    end
```

---

## 6. AI Agent Internals

### Component Responsibilities

```mermaid
flowchart LR
    subgraph AgentComponents["Kafka Ops Agent — Internal Architecture"]
        direction TB

        subgraph Observe["① Observation Layer"]
            LA["LagAnalyzer\n────────────────\nlist_consumer_groups()\nanalyze_group(group_id)\n_fetch_offsets()\n_compute_velocity()\n_classify() → 6 patterns\n_find_problematic_partitions()\n_score() → 0.0–1.0"]
            PS["PartitionScanner\n────────────────\nscan()\n_detect_hotspots()\nleader_imbalance_pct\nunder_replicated\noffline_count"]
        end

        subgraph Memory["② Memory Layer"]
            RD_MEM["Redis\nDedup Cache\n────────────────\ndedup_check(key, ttl)\ndedup_key(group, cls, topic)\nTTL: 600s per event"]
            PG_MEM["PostgreSQL\nPersistent Memory\n────────────────\nfetch_recent_lag_history()\nInserts for every cycle\nAudit trail for all actions"]
        end

        subgraph Retrieval["③ Retrieval Engine"]
            KR["KafkaKnowledgeRetriever\n────────────────\nsearch_similar_incidents()\nsearch_runbooks()\n_embed() via OpenAI\nQdrant: 3 collections\nscore_threshold: 0.4–0.5\ntop_k: 3–5 results"]
        end

        subgraph Planning["④ Reasoning & Planning"]
            SR["ScalingRecommender\n────────────────\ngenerate(analyses, health,\nincidents, runbooks)\nClaude Sonnet 4.6\nTool: submit_recommendations()\nFallback: heuristic rules"]
        end

        subgraph Snapshot["⑤ Continuity Layer"]
            CS["ClusterConfigSnapshotter\n────────────────\ncapture()\n_build_snapshot()\n_compute_delta(prev)\n_store() → Qdrant + PG\nInterval: every 6h"]
        end

        Observe --> Memory
        Memory --> Retrieval
        Retrieval --> Planning
        Observe --> Snapshot
        Snapshot --> Retrieval
    end
```

### Lag Classification Engine

The `LagAnalyzer` implements a deterministic scoring and classification system that maps raw Kafka metrics into semantically meaningful states:

```
Input Signals                  Classification Logic                  Output
──────────────                 ────────────────────                  ──────
committed_offset               velocity > 0 for N cycles             GROWING_LAG
log_end_offset           ───►  no commits for > 600s          ───►   STALLED_CONSUMER
lag history (30 min)           velocity < 0 (recovering)             CATCHUP_IN_PROGRESS
partition_count                all partitions spike together          THUNDERING_HERD
member_count                   one partition >> 3× average           SINGLE_PARTITION_STALL
                               score < 0.4                           HEALTHY
```

**Scoring Formula:**

```
severity_score = f(
    lag_ratio        = min(total_lag / MAX_LAG_REFERENCE, 1.0),
    velocity_factor  = tanh(lag_velocity_per_min / 10_000),
    classification   = {STALLED: +0.3, GROWING: +0.2, THUNDERING_HERD: +0.25, ...},
    partition_health = under_replicated_count × 0.1 + offline_count × 0.2
)

Thresholds:
  LOW      score < 0.4
  MEDIUM   0.4 ≤ score < 0.7
  HIGH     0.7 ≤ score < 0.85   (inferred from CRITICAL boundary)
  CRITICAL score ≥ 0.7
```

### LLM Tool Contract

Claude receives a structured context object and must call exactly one tool:

```json
{
  "name": "submit_recommendations",
  "description": "Submit risk-ranked scaling recommendations",
  "input_schema": {
    "root_cause_summary": "string",
    "confidence": "float (0.0–1.0)",
    "recommendations": [{
      "recommendation_type": "CONSUMER_RESTART | CONSUMER_SCALE_OUT | PARTITION_REBALANCE | TOPIC_PARTITION_INCREASE | BROKER_SCALE | MESSAGE_SKIP | CONFIG_CHANGE | PREFERRED_REPLICA_ELECTION | BROKER_HEALTH_CHECK",
      "priority": "IMMEDIATE | WITHIN_15MIN | WITHIN_1HOUR | ADVISORY",
      "risk_level": "LOW | MEDIUM | HIGH | CRITICAL",
      "grounding": "RUNBOOK | HISTORICAL_INCIDENT | HEURISTIC_ONLY",
      "rationale": "string",
      "consumer_group": "string | null",
      "topic": "string | null",
      "estimated_resolution_minutes": "integer",
      "cli_commands": ["string"]
    }]
  }
}
```

---

## 7. RAG Architecture

```mermaid
flowchart TD
    subgraph Ingestion["Knowledge Ingestion  (One-Time Seeder)"]
        direction LR
        INC_FILES["40 Historical Incidents\nJSON format\nINC-001 → INC-040"]
        RB_FILES["12 Runbook Files\nMarkdown format\nconsumer-lag-growing.md\nhot-partition-mitigation.md\npartition-rebalancing.md\n+ 9 others"]

        INC_FILES --> CHUNK1["Chunk by Incident\n3 chunks per incident:\n· symptoms_description\n· resolution_summary\n· cluster_state_at_onset"]

        RB_FILES --> CHUNK2["Chunk by Section\nSplit on '### ' headings\nExtract lag_classifications\nfrom content keywords"]

        CHUNK1 --> EMB1["OpenAI Embedding\ntext-embedding-3-large\n3072 dimensions"]
        CHUNK2 --> EMB2["OpenAI Embedding\ntext-embedding-3-large\n3072 dimensions"]

        EMB1 --> META1["Enrich Metadata\nincident_id · chunk_type\nlag_classification\nresolution_type\ntime_to_resolution_min\nseverity · summary"]

        EMB2 --> META2["Enrich Metadata\nrunbook_id · title · section\nlag_classifications list\nrelevance_score"]

        META1 --> QDRANT_INC[("Qdrant\nkafka_incidents\n~120 points\n3 chunks × 40 incidents")]
        META2 --> QDRANT_RB[("Qdrant\nkafka_runbooks\n~30–50 points\nN sections × 12 files")]
    end

    subgraph Retrieval["Runtime Retrieval  (Per Analysis Cycle)"]
        direction TB
        QUERY["Analysis Context\nconsumer_group · topic\nlag_classification\nsymptom_description"]

        QUERY --> QEMB["Embed Query\nOpenAI text-embedding-3-large"]

        QEMB --> SEARCH1["Incident Search\nQdrant.search(kafka_incidents)\nscore_threshold=0.5\ntop_k=5\nNo metadata filter"]

        QEMB --> SEARCH2["Runbook Search\nQdrant.search(kafka_runbooks)\nfilter: lag_classification\nscore_threshold=0.4\ntop_k=3"]

        SEARCH1 --> DEDUP["Deduplicate by incident_id\nKeep highest score per incident"]
        SEARCH2 --> RERANK["Pre-filtered by classification\nOrdered by relevance_score"]

        DEDUP --> ASSEMBLE["Context Assembly\nIncidents: id, score, resolution_type,\ntime_to_resolve, summary\nRunbooks: title, section, content excerpt"]

        RERANK --> ASSEMBLE

        ASSEMBLE --> LLM_CTX["LLM Context Window\nSystem: SRE specialist persona\nUser: cluster analysis + retrieved context\nTool: submit_recommendations()"]
    end

    Ingestion --> Retrieval
```

---

## 8. Vector Store Design

### Qdrant Collection Schema

```
Collection: kafka_incidents
─────────────────────────────────────────────────────────────────────────────
Vector:    3072-dim float32  (text-embedding-3-large)
Distance:  Cosine similarity

Payload Schema:
{
  "incident_id":                "INC-007",
  "chunk_type":                 "symptoms_description" | "resolution_summary" | "cluster_state_at_onset",
  "consumer_group":             "payment-processor",
  "topic":                      "payments",
  "lag_classification":         "GROWING_LAG",
  "resolution_type":            "CONSUMER_SCALE_OUT",
  "time_to_resolution_minutes": 23,
  "severity":                   "HIGH",
  "summary":                    "Payment processor fell behind during end-of-month billing..."
}

Index:    lag_classification (keyword, for filter pushdown)


Collection: kafka_runbooks
─────────────────────────────────────────────────────────────────────────────
Vector:    3072-dim float32
Distance:  Cosine similarity

Payload Schema:
{
  "runbook_id":         "consumer-lag-growing",
  "title":              "Consumer Lag Growing Runbook",
  "section":            "Scaling Consumers",
  "content":            "Full markdown section text...",
  "lag_classifications": ["GROWING_LAG", "THUNDERING_HERD"],
  "relevance_score":    0.91
}

Index:    lag_classifications (keyword array, for filter pushdown)


Collection: kafka_cluster_configs
─────────────────────────────────────────────────────────────────────────────
Vector:    3072-dim float32
Distance:  Cosine similarity

Payload Schema:
{
  "snapshot_at":     "2024-01-15T14:30:00Z",
  "broker_count":    3,
  "topics":          {...},
  "consumer_groups": {...},
  "change_delta":    {...}
}
```

### Document Ingestion Pipeline

```mermaid
flowchart LR
    JSON["incidents/*.json\n40 incidents"] --> LOAD["Load & Parse"]
    MD["runbooks/*.md\n12 files"] --> LOAD

    LOAD --> SPLIT["Split by Type\nIncidents: 3 chunks each\nRunbooks: section boundaries"]
    SPLIT --> ENRICH["Metadata Enrichment\nClassification tags\nSeverity · Resolution type"]
    ENRICH --> BATCH["Batch Embedding\nOpenAI API\ntext-embedding-3-large"]
    BATCH --> UPSERT["Qdrant.upsert()\nWith payload + vector"]
    UPSERT --> INDEX["Create Indexes\nFor filter pushdown\non lag_classification"]
```

---

## 9. Data Flow

```mermaid
flowchart TD
    subgraph Input["Input Layer"]
        KF_IN["Kafka Cluster\nActive consumer groups\nPartition offsets\nBroker metadata"]
    end

    subgraph Processing["Processing Layer"]
        FETCH["Offset Fetching\nAdminClient per group\ncommitted vs log_end"]
        VELOCITY["Velocity Calculation\nlag_change / elapsed_min\n30-min rolling window\nfrom PostgreSQL history"]
        CLASSIFY["Pattern Classification\n6 lag states\nDeterministic rules"]
        SCORE["Severity Scoring\n0.0–1.0 composite\nlag + velocity + state + health"]
    end

    subgraph Retrieval["Retrieval Layer"]
        EMBED["Query Embedding\nOpenAI 3072-dim"]
        VEC_SEARCH["Vector Search\nQdrant cosine similarity\nIncidents + Runbooks"]
        CONTEXT["Context Assembly\nTop-K retrieved docs\n+ full analysis state"]
    end

    subgraph Reasoning["Reasoning Layer"]
        LLM["Claude Sonnet 4.6\nStructured tool use\nGPT-style chain-of-thought"]
        FALLBACK["Heuristic Engine\nRule-based fallback\n4 condition branches"]
    end

    subgraph Decision["Decision Layer"]
        RECS["Risk-Ranked\nRecommendations\ntype · priority · risk · grounding"]
        CONF["Confidence Score\n0.0–1.0\nLLM self-assessment"]
    end

    subgraph Action["Action Layer"]
        KF_PUB["Kafka Events\nanalysis.completed\nscaling.recommended"]
        PG_WRITE["PostgreSQL\nlag_analyses\nscaling_recommendations\naudit_log"]
        METRICS["Prometheus\nCounter increments\nGauge updates"]
    end

    subgraph Feedback["Feedback Layer"]
        HISTORY["PostgreSQL History\nFutures velocity calcs\nread on next cycle"]
        DEDUP["Redis Dedup\n600s suppression\nsame group+classification"]
    end

    Input --> Processing
    Processing --> Retrieval
    Retrieval --> Reasoning
    Reasoning --> Decision
    Decision --> Action
    Action --> Feedback
    Feedback --> Processing
```

---

## 10. Knowledge Base Design

### Structure

```
knowledge/
├── incidents/
│   └── historical_incidents.json     # 40 real-world Kafka incidents
│                                       # INC-001 → INC-040
│                                       # Classifications: all 6 lag patterns
│                                       # Resolutions: 9 action types
│                                       # Time-to-resolve: 5–240 minutes
│
└── runbooks/
    ├── consumer-lag-growing.md        # GROWING_LAG mitigation SOP
    ├── hot-partition-mitigation.md    # Hotspot detection & remediation
    ├── partition-rebalancing.md       # Preferred replica election
    ├── broker-leader-rebalance.md     # Leader distribution balancing
    ├── compaction-lag-debug.md        # Log compaction lag debugging
    ├── consumer-group-reset.md        # Offset reset procedures
    ├── consumer-thread-deadlock.md    # Consumer thread stall recovery
    ├── network-partition-recovery.md  # Split-brain recovery
    ├── stalled-consumer-detection.md  # STALLED_CONSUMER playbook
    ├── thundering-herd-response.md    # THUNDERING_HERD response
    ├── topic-partition-increase.md    # Partition scaling procedures
    └── under-replicated-recovery.md  # ISR rebuild procedures
```

### Incident Coverage Matrix

| Classification | Incidents | Resolutions Available |
|---|---|---|
| `GROWING_LAG` | 15 | CONSUMER_SCALE_OUT, CONFIG_CHANGE, TOPIC_PARTITION_INCREASE |
| `STALLED_CONSUMER` | 12 | CONSUMER_RESTART, MESSAGE_SKIP, CONFIG_CHANGE |
| `THUNDERING_HERD` | 6 | CONSUMER_SCALE_OUT, PARTITION_REBALANCE |
| `SINGLE_PARTITION_STALL` | 4 | CONSUMER_RESTART, MESSAGE_SKIP |
| `CATCHUP_IN_PROGRESS` | 3 | ADVISORY only |
| Partition Health | 40 incidents total | BROKER_HEALTH_CHECK, PREFERRED_REPLICA_ELECTION, BROKER_SCALE |

### Knowledge Retrieval Flow

```mermaid
flowchart LR
    SYMPTOM["Symptom\npayment-processor GROWING_LAG\nvelocity=450 msgs/min\nlag=82,000"] --> EMBED_Q["Embed with OpenAI\n3072-dim vector"]

    EMBED_Q --> INC_SEARCH["Search kafka_incidents\nCosine similarity ≥ 0.5\nTop 5 results"]
    EMBED_Q --> RB_SEARCH["Search kafka_runbooks\nFilter: lag_classifications ∋ GROWING_LAG\nCosine similarity ≥ 0.4\nTop 3 results"]

    INC_SEARCH --> INC_RESULTS["INC-007 similarity=0.89\nresolution: CONSUMER_SCALE_OUT\ntime_to_resolve: 23 min\n─────\nINC-019 similarity=0.76\nresolution: CONFIG_CHANGE\ntime_to_resolve: 45 min"]

    RB_SEARCH --> RB_RESULTS["consumer-lag-growing.md\n§ Scaling Consumers\n§ Diagnosing Root Cause\n─────────────────\nhot-partition-mitigation.md\n§ Detection Steps"]

    INC_RESULTS --> LLM_CONTEXT["LLM Context\n(provided to Claude)"]
    RB_RESULTS --> LLM_CONTEXT
```

---

## 11. Reasoning Loop

```mermaid
flowchart LR
    OBS["🔍 OBSERVE\nPoll Kafka offsets\nCompute velocity\nScan partition health\nEvery 60 seconds"]

    THINK["🧠 THINK\nClassify lag pattern\nScore severity 0–1\nCheck Redis dedup\nDecide: act or skip"]

    PLAN["📋 PLAN\nEmbed + retrieve context\nAssemble LLM prompt\nClaude tool-use call\nFallback to heuristics"]

    ACT["⚡ ACT\nPublish Kafka events\nPersist to PostgreSQL\nUpdate metrics\nAudit log"]

    VERIFY["✅ VERIFY\nConfidence score check\nNext cycle velocity\nPostgreSQL history\nDrift detection"]

    LEARN["📚 LEARN\nCluster config snapshot\nQdrant vector upsert\nPG history for velocity\nDedup TTL expires"]

    OBS --> THINK --> PLAN --> ACT --> VERIFY --> LEARN --> OBS
```

The agent follows an **Observe → Think → Plan → Act → Verify → Learn** cycle that repeats every 60 seconds for lag analysis and every 6 hours for knowledge enrichment.

---

## 12. Agent State Machine

```mermaid
stateDiagram-v2
    [*] --> Idle : Agent starts

    Idle --> Observing : Poll timer fires (60s)

    Observing --> Classifying : Offsets fetched successfully
    Observing --> Idle : No consumer groups found
    Observing --> ErrorRecovery : Kafka connection failure

    Classifying --> Deduplicating : Severity score computed
    Classifying --> Idle : All groups HEALTHY

    Deduplicating --> Retrieving : New event (Redis SET nx=True)
    Deduplicating --> Idle : Duplicate (TTL 600s active)

    Retrieving --> Reasoning : Context assembled from Qdrant
    Retrieving --> Reasoning : Qdrant unavailable (empty context)

    Reasoning --> Recommending : Claude tool_use response
    Reasoning --> FallbackReasoning : LLM call fails / timeout

    FallbackReasoning --> Recommending : Heuristic rules applied

    Recommending --> Publishing : Recommendations ranked
    Publishing --> Persisting : Kafka events published

    Persisting --> Idle : PostgreSQL inserts committed
    Persisting --> ErrorRecovery : DB write failure

    ErrorRecovery --> Idle : Retry exhausted (emit dead letter)

    state Observing {
        [*] --> ListGroups
        ListGroups --> FetchOffsets
        FetchOffsets --> FetchHistory
        FetchHistory --> [*]
    }

    state Reasoning {
        [*] --> BuildPrompt
        BuildPrompt --> CallLLM
        CallLLM --> ExtractToolUse
        ExtractToolUse --> [*]
    }
```

---

## 13. Error Handling & Reliability

```mermaid
sequenceDiagram
    participant AG as Agent Loop
    participant KF as Kafka
    participant RD as Redis
    participant QD as Qdrant
    participant CL as Claude LLM
    participant PG as PostgreSQL
    participant DLQ as Dead Letter Queue

    AG->>KF: list_consumer_groups()
    alt Kafka Unavailable
        KF-->>AG: ConnectionError
        AG->>AG: Log warning, increment error counter
        AG->>AG: Sleep 60s → retry next cycle
    end

    AG->>RD: dedup_check(key)
    alt Redis Unavailable
        RD-->>AG: ConnectionError
        AG->>AG: Treat as new event (fail-open)
        Note over AG,RD: Fail-open: prefer duplicate processing<br/>over missing critical alerts
    end

    AG->>QD: search(kafka_incidents, vector)
    alt Qdrant Unavailable
        QD-->>AG: ConnectionError
        AG->>AG: Proceed with empty context
        Note over AG,QD: Recommendations still generated<br/>via heuristic fallback path
    end

    AG->>CL: call_with_tools(system, messages, tools)
    alt LLM Timeout or Error
        CL-->>AG: APIError / Timeout
        AG->>AG: Activate heuristic fallback
        Note over AG,CL: Fallback rules:<br/>STALLED → CONSUMER_RESTART (IMMEDIATE, LOW)<br/>GROWING → CONSUMER_SCALE_OUT (15MIN, MEDIUM)<br/>under_replicated > 0 → BROKER_HEALTH_CHECK (IMMEDIATE, CRITICAL)<br/>imbalance > 40% → PREFERRED_REPLICA_ELECTION (1HOUR, LOW)
        AG->>AG: grounding=HEURISTIC_ONLY, confidence=0.5
    end

    AG->>PG: insert_lag_analysis(analysis)
    alt PostgreSQL Write Failure
        PG-->>AG: OperationalError
        AG->>DLQ: publish(dead.letter.queue, failed_analysis)
        AG->>AG: Log error, increment failure counter
    end
```

### Reliability Properties

| Property | Implementation |
|---|---|
| Deduplication | Redis SET nx=True with 600s TTL prevents duplicate recommendation storms |
| Graceful degradation | LLM failure → heuristic fallback, Qdrant failure → empty context, Redis failure → fail-open |
| Audit trail | Every agent action written to `agent_audit_log` table in PostgreSQL |
| Dead letter queue | `dead.letter.queue` Kafka topic for failed analysis events |
| Idempotency | `analysis_id` UUID generated per cycle; PostgreSQL UPSERT-safe schema |

---

## 14. Scalability Architecture

```mermaid
flowchart TD
    subgraph HorizontalScale["Horizontal Scaling"]
        subgraph AgentPool["Agent Pool (Stateless)"]
            AG1["kafka-ops-agent\nInstance 1\nGroups: A–H"]
            AG2["kafka-ops-agent\nInstance 2\nGroups: I–P"]
            AG3["kafka-ops-agent\nInstance 3\nGroups: Q–Z"]
        end

        subgraph KafkaLayer["Kafka (Partitioned)"]
            KF_AGT["Agent Topics\n3–6 partitions each\nConsumer group per instance"]
        end

        subgraph VectorLayer["Qdrant Cluster"]
            QD1["Qdrant Node 1\nkafka_incidents shard"]
            QD2["Qdrant Node 2\nkafka_runbooks shard"]
            QD3["Qdrant Node 3\nkafka_cluster_configs shard"]
        end

        subgraph DataLayer["PostgreSQL (Read Replicas)"]
            PG_PRI["Primary\nWrites"]
            PG_R1["Replica 1\nAPI reads"]
            PG_R2["Replica 2\nHistory queries"]
        end
    end

    AG1 & AG2 & AG3 --> KafkaLayer
    AG1 & AG2 & AG3 --> VectorLayer
    AG1 & AG2 & AG3 --> DataLayer
```

### Scalability Properties

| Dimension | Current | Scale-Out Path |
|---|---|---|
| Consumer groups monitored | Bounded by 60s loop | Partition groups across agent instances |
| Kafka throughput | 200 msg/s (simulator) | Kafka partition increase + consumer scale-out |
| Vector search latency | Single Qdrant node | Qdrant distributed cluster with sharding |
| LLM concurrency | Sequential per group | Async batch calls with rate limiting |
| API read throughput | Single PostgreSQL | Read replicas + connection pooling |
| Config snapshot storage | Unlimited (Qdrant) | TTL-based eviction of old snapshots |

---

## 15. Security Architecture

```mermaid
flowchart TD
    subgraph External["External Boundary"]
        USER["Operators\nInternal Network"]
        API_GW["API Gateway\nRate Limiting + TLS"]
    end

    subgraph AppLayer["Application Layer"]
        API["Kafka Ops API\n:8000\nRead-only endpoints"]
        AG["Kafka Ops Agent\n:8207\n/health + /metrics only"]
    end

    subgraph Secrets["Secret Management"]
        ENV[".env file\nDocker secrets\nKubernetes Secrets"]
        KEYS["ANTHROPIC_API_KEY\nOPENAI_API_KEY\nPOSTGRES_PASSWORD"]
    end

    subgraph AuthZ["Authorization"]
        PG_AUTH["PostgreSQL\nDedicated DB user\nLeast privilege\nno DROP/CREATE in runtime"]
        KF_AUTH["Kafka\nSASL/SCRAM or mTLS\n(production deployment)"]
        QD_AUTH["Qdrant\nAPI key authentication"]
    end

    subgraph Audit["Audit & Compliance"]
        AUDIT["agent_audit_log table\nEvery agent action logged\nevent_type · payload · timestamp"]
        PROM_LOG["Prometheus metrics\nAll analysis outcomes\nRecommendation types"]
    end

    USER --> API_GW --> AppLayer
    ENV --> KEYS --> AppLayer
    AppLayer --> AuthZ
    AppLayer --> Audit
```

| Security Control | Implementation |
|---|---|
| API keys | Environment variables, never in source code; `.env.example` provided |
| Network isolation | All services on Docker bridge network; only ports explicitly exposed |
| Least privilege | PostgreSQL user scoped to `kafka_ops` DB with DML only |
| Audit logging | All agent decisions written to `agent_audit_log` with full payload |
| Secret rotation | API keys externalized to environment; no restart required for Postgres password rotation |
| Input validation | LLM output parsed via typed Pydantic schema; invalid tool calls rejected |

---

## 16. Observability Stack

```mermaid
flowchart LR
    subgraph Sources["Metric Sources"]
        AG_MET["Kafka Ops Agent\n:8207/metrics\n4 custom metrics\n15s scrape interval"]
        KF_EXP["Kafka Exporter\n:9308\nBroker + topic metrics\nconsumer group lag"]
        SIM_MET["Traffic Simulator\n:8100/metrics\nsimulator_messages_produced_total\nby topic"]
    end

    subgraph Collection["Collection"]
        PROM["Prometheus\n:9090\nTime-series storage\n15s global scrape"]
    end

    subgraph Alerting["Alerting"]
        AR1["KafkaConsumerLagHigh\nlag > 10,000\nduration: 5 min"]
        AR2["KafkaConsumerLagCritical\nlag > 100,000\nduration: 2 min"]
        AR3["KafkaUnderReplicatedPartitions\nany under-replicated\nduration: 1 min"]
        AR4["KafkaOfflinePartitions\nany offline\nduration: 30 sec"]
    end

    subgraph Visualization["Visualization"]
        GRAF["Grafana :3000\nkafka-ops-overview dashboard"]
        P1["Consumer Group Lag\ntimeseries by group/topic"]
        P2["Under-Replicated Count\nstat panel"]
        P3["Offline Partitions\nstat panel"]
        P4["Active Brokers\nstat panel"]
        P5["Messages/sec Rate\nby topic"]
        P6["Consumer Lag Heatmap\npartition-level granularity"]
    end

    Sources --> Collection
    Collection --> Alerting
    Collection --> GRAF
    GRAF --> P1 & P2 & P3 & P4 & P5 & P6
```

### Agent-Emitted Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `kafka_ops_analyses_total` | Counter | `severity` | Total analysis cycles by severity |
| `kafka_ops_recommendations_total` | Counter | `type`, `priority` | Recommendations generated |
| `kafka_ops_consumer_groups_monitored` | Gauge | — | Active groups under observation |
| `kafka_ops_unhealthy_partitions` | Gauge | `kind` | under_replicated or offline count |

---

## 17. Project Structure

```
kafka-ops-agent/
│
├── agents/                             # Agent runtime code
│   ├── kafka_ops/                      # Main agent service
│   │   ├── src/
│   │   │   ├── main.py                 # Entry point: HTTP server + analysis loops
│   │   │   ├── lag_analyzer.py         # Velocity calc, classification, scoring
│   │   │   ├── partition_scanner.py    # Under-replicated, offline, hotspot detection
│   │   │   ├── kafka_knowledge_retriever.py  # RAG engine: Qdrant + OpenAI
│   │   │   ├── scaling_recommender.py  # Claude LLM tool-use for recommendations
│   │   │   └── cluster_config_snapshotter.py # 6h cluster state capture
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   └── shared/                         # Shared client libraries
│       ├── kafka_client.py             # Producer, consumer, admin client factory
│       ├── llm_client.py               # Anthropic SDK wrapper (tool-use + text)
│       ├── models.py                   # All Pydantic models and enums
│       ├── postgres_client.py          # Connection pool, CRUD operations
│       └── redis_client.py             # Dedup cache operations
│
├── application/                        # REST API service
│   └── kafka-ops-api/
│       ├── src/
│       │   └── main.py                 # FastAPI: 6 read-only endpoints
│       ├── requirements.txt
│       └── Dockerfile
│
├── knowledge/                          # Knowledge base for RAG
│   ├── incidents/
│   │   └── historical_incidents.json   # 40 real Kafka incidents (INC-001–INC-040)
│   ├── runbooks/                       # 12 operational runbooks (.md)
│   │   ├── consumer-lag-growing.md
│   │   ├── hot-partition-mitigation.md
│   │   ├── partition-rebalancing.md
│   │   └── ... (9 more runbooks)
│   └── seeder/                         # One-time Qdrant initialization
│       ├── src/main.py                 # Chunks, embeds, and upserts all knowledge
│       ├── requirements.txt
│       └── Dockerfile
│
├── infrastructure/                     # Infrastructure configuration
│   ├── kafka/
│   │   ├── topics.json                 # 11 topic definitions (5 biz + 6 agent)
│   │   └── create-topics.sh            # Topic provisioning script
│   ├── postgres/
│   │   └── init.sql                    # 5-table schema with indexes
│   ├── prometheus/
│   │   ├── prometheus.yml              # Scrape config (3 targets)
│   │   └── alert-rules.yml             # 4 alert rules
│   └── grafana/
│       ├── dashboards/
│       │   └── kafka-ops-overview.json # 6-panel Grafana dashboard
│       └── datasources/
│           └── prometheus.yml          # Grafana datasource config
│
├── simulation/                         # Load generation & chaos testing
│   ├── traffic-simulator/
│   │   ├── src/main.py                 # 200 msg/s across 5 topics
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── failure-injector/
│       ├── src/
│       │   ├── main.py                 # Scheduled failure injection (120s)
│       │   └── failure_scenarios.py    # 3 chaos scenarios
│       ├── requirements.txt
│       └── Dockerfile
│
├── docker-compose.yml                  # Full stack: 15 services
├── Makefile                            # Developer workflows
├── .env.example                        # Required environment variables
└── README.md
```

---

## 18. Deployment Architecture

### Local / Docker Compose

```mermaid
flowchart TD
    subgraph DockerNetwork["Docker Bridge Network: kafka-ops-net"]
        direction TB

        subgraph KafkaStack["Kafka Stack"]
            ZK["Zookeeper\n:2181"]
            KF["Kafka Broker\n:9092 internal\n:29092 external"]
            KUI["Kafka UI\n:8080"]
            KEXP["Kafka Exporter\n:9308"]
        end

        subgraph DataStack["Data Stack"]
            PG["PostgreSQL 15\n:5432\nDB: kafka_ops"]
            RD["Redis 7\n:6379"]
            QD["Qdrant\n:6333 HTTP\n:6334 gRPC"]
        end

        subgraph AgentStack["Agent Stack"]
            SEED["Knowledge Seeder\nRuns once, exits\nInit condition"]
            AG["Kafka Ops Agent\n:8207\nDepends: kafka + pg + redis + qdrant + seeder"]
            API["Kafka Ops API\n:8000\nDepends: postgres"]
        end

        subgraph SimStack["Simulation Stack"]
            TS["Traffic Simulator\n:8100\nDepends: kafka"]
            FI["Failure Injector\nNo exposed port\nDepends: kafka"]
        end

        subgraph ObsStack["Observability Stack"]
            PROM["Prometheus\n:9090"]
            GRAF["Grafana\n:3000"]
        end

        ZK --> KF
        KF --> KUI
        KF --> KEXP
        KF --> SEED
        SEED --> AG
        PG --> AG
        RD --> AG
        QD --> AG
        AG --> API
        KF --> TS
        KF --> FI
        KEXP --> PROM
        AG --> PROM
        TS --> PROM
        PROM --> GRAF
    end
```

### Kubernetes Deployment (Production Target)

```
Namespace: kafka-ops
─────────────────────────────────────────────────────────────────
Deployments:
  kafka-ops-agent       replicas: 2–5 (HPA on CPU + custom metric)
  kafka-ops-api         replicas: 2–3 (HPA on request rate)
  knowledge-seeder      Job (run-once, init container pattern)

StatefulSets:
  kafka                 replicas: 3 (KRaft mode, no Zookeeper)
  qdrant                replicas: 3 (distributed mode)
  postgresql            replicas: 1 primary + 2 replicas

Services:
  kafka-ops-agent       ClusterIP :8207 (metrics scraping)
  kafka-ops-api         LoadBalancer :8000 (external)
  kafka                 ClusterIP :9092
  qdrant                ClusterIP :6333
  postgresql            ClusterIP :5432

ConfigMaps:
  agent-config          LAG_POLL_INTERVAL, PARTITION_SCAN_INTERVAL, etc.

Secrets:
  api-keys              ANTHROPIC_API_KEY, OPENAI_API_KEY
  db-credentials        POSTGRES_PASSWORD

HPA Rules:
  kafka-ops-agent       min=2, max=10, targetCPU=70%, scale on consumer group count
```

---

## 19. Example Walkthrough

**Scenario:** Black Friday traffic causes payment processor consumer group to fall behind by 82,000 messages at a velocity of 450 messages per minute.

```mermaid
sequenceDiagram
    autonumber
    participant FI as Failure Injector
    participant KF as Kafka (payments topic)
    participant AG as Kafka Ops Agent
    participant PG as PostgreSQL
    participant RD as Redis
    participant QD as Qdrant (RAG)
    participant CL as Claude Sonnet 4.6
    participant KAT as kafka.ops.* Topics

    FI->>KF: Produce 5,000 messages to payments (spike scenario)
    Note over FI,KF: Consumer falls behind immediately

    AG->>KF: list_consumer_group_offsets(payment-processor)
    KF-->>AG: payments-p0..p5: lag=[14k, 13k, 14k, 13k, 14k, 14k]

    AG->>PG: fetch_recent_lag_history(payment-processor, payments, 30min)
    PG-->>AG: [{lag: 2000, t: -30m}, {lag: 5000, t: -20m}, ...]

    AG->>AG: compute_velocity() → +450 msgs/min
    AG->>AG: classify() → GROWING_LAG
    AG->>AG: score() → 0.82 (CRITICAL)

    AG->>RD: SET nx=True "payment-processor:GROWING_LAG:payments" EX 600
    RD-->>AG: 1 (new — proceed)

    AG->>KF: describe_topics() partition health check
    KF-->>AG: all partitions replicated, no hotspots

    AG->>QD: search(kafka_incidents, embed("payment-processor GROWING_LAG payments velocity=450"), k=5)
    QD-->>AG: INC-007 (0.89), INC-019 (0.76), INC-034 (0.71)

    AG->>QD: search(kafka_runbooks, filter=GROWING_LAG, k=3)
    QD-->>AG: consumer-lag-growing.md §Scaling Consumers (0.91)

    AG->>CL: call_with_tools(SRE system prompt + full context + submit_recommendations tool)
    Note over AG,CL: Context: lag=82k, velocity=+450/min,<br/>3 similar incidents, runbook excerpts

    CL-->>AG: tool_use: submit_recommendations({<br/>  root_cause_summary: "Payment consumer throughput<br/>  insufficient for Black Friday spike...",<br/>  confidence: 0.87,<br/>  recommendations: [<br/>    {type: CONSUMER_SCALE_OUT,<br/>     priority: IMMEDIATE,<br/>     risk: MEDIUM,<br/>     grounding: HISTORICAL_INCIDENT,<br/>     estimated_resolution_minutes: 23,<br/>     cli_commands: ["kubectl scale deploy/payment-processor --replicas=6"]},<br/>    {type: CONFIG_CHANGE,<br/>     priority: WITHIN_15MIN,<br/>     ...}<br/>  ]<br/>})

    AG->>KAT: publish(kafka.ops.analysis.completed, full_analysis_event)
    AG->>KAT: publish(kafka.ops.scaling.recommended, recommendations)

    AG->>PG: insert_lag_analysis(analysis_id=UUID, severity=CRITICAL, recommendations=[...])
    AG->>PG: insert_scaling_recommendations([CONSUMER_SCALE_OUT IMMEDIATE, CONFIG_CHANGE WITHIN_15MIN])
    AG->>PG: audit(ANALYSIS_COMPLETED, {group: payment-processor, score: 0.82})
```

**Outcome:** Within ~8 seconds of the lag spike being detectable, the agent has published a grounded, confidence-scored recommendation to scale out the payment-processor consumer group — citing INC-007 where the same resolution took 23 minutes to implement and fully resolved the issue.

---

## 20. Technical Highlights

<details>
<summary><strong>Distributed Systems Design</strong></summary>

- **Event-driven decoupling:** Agent communicates results via Kafka topics (`kafka.ops.analysis.completed`, `kafka.ops.scaling.recommended`), enabling downstream consumers without coupling
- **Idempotent processing:** Analysis IDs are UUIDs generated per cycle; PostgreSQL schema is designed for safe re-insertion
- **Distributed deduplication:** Redis-based dedup with 600s TTL prevents recommendation storms across restarts
- **Dead letter queue:** Failed analysis events routed to `dead.letter.queue` for replay

</details>

<details>
<summary><strong>AI Orchestration & LLM Patterns</strong></summary>

- **Structured tool use:** LLM constrained to call exactly one tool (`submit_recommendations`) with a typed JSON schema — outputs are machine-parseable, not free-text
- **Grounding taxonomy:** Every recommendation tagged with `grounding ∈ {RUNBOOK, HISTORICAL_INCIDENT, HEURISTIC_ONLY}` so operators know the evidence quality
- **Graceful LLM fallback:** Deterministic heuristic engine activates on any LLM failure, ensuring recommendations are always produced
- **Confidence propagation:** LLM self-reports confidence (0–1); surfaced in API and events so downstream consumers can threshold-filter

</details>

<details>
<summary><strong>RAG Architecture</strong></summary>

- **Multi-collection retrieval:** Separate Qdrant collections for incidents, runbooks, and cluster configs — different schemas, different retrieval strategies, different score thresholds
- **Metadata filtering:** Runbook search pre-filtered by `lag_classification` in Qdrant payload index, reducing false positives
- **Deduplication on retrieval:** Incident results deduplicated by `incident_id` before LLM context assembly
- **Embedding model selection:** `text-embedding-3-large` (3072-dim) chosen for domain-specific semantic similarity over smaller alternatives

</details>

<details>
<summary><strong>Observability & Reliability Engineering</strong></summary>

- **RED metrics:** Rate (analyses_total), Errors (agent error counter), Duration (implicit in cycle time)
- **Custom Prometheus metrics:** Agent exports 4 typed metrics with meaningful labels; no generic framework metrics
- **Velocity-aware alerting:** Alert rules operate on rates of change, not just static thresholds
- **Full audit trail:** Every agent decision persisted to `agent_audit_log` with payload — enables post-hoc review and model improvement
- **Multi-layer health checks:** Agent `/health` endpoint, Docker health checks, PostgreSQL connectivity validation on startup

</details>

<details>
<summary><strong>Domain Engineering</strong></summary>

- **6-way lag classification:** Pattern recognizer distinguishes GROWING vs STALLED vs THUNDERING_HERD vs SINGLE_PARTITION_STALL — each has a distinct remediation path
- **Composite severity scoring:** Multi-factor scoring function combining lag magnitude, velocity, pattern severity, and partition health — not a simple threshold
- **Velocity calculation:** Rolling 30-minute lag history from PostgreSQL used to compute first derivative of lag — detects trends early, before thresholds are breached
- **40-incident knowledge base:** Curated historical incidents span all 6 lag classifications and 9 resolution types — provides diverse retrieval coverage

</details>

<details>
<summary><strong>Operational Excellence</strong></summary>

- **Repeatable environments:** Full stack in docker-compose with health-check-based startup ordering (seeder → agent → api)
- **Configuration-driven behavior:** Every tunable parameter externalized as environment variable with sensible defaults
- **Chaos engineering:** Built-in failure injector with 3 scenarios (lag spike, single partition stall, thundering herd) for continuous validation
- **Knowledge seeder pattern:** Separate service initializes vector DB once on startup — idempotent, reruns safely

</details>

---

## 21. Quick Start

### Prerequisites

- Docker & Docker Compose
- `ANTHROPIC_API_KEY` (Claude API)
- `OPENAI_API_KEY` (embeddings)

### Setup

```bash
# Clone the repository
git clone https://github.com/amudhan023/kafka-ops-agent
cd kafka-ops-agent

# Configure environment
cp .env.example .env
# Edit .env with your API keys

# Start the full stack
make up

# Or directly with Docker Compose
docker compose up -d
```

### Service Endpoints

| Service | URL | Description |
|---|---|---|
| Kafka Ops Agent | http://localhost:8207/health | Agent health + metrics |
| Kafka Ops API | http://localhost:8000/analyses | Analysis results |
| Kafka UI | http://localhost:8080 | Topic + consumer group browser |
| Grafana | http://localhost:3000 | `kafka-ops-overview` dashboard |
| Prometheus | http://localhost:9090 | Raw metrics + alert status |
| Qdrant UI | http://localhost:6333/dashboard | Vector store browser |

### Watch the Agent Work

```bash
# Tail agent logs
docker compose logs -f kafka-ops-agent

# Trigger a failure scenario immediately
docker compose exec failure-injector python -c "
from failure_scenarios import scenario_consumer_lag_spike
scenario_consumer_lag_spike().apply()
"

# Query latest analyses
curl http://localhost:8000/analyses?limit=5 | jq .

# Query latest recommendations
curl http://localhost:8000/recommendations?limit=10 | jq .

# View Prometheus metrics
curl http://localhost:8207/metrics | grep kafka_ops
```

### Makefile Targets

```bash
make up          # Start all services
make down        # Stop all services
make logs        # Follow agent logs
make ps          # Show service status
make seed        # Re-run knowledge seeder
make clean       # Remove volumes and reset state
```

---

## 22. Future Roadmap

### Near Term

- [ ] **Human-in-the-loop approval gate** — High-risk recommendations (`CRITICAL` risk level) routed to Slack for operator confirmation before publishing
- [ ] **Multi-broker cluster support** — Extend partition scanner to report per-broker load distribution and detect broker-level hotspots
- [ ] **Recommendation feedback loop** — Track whether applied recommendations resolved the issue; feed outcomes back into incident knowledge base
- [ ] **gRPC streaming API** — Real-time analysis event streaming for low-latency operator dashboards

### Medium Term

- [ ] **Multi-cluster support** — Agent monitors multiple Kafka clusters with per-cluster configuration namespacing
- [ ] **LLM evaluation harness** — Automated regression testing of recommendation quality using historical incidents as ground truth
- [ ] **Kubernetes operator** — CRD-based deployment model; agent auto-discovers consumer groups from cluster annotations
- [ ] **Vector store versioning** — Knowledge base versions tied to git tags; rollback support for incident and runbook updates

### Long Term

- [ ] **Active remediation** — Agent executes approved recommendations directly via Kubernetes API and Kafka Admin API (not just publish)
- [ ] **Predictive lag detection** — Time-series forecasting on lag velocity to predict threshold breaches 15–30 minutes in advance
- [ ] **Cross-cluster incident correlation** — Detect cascading failures across multiple Kafka clusters from shared upstream producers
- [ ] **Self-improving knowledge base** — Resolved incidents automatically chunked, embedded, and added to Qdrant after operator confirmation

---

<details>
<summary><strong>Architecture Decision Records</strong></summary>

**ADR-001: Claude Tool Use over Free-Text Parsing**
LLM output constrained to a typed JSON schema via Anthropic tool use API. Avoids fragile regex/JSON parsing of free-text responses. Fallback to heuristics on tool call failure ensures reliability.

**ADR-002: OpenAI Embeddings for RAG, Claude for Reasoning**
`text-embedding-3-large` produces higher-quality semantic embeddings for domain-specific Kafka terminology than Claude's native embeddings. Claude Sonnet 4.6 used only for reasoning where its instruction-following and tool-use capabilities provide value.

**ADR-003: Redis for Deduplication over Kafka Consumer Group Offsets**
600s TTL dedup in Redis is simpler and more reliable than offset-based dedup for time-windowed suppression. Fail-open behavior (Redis down → treat as new) preferred over missing critical alerts.

**ADR-004: PostgreSQL for Persistence over Pure Kafka Log**
Analysis results require complex queries (group by severity, filter by consumer group, time-range scans for velocity). Kafka topic compaction insufficient for these access patterns. PostgreSQL with appropriate indexes provides sub-10ms query latency for the API layer.

**ADR-005: Separate Knowledge Seeder Service**
Embedding generation is expensive and slow. Running it as a one-time init job decouples knowledge base setup from agent startup. Seeder is idempotent and can be re-run to update knowledge without agent downtime.

</details>

---

<br>

```
Built with Apache Kafka · Anthropic Claude · OpenAI Embeddings · Qdrant · PostgreSQL · Redis · Prometheus · Grafana
```
