from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import uuid


class LagClassification(str, Enum):
    GROWING = "GROWING_LAG"
    STALLED = "STALLED_CONSUMER"
    CATCHUP = "CATCHUP_IN_PROGRESS"
    THUNDERING_HERD = "THUNDERING_HERD"
    SINGLE_PARTITION_STALL = "SINGLE_PARTITION_STALL"
    HEALTHY = "HEALTHY"


class RecommendationType(str, Enum):
    CONSUMER_RESTART = "CONSUMER_RESTART"
    CONSUMER_SCALE_OUT = "CONSUMER_SCALE_OUT"
    PARTITION_REBALANCE = "PARTITION_REBALANCE"
    TOPIC_PARTITION_INCREASE = "TOPIC_PARTITION_INCREASE"
    BROKER_SCALE = "BROKER_SCALE"
    MESSAGE_SKIP = "MESSAGE_SKIP"
    CONFIG_CHANGE = "CONFIG_CHANGE"
    PREFERRED_REPLICA_ELECTION = "PREFERRED_REPLICA_ELECTION"
    BROKER_HEALTH_CHECK = "BROKER_HEALTH_CHECK"


class Priority(str, Enum):
    IMMEDIATE = "IMMEDIATE"
    WITHIN_15MIN = "WITHIN_15MIN"
    WITHIN_1HOUR = "WITHIN_1HOUR"
    ADVISORY = "ADVISORY"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Grounding(str, Enum):
    RUNBOOK = "RUNBOOK"
    HISTORICAL_INCIDENT = "HISTORICAL_INCIDENT"
    HEURISTIC_ONLY = "HEURISTIC_ONLY"


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class PartitionLag:
    partition: int
    committed_offset: int
    log_end_offset: int
    lag: int
    last_commit_at: Optional[datetime] = None

    @property
    def is_stalled(self) -> bool:
        if self.last_commit_at is None:
            return False
        elapsed = (datetime.utcnow() - self.last_commit_at).total_seconds()
        return elapsed > 600  # 10 minutes


@dataclass
class TopicLagDetail:
    topic: str
    partitions: list[PartitionLag] = field(default_factory=list)
    total_lag: int = 0
    lag_velocity_per_min: float = 0.0
    lag_classification: LagClassification = LagClassification.HEALTHY
    problematic_partitions: list[int] = field(default_factory=list)
    severity_score: float = 0.0


@dataclass
class ConsumerGroupAnalysis:
    group_id: str
    topics: list[TopicLagDetail] = field(default_factory=list)
    member_count: int = 0
    severity_score: float = 0.0
    last_commit_at: Optional[datetime] = None


@dataclass
class HotspotDetail:
    topic: str
    partition: int
    message_rate: float
    skew_factor: float


@dataclass
class PartitionHealthSummary:
    total_partitions: int = 0
    under_replicated: int = 0
    offline: int = 0
    hotspot_partitions: list[HotspotDetail] = field(default_factory=list)
    leader_imbalance_pct: float = 0.0
    broker_partition_counts: dict[int, int] = field(default_factory=dict)


@dataclass
class SimilarIncident:
    incident_id: str
    similarity_score: float
    lag_classification: str
    consumer_group: str
    topic: str
    resolution_type: str
    time_to_resolution_minutes: int
    summary: str


@dataclass
class RunbookChunk:
    runbook_id: str
    title: str
    section: str
    content: str
    relevance_score: float


@dataclass
class ScalingRecommendation:
    recommendation_type: RecommendationType
    priority: Priority
    rationale: str
    risk_level: RiskLevel
    grounding: Grounding
    consumer_group: Optional[str] = None
    topic: Optional[str] = None
    estimated_resolution_minutes: int = 15
    cli_commands: list[str] = field(default_factory=list)


@dataclass
class KafkaLagAnalysisEvent:
    analysis_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    triggered_by: str = "SCHEDULED_POLL"
    analyzed_at: datetime = field(default_factory=datetime.utcnow)
    consumer_groups: list[ConsumerGroupAnalysis] = field(default_factory=list)
    partition_health: Optional[PartitionHealthSummary] = None
    similar_incidents: list[SimilarIncident] = field(default_factory=list)
    runbook_references: list[RunbookChunk] = field(default_factory=list)
    recommendations: list[ScalingRecommendation] = field(default_factory=list)
    llm_root_cause_summary: Optional[str] = None
    overall_confidence: float = 0.0
    severity: Severity = Severity.LOW

    def to_dict(self) -> dict:
        import dataclasses
        return dataclasses.asdict(self)
