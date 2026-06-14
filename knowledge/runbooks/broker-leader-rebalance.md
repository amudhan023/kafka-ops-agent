# Broker Leader Rebalance Runbook

**Applies to:** Leader imbalance > 40%  
**Severity:** MEDIUM  
**Last Updated:** 2026-06-13

## Overview

One broker holding more than 40% of partition leadership causes uneven load distribution. This often happens after a broker restart — the restarted broker's partitions elect leaders on other brokers and may not transfer back automatically if `auto.leader.rebalance.enable` is false.

---

### Diagnosis

Check which broker has disproportionate leader count:
```bash
kafka-topics --bootstrap-server localhost:9092 --describe | \
  awk '{for(i=1;i<=NF;i++){if($i~/Leader:/){print $(i+1)}}}' | \
  sort | uniq -c | sort -rn
```

---

### Immediate Actions

```bash
kafka-leader-election --bootstrap-server localhost:9092 \
  --election-type PREFERRED --all-topic-partitions
```

This is low-risk and completes in seconds for small clusters.

---

### Preventative Configuration

```properties
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=300
leader.imbalance.per.broker.percentage=10
```

With these settings, Kafka automatically rebalances leaders every 5 minutes if any broker holds more than 10% more than its fair share.
