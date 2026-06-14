# Partition Leader Rebalancing Runbook

**Applies to:** Leader imbalance, hot broker  
**Severity:** MEDIUM  
**Last Updated:** 2026-06-13

## Overview

When partition leaders are unevenly distributed across brokers, one broker handles disproportionate produce/consume traffic while others are underutilized. Preferred replica election redistributes leaders back to their assigned preferred broker.

---

### Diagnosis

1. **Check leader distribution:**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 --describe | \
     awk '/Leader:/ {print $2}' | sort | uniq -c | sort -rn
   ```

2. **Identify which broker is overloaded** (leader count >> others).

3. **Check if the overloaded broker shows elevated CPU or network in Grafana.**

---

### Immediate Actions

**Trigger preferred replica election (low risk, transient disruption):**
```bash
kafka-leader-election --bootstrap-server localhost:9092 \
  --election-type PREFERRED --all-topic-partitions
```

This causes a brief leader transfer per partition (milliseconds per partition), during which producers see increased latency. Schedule during low-traffic periods if possible.

**Verify after election:**
```bash
kafka-topics --bootstrap-server localhost:9092 --describe | \
  awk '/Leader:/ {print $2}' | sort | uniq -c
```
Leaders should be roughly evenly distributed.

---

### Enable Auto Leader Rebalancing (Preventative)

Add to `server.properties`:
```properties
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=300
leader.imbalance.per.broker.percentage=10
```

---

### Verification

`kafka_topic_partition_leader_is_preferred` metric should return 1.0 for all partitions after successful rebalancing.
