# Under-Replicated Partition Recovery Runbook

**Applies to:** Partition health — under-replicated partitions  
**Severity:** CRITICAL  
**Last Updated:** 2026-06-13

## Overview

An under-replicated partition has fewer in-sync replicas (ISR) than the configured replication factor. This is latent data loss risk: if the leader fails while a follower is under-replicated, committed messages may be lost.

**Do NOT move partitions or increase load on brokers before diagnosing the root cause.**

---

### Diagnosis

1. **Find all under-replicated partitions:**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 \
     --describe --under-replicated-partitions
   ```

2. **Identify which broker is the lagging follower** (ISR list will be shorter than replicas list).

3. **Check the lagging broker's health:**
   - Disk usage: `df -h` on the broker node
   - Network connectivity: `ping <other-broker>`
   - Kafka log: `journalctl -u kafka --since "10 minutes ago"`

4. **Check replication lag metric:**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 \
     --describe --topic <TOPIC>
   ```
   If ISR is consistently shrinking, the follower is not catching up.

---

### Immediate Actions

**If broker disk is full:**
1. Do not restart Kafka. Free disk space first.
2. Delete old consumer group offsets or old log segments.
3. Once disk is freed, the follower will automatically rejoin ISR.

**If broker is unreachable:**
1. Check broker process: `systemctl status kafka`
2. Restart if crashed: `systemctl restart kafka`
3. Monitor ISR recovery — it should begin within 30 seconds of broker restart.

**If broker is healthy but lagging:**
1. Check `replica.lag.time.max.ms` configuration. If the follower is exceeding this threshold, it's kicked from ISR.
2. Temporarily reduce replication load by pausing non-critical producers.

---

### Verification

After fix, confirm no under-replicated partitions:
```bash
kafka-topics --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions
# Expected: no output
```

Monitor `kafka_topic_partition_under_replicated_partition` in Grafana for 15 minutes after recovery.

---

### Escalation

If ISR does not recover within 30 minutes after broker restart: escalate to Kafka infrastructure team. This may require a controlled partition reassignment.
