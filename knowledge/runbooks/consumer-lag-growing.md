# Growing Consumer Lag Runbook

**Applies to:** GROWING_LAG  
**Severity:** HIGH  
**Last Updated:** 2026-06-13

## Overview

A consumer group is falling behind production rate. Messages are accumulating faster than consumers can process them.

### When to use this runbook

- `kafka_consumer_group_lag_sum` is increasing monotonically
- Lag velocity is positive and accelerating over a 10-minute window
- Consumer throughput (msg/s) is lower than producer throughput (msg/s)

---

### Diagnosis

1. **Confirm the lag is growing and not a one-time spike:**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group <GROUP_ID>
   ```
   Run twice, 60 seconds apart. Compare LAG columns.

2. **Check consumer instance count:**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group <GROUP_ID> | grep -c CONSUMER-ID
   ```

3. **Check if consumers are CPU or I/O bound** (check the consuming application's resource metrics).

4. **Check if the producer rate has spiked** (look at `kafka_topic_partition_current_offset` rate in Grafana).

---

### Immediate Actions

1. **If produce rate spiked (thundering herd):** Wait and monitor. Consumers will catch up once the spike passes. Do NOT scale consumers immediately — a transient spike will self-resolve.

2. **If consume rate dropped (consumers degraded):** Restart the consumer group instances.
   ```bash
   kubectl rollout restart deployment/<consumer-deployment>
   ```

3. **If lag is persistent and consumer count < partition count:** Scale out consumers.
   ```bash
   kubectl scale deployment/<consumer-deployment> --replicas=<N>
   ```
   where N = number of partitions for the topic.

---

### Verification

After applying actions, confirm lag velocity turns negative within 5 minutes:
```bash
watch -n 10 'kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <GROUP_ID> | grep -E "GROUP|LAG"'
```

---

### Escalation

If lag exceeds 500,000 messages and is still growing after 15 minutes: escalate to on-call lead and consider increasing partition count (see `topic-partition-increase.md`).
