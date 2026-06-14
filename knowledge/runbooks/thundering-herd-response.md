# Thundering Herd Response Runbook

**Applies to:** THUNDERING_HERD  
**Severity:** MEDIUM  
**Last Updated:** 2026-06-13

## Overview

Multiple consumer groups are lagging simultaneously across multiple topics. This pattern indicates a producer-side burst (batch job, traffic spike) rather than a consumer-side failure. Consumers are healthy — they simply cannot keep up with a temporary spike.

**Key distinction:** THUNDERING_HERD lag velocity is positive but decelerating. GROWING_LAG velocity is positive and constant or accelerating.

---

### Diagnosis

1. **Confirm the pattern — all groups lag simultaneously:**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --all-groups | sort -k6 -rn | head -20
   ```
   If multiple groups all show increasing lag that started at the same time, this is thundering herd.

2. **Identify the burst source** — check producer metrics or application logs for a batch job, scheduled task, or traffic event.

3. **Estimate catch-up time:**
   ```
   catch_up_time = total_lag / (consume_rate - produce_rate)
   ```
   If produce rate has already dropped back to normal, catch-up time = lag / consume_rate.

---

### Immediate Actions

1. **Do NOT immediately scale consumers.** Scaling for a transient spike causes unnecessary rebalances and can make catch-up slower during the rebalance window.

2. **Monitor for 10 minutes.** If lag velocity turns negative (consumers are catching up), no action required.

3. **If the burst is ongoing** (batch job still running):
   - Throttle the producer at the source: add `max.block.ms` and `buffer.memory` limits.
   - Or coordinate with the team running the batch job to reduce produce rate.

4. **Only scale consumers** if catch-up time > 1 hour AND the lag has business SLA impact.

---

### Backpressure Configuration (Preventative)

For batch jobs known to cause thundering herd, configure producer-side throttling:
```properties
# In the batch producer config
max.block.ms=60000
buffer.memory=33554432
linger.ms=50
batch.size=65536
```

---

### Verification

Lag should begin declining within 15 minutes of the burst ending. Monitor `kafka_consumer_group_lag_sum` in Grafana.
