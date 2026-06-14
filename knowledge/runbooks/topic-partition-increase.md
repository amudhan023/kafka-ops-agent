# Topic Partition Increase Runbook

**Applies to:** GROWING_LAG when consumer count == partition count  
**Severity:** HIGH (action is HIGH risk)  
**Last Updated:** 2026-06-13

## Overview

When consumer count has reached the partition count ceiling and lag continues to grow, the only way to add more parallelism is to increase the partition count. **This is a high-risk operation** — it causes all consumer groups subscribed to the topic to rebalance, and key-based message ordering within a partition may be disrupted.

---

### Prerequisites — Do NOT proceed without confirming

1. Confirm `current_consumers == current_partitions`. Scaling consumers further will have no effect.
2. Confirm with the application team that **message ordering is not required** (or that ordering is only required per-key, not globally).
3. Confirm there is no active incident in progress — do not increase partitions during an active outage.
4. Check that producer key cardinality > new_partition_count (otherwise hotspots will remain).

---

### Procedure

1. **Plan new partition count:**
   - New count = current_count × 2 (always double, never add just 1)
   - Ensure new count is a multiple of current replication factor

2. **Increase partitions:**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 \
     --alter --topic <TOPIC> \
     --partitions <NEW_COUNT>
   ```
   ⚠️ Partition count can only be increased, never decreased.

3. **Scale consumers to match:**
   ```bash
   kubectl scale deployment/<consumer-deployment> --replicas=<NEW_COUNT>
   ```

4. **Monitor rebalance completion:**
   All consumer instances should rejoin the group within `session.timeout.ms` (default 30s). Lag will spike briefly during rebalance — this is expected.

---

### Rollback

Partition count cannot be decreased. If the increase causes issues, the only rollback is to scale consumers back to the old count and wait for the rebalance to stabilize.

---

### Verification

After rebalance completes (watch for `Assignment:` column in `--describe --group`), confirm lag is declining.
