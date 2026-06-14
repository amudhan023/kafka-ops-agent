# Network Partition Recovery Runbook

**Applies to:** ISR shrinkage, under-replicated partitions after network events  
**Severity:** CRITICAL  
**Last Updated:** 2026-06-13

## Overview

A network partition between Kafka brokers causes followers to be removed from ISR (they can't replicate fast enough). After the network heals, followers must fully re-replicate before rejoining ISR. This can take minutes to hours depending on how much data was produced during the partition.

---

### Diagnosis

1. **Identify affected brokers:**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 \
     --describe --under-replicated-partitions
   ```

2. **Check broker connectivity:**
   ```bash
   # From broker A to broker B
   nc -zv <broker-B-host> 9092
   ```

3. **Check replication lag:**
   Monitor `kafka_topic_partition_under_replicated_partition` metric over time. If it is decreasing, recovery is in progress.

---

### Immediate Actions

**During the network partition (brokers unreachable from each other):**
1. Do NOT restart brokers — this will make recovery slower.
2. Do NOT increase `min.insync.replicas` — producers will start failing.
3. Notify producers to expect increased latency if `acks=all`.

**After network heals:**
1. Monitor ISR recovery automatically. Kafka will re-replicate missing data.
2. If ISR recovery is not starting after 2 minutes of network healing: check if the lagging broker's Kafka process is still running.

---

### Fencing Stale Leaders (Split-Brain Prevention)

Kafka uses epoch numbers to fence stale leaders. After a partition heals, Kafka's controller will automatically invalidate the stale leader's epoch and elect a new leader from the current ISR. No manual intervention is needed.

---

### Verification

`kafka_topic_partition_under_replicated_partition` should return to 0 after full ISR recovery. Time to recovery = bytes_behind / network_throughput_between_brokers.
