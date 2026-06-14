# Compaction-Induced Consumer Lag Runbook

**Applies to:** GROWING_LAG on compacted topics  
**Severity:** MEDIUM  
**Last Updated:** 2026-06-13

## Overview

Log compaction on Kafka topics with `cleanup.policy=compact` can cause consumer lag when compaction falls behind. Active segments are not compacted, so if the compaction thread is slow, old messages accumulate and consumers appear to lag even though they are keeping up with the active segment.

---

### Diagnosis

1. **Confirm the topic uses compaction:**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 \
     --describe --topic <TOPIC> | grep cleanup.policy
   ```

2. **Check compaction thread status** (on the broker with the partition leader):
   ```bash
   kafka-log-dirs --bootstrap-server localhost:9092 \
     --topic-list <TOPIC> --describe
   ```
   Look for large `offsetLag` values on non-active segments.

3. **Check broker log for compaction errors:**
   ```bash
   grep -i "compaction" /var/log/kafka/server.log | tail -50
   ```

---

### Immediate Actions

1. **Trigger compaction manually** (Kafka 2.4+):
   There is no direct "compact now" command. Instead, temporarily reduce `min.cleanable.dirty.ratio` to force more frequent compaction:
   ```bash
   kafka-configs --bootstrap-server localhost:9092 \
     --entity-type topics --entity-name <TOPIC> \
     --alter --add-config min.cleanable.dirty.ratio=0.1
   ```

2. **Increase compaction threads** (in `server.properties`):
   ```properties
   log.cleaner.threads=4
   log.cleaner.io.max.bytes.per.second=52428800
   ```
   Requires broker restart.

3. **After compaction catches up, restore original config.**

---

### Verification

Compaction lag (`offsetLag` on non-active segments) should decrease over 30-60 minutes after config changes.
