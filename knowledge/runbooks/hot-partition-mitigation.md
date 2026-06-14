# Hot Partition Mitigation Runbook

**Applies to:** Partition hotspot detected  
**Severity:** MEDIUM  
**Last Updated:** 2026-06-13

## Overview

A hot partition receives significantly more messages than other partitions of the same topic. This overloads the partition's leader broker and limits parallelism for consumers. Root cause is almost always a low-cardinality producer key or poor key selection.

---

### Diagnosis

1. **Identify the hot partition:**
   ```bash
   kafka-log-dirs --bootstrap-server localhost:9092 \
     --topic-list <TOPIC> --describe | \
     python3 -c "import json,sys; \
       data=json.load(sys.stdin); \
       [print(p['partition'], p['size']) \
        for b in data['brokers'] \
        for log in b['logDirs'] \
        for p in log['partitions']]"
   ```

2. **Identify the hot key:**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic <TOPIC> --partition <HOT_PARTITION> \
     --max-messages 100 --property print.key=true | \
     awk -F'\t' '{print $1}' | sort | uniq -c | sort -rn | head -10
   ```

---

### Mitigations

**Option 1: Add a random suffix to the key (immediate, reversible)**
Change the producer to append a random 0-9 suffix to the hot key. This spreads messages across 10 partitions at the cost of losing per-key ordering guarantees.

**Option 2: Custom partitioner**
Implement a custom partitioner that distributes high-volume keys across multiple target partitions.

**Option 3: Increase partition count**
See `topic-partition-increase.md`. Only recommended if the key cardinality is inherently high but current partition count is too low.

---

### Verification

After applying key distribution changes, monitor per-partition offset delta rate in Grafana. Hot partition's rate should drop to within 2× of the average.
