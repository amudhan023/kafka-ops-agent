# Stalled Consumer Detection Runbook

**Applies to:** STALLED_CONSUMER, SINGLE_PARTITION_STALL  
**Severity:** CRITICAL  
**Last Updated:** 2026-06-13

## Overview

A consumer has stopped committing offsets. This is different from growing lag — the consumer may be alive but stuck processing a single message (poison pill) or may have crashed silently.

---

### Diagnosis

1. **Find the stalled consumer and which offset it's stuck on:**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group <GROUP_ID>
   ```
   Look for a partition where LAG is non-zero but CURRENT-OFFSET hasn't changed.

2. **Check if the consumer process is alive:**
   ```bash
   kubectl get pods -l app=<consumer-app> | grep -v Running
   ```

3. **Inspect the message at the stuck offset:**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic <TOPIC> \
     --partition <PARTITION> \
     --offset <STUCK_OFFSET> \
     --max-messages 1
   ```
   Look for malformed JSON, unexpectedly large payloads, or null values.

---

### Immediate Actions

**If consumer is alive but stuck (poison pill pattern):**

1. Read the problematic message and log it:
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic <TOPIC> --partition <P> --offset <N> --max-messages 1 > /tmp/poison_pill.json
   ```

2. **Skip the message** by resetting the offset to N+1:
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --group <GROUP_ID> --topic <TOPIC>:<P> \
     --reset-offsets --to-offset <N+1> --execute
   ```
   ⚠️ **Risk:** The message at offset N will not be processed. Confirm this is acceptable with the service owner before executing.

3. Restart the consumer after offset reset.

**If consumer has crashed:**

1. Restart the consumer deployment.
2. Confirm offset is picked up correctly.

---

### SINGLE_PARTITION_STALL

When only one partition is stalled while others are healthy, the root cause is almost always:
- A poison pill message at the current offset
- A consumer thread deadlock processing a specific key (check `consumer-thread-deadlock.md`)

The partition stall pattern is: lag on partition P >> lag on all other partitions.

---

### Verification

Confirm `CURRENT-OFFSET` is advancing on the previously stalled partition within 2 minutes of applying the fix.
