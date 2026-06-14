# Consumer Group Offset Reset Runbook

**Applies to:** STALLED_CONSUMER, post-incident catch-up  
**Severity:** HIGH (data loss risk)  
**Last Updated:** 2026-06-13

## Overview

Resetting consumer group offsets allows a stuck or recovering group to skip problematic messages or replay from an earlier point. **Always confirm the business impact of skipping messages before executing.**

---

### Offset Reset Options

| Strategy | Command Flag | When to Use |
|----------|-------------|-------------|
| Skip to latest | `--to-latest` | Consumer fell too far behind; older messages are expired |
| Skip N messages | `--to-offset <N>` | Skip a specific poison pill message |
| Reset to timestamp | `--to-datetime 2026-06-13T10:00:00.000` | Replay from a known-good point in time |
| Skip by duration | `--by-duration PT1H` | Skip the last 1 hour of messages |

---

### Procedure

**The consumer group MUST be stopped before resetting offsets.**

1. **Stop the consumer group:**
   ```bash
   kubectl scale deployment/<consumer-deployment> --replicas=0
   ```

2. **Verify the group is inactive:**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group <GROUP_ID>
   # STATE should show "Empty"
   ```

3. **Preview the reset (dry run):**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --group <GROUP_ID> --topic <TOPIC> \
     --reset-offsets --to-latest --dry-run
   ```

4. **Apply the reset:**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --group <GROUP_ID> --topic <TOPIC> \
     --reset-offsets --to-latest --execute
   ```

5. **Restart the consumer:**
   ```bash
   kubectl scale deployment/<consumer-deployment> --replicas=<N>
   ```

---

### Data Loss Warning

`--to-latest` will cause all messages produced between the old committed offset and the current end of the topic to be skipped permanently. Confirm with the service owner and log the skipped offset range in the incident record.
