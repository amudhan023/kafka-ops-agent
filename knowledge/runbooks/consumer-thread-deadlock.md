# Consumer Thread Deadlock Runbook

**Applies to:** STALLED_CONSUMER where consumer process is alive  
**Severity:** CRITICAL  
**Last Updated:** 2026-06-13

## Overview

A consumer is alive (heartbeating to Kafka coordinator) but not making progress on offset commits. This pattern — consumer alive, offsets frozen — indicates a processing thread deadlock or infinite loop inside the consumer's `poll()` callback.

---

### Diagnosis

1. **Confirm consumer is alive but not committing:**
   - Check `kafka_consumer_group_lag` is non-zero and frozen
   - Check consumer process health: `kubectl get pods -l app=<app>`
   - Consumer pod shows Running but lag is stuck

2. **Get a thread dump from the consumer JVM:**
   ```bash
   kubectl exec -it <consumer-pod> -- jstack <PID>
   ```
   Or for Python:
   ```bash
   kubectl exec -it <consumer-pod> -- kill -SIGQUIT <PID>
   ```

3. **Look for:**
   - `BLOCKED` threads waiting on a lock
   - `WAITING` threads on a lock held by another thread
   - Threads with very deep call stacks stuck in application code

---

### Immediate Actions

1. **Restart the consumer pod immediately** (interrupts the deadlock):
   ```bash
   kubectl delete pod <consumer-pod>
   ```
   The deployment will recreate it. If the deadlock is deterministic (same message causes it every time), the consumer will deadlock again.

2. **If deadlock recurs:** Skip the offending message (see `consumer-group-reset.md`).

3. **Escalate to the application team** with the thread dump for root cause analysis.

---

### Root Cause Categories

- Database connection pool exhausted inside the poll callback (connection pool too small)
- Downstream API call with no timeout
- Synchronized block held by a background thread that never releases
