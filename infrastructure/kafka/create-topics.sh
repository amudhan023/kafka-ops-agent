#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 60

TOPICS_FILE="/topics.json"
BOOTSTRAP="kafka:29092"

python3 - <<'EOF'
import json, subprocess, sys

with open("/topics.json") as f:
    config = json.load(f)

for topic in config["topics"]:
    name = topic["name"]
    partitions = topic["partitions"]
    replication = topic["replication"]
    retention = topic.get("retention_ms", 604800000)

    result = subprocess.run(
        ["kafka-topics", "--bootstrap-server", "kafka:29092",
         "--create", "--if-not-exists",
         "--topic", name,
         "--partitions", str(partitions),
         "--replication-factor", str(replication),
         "--config", f"retention.ms={retention}"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  [OK] {name} ({partitions} partitions)")
    else:
        print(f"  [ERR] {name}: {result.stderr.strip()}", file=sys.stderr)

print("Topic creation complete.")
EOF
