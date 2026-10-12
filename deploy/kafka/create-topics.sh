#!/usr/bin/env bash
set -euo pipefail
bs="${BOOTSTRAP_SERVER:-kafka:9092}"
topics=/opt/kafka/bin/kafka-topics.sh
"$topics" --bootstrap-server "$bs" --create --if-not-exists \
  --topic incoming.user_activity --partitions 6 --replication-factor 1 \
  --config retention.ms=604800000
"$topics" --bootstrap-server "$bs" --create --if-not-exists \
  --topic incoming.user_activity.dlq --partitions 3 --replication-factor 1 \
  --config retention.ms=1209600000
