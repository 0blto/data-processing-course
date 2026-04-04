#!/bin/bash
set -euo pipefail
CP="${SPARK_CHECKPOINT_LOCATION:-/tmp/spark-checkpoint}"
mkdir -p "$CP"
chown -R spark:spark "$CP"
exec gosu spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.7.4 \
  /opt/spark/work-dir/streaming_job.py
