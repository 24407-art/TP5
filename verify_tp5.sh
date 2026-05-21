#!/usr/bin/env bash
# Quick smoke tests for TP5 stack (run from repo root after docker compose up)
set -euo pipefail

echo "=== TP5 verification ==="

echo "[1] Container health"
docker compose ps --format "table {{.Name}}\t{{.Status}}" | grep -E "kafka|generator|spark|jupyter" || true

echo "[2] Kafka topic bank-transactions"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep bank-transactions \
  && echo "OK: topic exists" || echo "WARN: topic not found yet"

echo "[3] Generator logs (last 5 lines)"
docker logs transaction-generator --tail 5 2>/dev/null || echo "WARN: generator not running"

echo "[4] Spark processor logs"
docker logs spark-processor --tail 8 2>/dev/null || echo "WARN: processor not running"

echo "[5] Output directories"
for d in windowed/3_hours lifetime recent_transactions; do
  if ls data/tp5/output/$d 2>/dev/null | head -1 | grep -q .; then
    echo "OK: data/tp5/output/$d has files"
  else
    echo "WAIT: data/tp5/output/$d empty (wait ~30s after start)"
  fi
done

echo "[6] Sample Parquet row count (requires pandas in host or jupyter)"
docker exec jupyter python -c "
import pandas as pd
from pathlib import Path
p = Path('/workspace/data/tp5/output/recent_transactions')
files = list(p.rglob('*.parquet'))
print('recent parquet files:', len(files))
if files:
    df = pd.read_parquet(files[-1])
    print('last file rows:', len(df))
" 2>/dev/null || echo "SKIP: run dashboard in Jupyter for Parquet check"

echo "=== Done ==="
