# Getting Started — TP5 Fraud Detection

Step-by-step guide with expected execution traces.

---

## 1. Prerequisites

- Docker Engine + Compose v2
- 8 GB RAM recommended
- Ports free: `7077`, `8080`, `8081`, `8082-8084`, `8888`, `9092`, `9094`

---

## 2. Start infrastructure

From repository root:

```bash
cd /path/to/spark-lab
docker compose up -d --build
```

**Expected trace:**

```text
[+] Building transaction-generator ...
[+] Running 9/9
 ✔ Container kafka                  Started
 ✔ Container kafka-ui               Started
 ✔ Container spark-master           Started
 ✔ Container spark-worker-1         Started
 ✔ Container spark-worker-2         Started
 ✔ Container spark-worker-3         Started
 ✔ Container transaction-generator  Started
 ✔ Container spark-processor        Started
 ✔ Container jupyter                Started
```

Verify:

```bash
docker compose ps
```

All services should show `Up` (generator/processor may show `Restarting` briefly on first Kafka boot — wait 30s).

---

## 3. Smoke test script

```bash
chmod +x scripts/verify_tp5.sh
./scripts/verify_tp5.sh
```

**Expected trace (after ~30–60 seconds):**

```text
=== TP5 verification ===
[1] Container health
kafka                  Up
transaction-generator  Up
spark-processor        Up
...
[2] Kafka topic bank-transactions
OK: topic exists
[3] Generator logs (last 5 lines)
2026-05-21 14:00:10 [INFO] tick=10 batch=42 cumulative=380 peak_mult=1.0
[5] Output directories
OK: data/tp5/output/recent_transactions has files
```

---

## 4. Monitor Kafka (optional)

1. Open http://localhost:8080
2. Topic: `bank-transactions`
3. Confirm messages with JSON fields: `send_id`, `receive_id`, `amount`, `date`, `tx_id`

**CLI trace:**

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bank-transactions \
  --max-messages 3
```

Sample message:

```json
{"msg_entity":"bank_X","app_type":"mobile_app","send_entity":"bank_X",
 "receive_entity":"bank_A","send_id":"client_42","receive_id":"user_a_17",
 "amount": 245.31, "date":"2026-05-21T14:05:01Z","tx_type":"transfer",
 "tx_id":"a1b2c3d4-..."}
```

---

## 5. Spark processor

Logs:

```bash
docker logs -f spark-processor
```

**Expected trace:**

```text
Started window query: 3_hours -> /workspace/data/tp5/output/windowed/3_hours
Started window query: 7_days -> ...
Started lifetime query -> /workspace/data/tp5/output/lifetime
Started recent tx sink -> /workspace/data/tp5/output/recent_transactions
Running 6 streaming queries.
```

Spark Master UI: http://localhost:8081 — should show running application `BankXFraudStreaming`.

Check output files:

```bash
find data/tp5/output -name "*.parquet" | head
```

---

## 6. Jupyter dashboard

1. Open http://localhost:8888
2. Token: `ChangeMeStrong` (see `docker-compose.yml`)
3. Open `work/fraud_dashboard.ipynb`
4. Run all cells — enable **Auto-refresh**

**Expected behavior:**

- Section *Last 10 seconds activity* fills with rows
- *Last 20 active users* lists `client_*` / `user_a_*` / `user_b_*`
- Window tables appear for `3_hours`, `7_days`, etc.
- Bar chart: sent vs received totals in last 10s
- Rows with amount >> lifetime average highlighted red/yellow


---

## 7. Execution order (logic)

```text
1. Kafka broker ready
2. transaction-generator creates topic + publishes batches every 1s
3. spark-processor consumes Kafka → writes Parquet + checkpoints
4. fraud_dashboard reads Parquet → displays metrics
```

Restart processor only (keeps checkpoints):

```bash
docker compose restart spark-processor
```

Full reset (clears state):

```bash
docker compose down
rm -rf data/tp5/output/* checkpoints/tp5/*
docker compose up -d --build
```

---

## 8. Scale to full specification

Edit `docker-compose.yml`:

```yaml
transaction-generator:
  environment:
    - N_CLIENTS=100000
    - M_EXTERNAL=200000
    - PEAK_MULTIPLIER=50
```

Increase Spark memory if OOM:

```yaml
spark-processor:
  environment:
    - SHUFFLE_PARTITIONS=24
```

---

## 9. Stop

```bash
docker compose down
```

Data in `data/tp5/output` and `checkpoints/tp5` persists until you delete them.

---

## 10. Troubleshooting

| Symptom | Action |
|---------|--------|
| Empty dashboard | Wait 60s; check `docker logs spark-processor` |
| Generator exits | `docker logs transaction-generator`; ensure Kafka is Up |
| Processor restart loop | Delete `checkpoints/tp5/*` and restart |
| No workers in Spark UI | `docker compose ps` — start worker containers |
| Kafka connection refused | Wait for kafka healthy; restart generator |

---

## 11. Test checklist (presentation)

- [ ] Kafka UI shows live messages on `bank-transactions`
- [ ] Generator log shows cumulative tx increasing
- [ ] Spark UI shows 3 workers + streaming app
- [ ] Parquet files under `data/tp5/output/`
- [ ] Dashboard auto-refresh with 20 users + 10s window
