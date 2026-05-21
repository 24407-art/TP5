# TP5 — Real-time Banking Fraud Detection
 
**Stack:** Kafka + Spark Structured Streaming + Jupyter + Docker Compose

End-to-end pipeline that simulates Bank X transactions, streams them through Kafka, computes per-user fraud-detection statistics in real time, and displays a live dashboard.

---

## Architecture

```
[Transaction Generator] → [Kafka] → [Spark Processor] → [Parquet] → [Jupyter Dashboard]
                              ↓
                         [Kafka UI]
```

See [docs/architecture.md](docs/architecture.md) for diagrams and design decisions.


---

## Quick start

```bash
docker compose up -d --build
bash scripts/verify_tp5.sh
```

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8081 |
| Jupyter | http://localhost:8888 (token: `ChangeMeStrong`) |

Open **`notebooks/fraud_dashboard.ipynb`** and run all cells.

Full traces: [getting_started.md](getting_started.md)

---

## Simulation parameters (documented choices)

| Parameter | Default (dev) | Production-scale |
|-----------|---------------|------------------|
| `N_CLIENTS` | 5,000 | 100,000 (Bank X) |
| `M_EXTERNAL` | 10,000 | 200,000 (banks A+B) |
| `PEAK_MULTIPLIER` | 10 | Burst toward 1000+ tx/s |
| `FRAUD_RATE` | 0.0005 | Injects abnormal amounts |

Income: power law P(I) ∝ 1/I² on [1000, 1M] MRU.  
Spending: uniform in [I/1000, I/100].  
Probability per second: f / (30×24×3600) with peak multiplier.

Override in `docker-compose.yml` under `transaction-generator` → `environment`.

---

## Required metrics (implemented)

Per `user_id` and direction (`sent` / `received`):

- **Windowed:** avg amount, tx count, distinct counterparties — windows 3h, 7d, 3w, 3mo
- **Lifetime:** avg amount, tx count, total amount, distinct counterparties since stream start
- **Dashboard:** last 20 users, last 10 seconds activity, auto-refresh, anomaly coloring

---

## Python dependencies

- Generator: `numpy`, `kafka-python`
- Processor: PySpark 3.5.3 (Spark image)
- Jupyter: `pandas`, `numpy`, `matplotlib`, `plotly`, `ipywidgets`, `kafka-python`

---

