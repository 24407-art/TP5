#!/usr/bin/env python3
"""
Bank X transaction simulator — publishes JSON batches to Kafka every second.
Vectorized NumPy sampling for throughput; configurable population via env vars.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tx-generator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bank-transactions")
N_CLIENTS = int(os.getenv("N_CLIENTS", "5000"))
M_EXTERNAL = int(os.getenv("M_EXTERNAL", "10000"))
PEAK_MULTIPLIER = float(os.getenv("PEAK_MULTIPLIER", "10"))
PEAK_HOURS = os.getenv("PEAK_HOURS", "9-12,17-20")
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.0002"))
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "1.0"))
MSG_ENTITY = os.getenv("MSG_ENTITY", "bank_X")
APP_TYPE = os.getenv("APP_TYPE", "mobile_app")
TX_TYPE = os.getenv("TX_TYPE", "transfer")

I_MIN, I_MAX = 1000.0, 1_000_000.0


@dataclass
class Population:
    ids: np.ndarray
    banks: np.ndarray
    income: np.ndarray
    spending: np.ndarray
    frequency: np.ndarray
    balance: np.ndarray
    prob_per_tick: np.ndarray


def parse_peak_hours(spec: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        start_s, end_s = part.split("-")
        ranges.append((int(start_s), int(end_s)))
    return ranges


def in_peak_hour(hour: int, ranges: list[tuple[int, int]]) -> bool:
    for start, end in ranges:
        if start <= end:
            if start <= hour < end:
                return True
        else:
            if hour >= start or hour < end:
                return True
    return False


def sample_power_law_income(n: int) -> np.ndarray:
    u = np.random.random(n)
    inv_a = 1.0 / I_MIN
    inv_b = 1.0 / I_MAX
    inv_i = inv_a - u * (inv_a - inv_b)
    return 1.0 / inv_i


def build_population(n_clients: int, m_external: int) -> Population:
    n_total = n_clients + m_external
    ids = np.empty(n_total, dtype=object)
    banks = np.empty(n_total, dtype=object)

    for i in range(n_clients):
        ids[i] = f"client_{i}"
        banks[i] = "bank_X"

    half = m_external // 2
    for i in range(half):
        ids[n_clients + i] = f"user_a_{i}"
        banks[n_clients + i] = "bank_A"
    for i in range(half, m_external):
        ids[n_clients + i] = f"user_b_{i - half}"
        banks[n_clients + i] = "bank_B"

    income = sample_power_law_income(n_total)
    low = income / 1000.0
    high = income / 100.0
    spending = np.random.uniform(low, high)
    frequency = income / spending
    balance = np.random.uniform(0, 3 * income)

    seconds_per_month = 30 * 24 * 3600
    prob_per_tick = frequency / seconds_per_month

    log.info(
        "Population: N=%d bank_X, M=%d external, total=%d, mean_prob/tick=%.6f",
        n_clients,
        m_external,
        n_total,
        float(prob_per_tick.mean()),
    )
    return Population(ids, banks, income, spending, frequency, balance, prob_per_tick)


def ensure_topic(bootstrap: str, topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="tx-gen-admin")
    try:
        admin.create_topics(
            [NewTopic(topic, num_partitions=6, replication_factor=1)],
            validate_only=False,
        )
        log.info("Created Kafka topic '%s'", topic)
    except TopicAlreadyExistsError:
        log.info("Kafka topic '%s' already exists", topic)
    finally:
        admin.close()


def sample_amounts(spending: np.ndarray, n: int) -> np.ndarray:
    sigma = spending / 2.0
    low = np.maximum(spending - 2 * sigma, 1.0)
    high = spending + 2 * sigma
    return np.random.uniform(low, high, size=n)


def inject_fraud(
    pop: Population,
    sender_idx: np.ndarray,
    receiver_idx: np.ndarray,
    amounts: np.ndarray,
    n_fraud: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if n_fraud <= 0 or sender_idx.size == 0:
        return sender_idx, receiver_idx, amounts
    n_fraud = min(n_fraud, sender_idx.size)
    fraud_s = np.random.choice(sender_idx, size=n_fraud, replace=False)
    fraud_r = np.random.randint(0, len(pop.ids), size=n_fraud)
    fraud_a = pop.spending[fraud_s] * np.random.uniform(5, 20, size=n_fraud)
    return (
        np.concatenate([sender_idx, fraud_s]),
        np.concatenate([receiver_idx, fraud_r]),
        np.concatenate([amounts, fraud_a]),
    )


def build_messages(
    pop: Population,
    sender_idx: np.ndarray,
    receiver_idx: np.ndarray,
    amounts: np.ndarray,
    ts: datetime,
) -> list[dict]:
    iso_date = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    messages: list[dict] = []
    for s, r, amount in zip(sender_idx, receiver_idx, amounts):
        messages.append(
            {
                "msg_entity": MSG_ENTITY,
                "app_type": APP_TYPE,
                "send_entity": str(pop.banks[s]),
                "receive_entity": str(pop.banks[r]),
                "send_id": str(pop.ids[s]),
                "receive_id": str(pop.ids[r]),
                "amount": round(float(amount), 2),
                "date": iso_date,
                "tx_type": TX_TYPE,
                "tx_id": str(uuid.uuid4()),
            }
        )
    return messages


class TransactionGenerator:
    def __init__(self) -> None:
        self._running = True
        self.pop = build_population(N_CLIENTS, M_EXTERNAL)
        self.peak_ranges = parse_peak_hours(PEAK_HOURS)
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_: object) -> None:
        log.info("Shutdown requested...")
        self._running = False

    def run(self) -> None:
        ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
            batch_size=65536,
            acks=1,
        )
        log.info("Kafka %s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)

        tick = 0
        total_sent = 0
        while self._running:
            t0 = time.perf_counter()
            now = datetime.now(timezone.utc)
            mult = PEAK_MULTIPLIER if in_peak_hour(now.hour, self.peak_ranges) else 1.0
            probs = np.minimum(self.pop.prob_per_tick * mult, 1.0)

            active_mask = np.random.random(len(self.pop.ids)) < probs
            sender_idx = np.where(active_mask)[0]
            if sender_idx.size == 0:
                self._sleep_remainder(t0)
                tick += 1
                continue

            receiver_idx = np.random.randint(0, len(self.pop.ids), size=sender_idx.size)
            amounts = sample_amounts(self.pop.spending[sender_idx], sender_idx.size)

            valid = amounts <= self.pop.balance[sender_idx]
            sender_idx = sender_idx[valid]
            receiver_idx = receiver_idx[valid]
            amounts = amounts[valid]

            if sender_idx.size == 0:
                self._sleep_remainder(t0)
                tick += 1
                continue

            n_fraud = max(1, int(sender_idx.size * FRAUD_RATE)) if FRAUD_RATE > 0 else 0
            sender_idx, receiver_idx, amounts = inject_fraud(
                self.pop, sender_idx, receiver_idx, amounts, n_fraud
            )

            self.pop.balance[sender_idx] -= amounts
            self.pop.balance[receiver_idx] += amounts

            messages = build_messages(self.pop, sender_idx, receiver_idx, amounts, now)
            for msg in messages:
                producer.send(KAFKA_TOPIC, value=msg)
            producer.flush()

            total_sent += len(messages)
            tick += 1
            if tick % 10 == 0:
                log.info(
                    "tick=%d batch=%d cumulative=%d peak_mult=%.1f",
                    tick,
                    len(messages),
                    total_sent,
                    mult,
                )
            self._sleep_remainder(t0)

        producer.close()
        log.info("Stopped. Total transactions: %d", total_sent)

    @staticmethod
    def _sleep_remainder(t0: float) -> None:
        remaining = TICK_SECONDS - (time.perf_counter() - t0)
        if remaining > 0:
            time.sleep(remaining)


def main() -> None:
    log.info("Start N=%d M=%d peak=%s", N_CLIENTS, M_EXTERNAL, PEAK_MULTIPLIER)
    TransactionGenerator().run()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Fatal error")
        sys.exit(1)
