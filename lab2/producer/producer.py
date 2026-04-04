import csv
import json
import os
import sys
import time
from typing import Any, Dict, Iterator, List

from kafka import KafkaProducer
from kafka.errors import KafkaError


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def read_rows_cycle(path: str) -> Iterator[Dict[str, Any]]:
    """Бесконечный итератор по строкам CSV (перезапуск с начала после EOF)."""
    while True:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise RuntimeError("CSV has no header row")
            for row in reader:
                # Нормализация: пустые строки -> None для компактного JSON
                yield {k: (v if v != "" else None) for k, v in row.items()}


def main() -> None:
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS")
    topic = env("KAFKA_TOPIC", "casino_games")
    csv_path = env("CSV_PATH")
    interval = float(env("SEND_INTERVAL_SEC", "3"))
    rows_per_tick = int(env("ROWS_PER_TICK", "50"))

    if not os.path.isfile(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=10,
        retries=5,
        acks="all",
    )

    print(f"Producer started: topic={topic}, csv={csv_path}, every {interval}s -> {rows_per_tick} rows")
    row_iter = read_rows_cycle(csv_path)
    batch_id = 0

    while True:
        batch: List[Dict[str, Any]] = []
        for _ in range(rows_per_tick):
            batch.append(next(row_iter))

        for i, payload in enumerate(batch):
            key = f"{batch_id}-{i}"
            try:
                producer.send(topic, key=key, value=payload)
            except KafkaError as e:
                print(f"Kafka send error: {e}", file=sys.stderr)

        producer.flush()
        batch_id += 1
        print(f"Sent batch {batch_id}, {len(batch)} messages")
        time.sleep(interval)


if __name__ == "__main__":
    main()
