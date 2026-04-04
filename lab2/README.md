# Лабораторная: Kafka + Spark Streaming + PostgreSQL

Стриминговый пайплайн: продюсер читает CSV (датасет Kaggle *Online Casino Games*) и шлёт JSON в Kafka; Spark Structured Streaming обрабатывает сообщения и пишет результат в Postgres.


### Датасет

```bash
python scripts/fetch_dataset.py
```

## Запуск

Из корня проекта:

```bash
docker compose up --d
```

### 1. Логи

- **producer** — строки вида `Sent batch N, 50 messages`.
- **spark-streaming** — без падений, периодически предупреждения Kafka (WARN можно игнорировать для лабораторной).

```bash
docker compose logs -f producer
docker compose logs -f spark-streaming
```

### 2. PostgreSQL

Подключение с хоста: `localhost:5432`, пользователь `spark`, пароль `spark`, база `casino_dw`.

Примеры запросов:

```bash
docker compose exec postgres psql -U spark -d casino_dw -c "SELECT COUNT(*) FROM casino_games_processed;"
docker compose exec postgres psql -U spark -d casino_dw -c "SELECT risk_tier, COUNT(*) FROM casino_games_processed GROUP BY risk_tier;"
docker compose exec postgres psql -U spark -d casino_dw -c "SELECT * FROM casino_category_batch_agg ORDER BY batch_id DESC LIMIT 10;"
```

### 3. Kafka

С хоста брокер доступен как `localhost:29092` (внутри Docker-сети контейнеры ходят на `kafka:9092`).

## Переменные

| Сервис | Переменная | Смысл |
|--------|------------|--------|
| producer | `SEND_INTERVAL_SEC` | пауза между пачками сообщений |
| producer | `ROWS_PER_TICK` | сколько строк за одну пачку |
| producer | `KAFKA_TOPIC` | имя топика (должно совпадать со Spark) |

После смены переменных: `docker compose up -d --build`.
