import os
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, lit, udf
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env: {name}")
    return v


KAFKA_JSON_SCHEMA = StructType(
    [
        StructField("casino", StringType(), True),
        StructField("game", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("rtp", StringType(), True),
        StructField("volatility", StringType(), True),
        StructField("jackpot", StringType(), True),
        StructField("country_availability", StringType(), True),
        StructField("min_bet", StringType(), True),
        StructField("max_win", StringType(), True),
        StructField("game_type", StringType(), True),
        StructField("game_category", StringType(), True),
        StructField("license_jurisdiction", StringType(), True),
        StructField("release_year", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("mobile_compatible", StringType(), True),
        StructField("free_spins_feature", StringType(), True),
        StructField("bonus_buy_available", StringType(), True),
        StructField("max_multiplier", StringType(), True),
        StructField("languages", StringType(), True),
        StructField("last_updated", StringType(), True),
    ]
)


@udf(StringType())
def risk_tier_udf(rtp: float | None, volatility: str | None) -> str:
    if rtp is None:
        return "unknown"
    v = (volatility or "").strip().lower()
    if rtp < 95.0:
        return "high"
    if rtp >= 97.0 and v in ("low", "very low"):
        return "low"
    if v in ("high", "very high"):
        return "high"
    return "medium"


def jdbc_props() -> Dict[str, str]:
    return {
        "driver": "org.postgresql.Driver",
        "user": env("JDBC_USER"),
        "password": env("JDBC_PASSWORD"),
    }


def build_transformed_df(raw):
    parsed = (
        raw.select(
            from_json(col("value").cast("string"), KAFKA_JSON_SCHEMA).alias("j"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
        )
        .select("j.*", "kafka_offset", "kafka_partition")
        .filter(col("casino").isNotNull())
    )

    typed = (
        parsed.withColumn("rtp_d", col("rtp").cast(DoubleType()))
        .withColumn("min_bet_d", col("min_bet").cast(DoubleType()))
        .withColumn("max_win_d", col("max_win").cast(DoubleType()))
        .withColumn("max_mult_d", col("max_multiplier").cast(DoubleType()))
        .withColumn("release_year_i", col("release_year").cast(IntegerType()))
        .withColumn(
            "mobile_ok",
            col("mobile_compatible").isin("True", "true", "1"),
        )
    )

    filtered = typed.filter(
        (col("rtp_d") >= 96.0) & col("game_category").isNotNull()
    )

    with_udf = filtered.withColumn(
        "risk_tier", risk_tier_udf(col("rtp_d"), col("volatility"))
    )
    return with_udf


def foreach_batch_fn(batch_df, batch_id: int) -> None:
    url = env("JDBC_URL")
    props = jdbc_props()

    if batch_df.isEmpty():
        return

    events = batch_df.select(
        col("casino"),
        col("game"),
        col("provider"),
        col("rtp_d").alias("rtp"),
        col("volatility"),
        col("jackpot"),
        col("game_type"),
        col("game_category"),
        col("min_bet_d").alias("min_bet"),
        col("max_win_d").alias("max_win"),
        col("release_year_i").alias("release_year"),
        col("mobile_ok").alias("mobile_compatible"),
        col("max_mult_d").alias("max_multiplier"),
        col("risk_tier"),
        col("kafka_offset"),
        col("kafka_partition"),
    )

    events.write.jdbc(url, "casino_games_processed", "append", properties=props)

    agg = (
        batch_df.groupBy(col("game_category"))
        .agg(
            count(lit(1)).alias("event_count"),
            avg(col("rtp_d")).alias("avg_rtp"),
            avg(col("max_win_d")).alias("avg_max_win"),
        )
        .withColumn("batch_id", lit(int(batch_id)))
    )

    agg.select(
        col("batch_id"),
        col("game_category"),
        col("event_count"),
        col("avg_rtp"),
        col("avg_max_win"),
    ).write.jdbc(
        url, "casino_category_batch_agg", "append", properties=props
    )


def main() -> None:
    bootstrap = env("SPARK_KAFKA_BOOTSTRAP")
    topic = env("SPARK_KAFKA_TOPIC")
    checkpoint = env("SPARK_CHECKPOINT_LOCATION")

    spark = (
        SparkSession.builder.appName("casino-kafka-to-postgres")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    transformed = build_transformed_df(raw)

    query = (
        transformed.writeStream.outputMode("append")
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", checkpoint)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
