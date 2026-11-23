"""Rebuild Bronze, Silver, and Gold Parquet snapshots from order data."""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def run(spark, source, output):
    bronze = output / "bronze"
    silver = output / "silver"
    gold = output / "gold"
    raw = spark.read.option("header", True).option("mode", "FAILFAST").csv(str(source))
    raw.select("order_id", "region", "amount").write.mode("overwrite").parquet(str(bronze))

    cleaned = spark.read.parquet(str(bronze)).select(
        F.trim("order_id").alias("order_id"),
        F.lower(F.trim("region")).alias("region"),
        F.expr("try_cast(amount as decimal(18,2))").alias("amount"),
    )
    valid = cleaned.where(
        (F.col("order_id") != "") & (F.col("region") != "") & (F.col("amount") >= 0)
    ).distinct()
    unique_keys = valid.groupBy("order_id").count().where("count = 1").select("order_id")
    valid.join(unique_keys, "order_id").write.mode("overwrite").parquet(str(silver))

    summary = spark.read.parquet(str(silver)).groupBy("region").agg(
        F.count("*").alias("orders"), F.sum("amount").alias("revenue")
    )
    summary.write.mode("overwrite").parquet(str(gold))
    spark.read.parquet(str(gold)).orderBy("region").show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/orders.csv"))
    parser.add_argument("--output", type=Path, default=Path("output"))
    args = parser.parse_args()
    spark = SparkSession.builder.master("local[2]").appName("orders-medallion").getOrCreate()
    try:
        run(spark, args.input, args.output)
    finally:
        spark.stop()
