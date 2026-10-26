"""Spark job 1: hourly raw snapshots -> one row per game per day.

python soarj_jobs/daily_stats.py -- date 2026-10-20 --lake data
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# Explicit schema: skips inference scan and a renamed field shows up as nulls (what is inference scan?)
RAW_SCHEMA = StructType([
	StructField("appid", LongType()),
	StructField("game_name", StringType()),
	StructField("player_count", LongType()),
	StructField("api_result", IntegerType()),
	StructField("fetched_at", StringType()),
	StructField("logical_hour", StringType()),
])

HOURS_PER_DAY = 24

def build_spark(app_name: str = "daily_stats") -> SparkSession:
	return ( SparkSession.builder.appName(app_name)
			.config("spark.sql.sources.partitionOverwriteMode", "dynamic") # overwrite only partitions we write 
			.config("spark.sql.session.timeZone", "UTC")
			.config("spark.sql.shuffle.partitions", "4")
			.getOrCreate()
	)
def compute_daily_stats(raw: DataFrame, dt: str) -> DataFrame:
	"""Pure transformation: DataFrame in, DataFrame out. Unit tests hit this directly."""
	"""i need to know how this works, as a whole and need a line by line breakdown"""
	ok = raw.filter((F.col("api_result") == 1) & F.col("player_count").isNotNull())
	return (
		ok.groupBy("appid", "game_name")
		.agg(
			F.max("player_count").alias("peak_players"),
			F.avg("player_count").alias("avg_players"),
			F.min("player_count").alias("min_players"),
			F.count_distinct("logical_hour").alias("snapshot_count"),
		)
		.withColumn("coverage_pct", F.round(F.col("snapshot_count") / HOURS_PER_DAY, 4))
		.withColumn("dt", F.lit(dt).cast("date"))
	)

def main() -> None:
	p = argparse.ArgumentParser()
	p.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
	p.add_argument("--lake", default="data")
	args = p.parse_args()

	spark = build_spark()
	raw_glob = F"{args.lake}/raw/player_counts/dt={args.date}/hour=*/snapshot.jsonl"
	raw = spark.read.schema(RAW_SCHEMA).json(raw_glob) # lazy nothing read yet 

	stats = compute_daily_stats(raw, args.date) # still lazy
	stats.explain() # print optimized plan

	(stats.write.mode("overwrite").partitionBy("dt")
		.parquet(f"{args.lake}/curated/daily_game_stats")) # job runs here

	print(f"daily_game_stats dt={args.date}: {stats.count()} games") # 2nd action: recomputes
	spark.stop()

if __name__ == "__main__":
	main()