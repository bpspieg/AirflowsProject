"""Spark job 2: daily stats -> 7 day baseline, week over week change spike flag

spark-submit spark_jobs/trends.py --date 2026-10-22 --lake data
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOOKBACK_DAYS = 7
MIN_PRIOR_DAYS = 3 # dont flag spikes until some history

def build_spark(app_name: str = "trends") -> SparkSession:
	return (
		SparkSession.builder.appName(app_name)
		.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
		.config("spark.sql.session.timeZone", "UTC")
		.getOrCreate()
	)

def compute_trends(daily: DataFrame, dt: str, spike_threshold: float = 1.5) -> DataFrame:
	# Date -> integer day number, so the window frame can be measured in calendar days 
	day_num = F.datediff(F.col("dt"), F.lit("1970-01-01"))
	by_game = Window.partitionBy("appid").orderBy(day_num)
	prior_7 = by_game.rangeBetween(-LOOKBACK_DAYS, -1) # the 7 calendar days before this row 
	week_ago = by_game.rangeBetween(-7, -7) # exactly 7 days before (null if missing)

	return (
		daily
		.withColumn("peak_7d_avg", F.avg("peak_players").over(prior_7))
		.withColumn("prior_days", F.count("peak_players").over(prior_7))
		.withColumn("peak_week_ago", F.first("peak_players").over(week_ago))
		# Filter AFTER the windows: filtering first would throw away the history they need
		.filter(F.col("dt") == F.lit(dt).cast("date")) # what is DT exactly, other than a string 
		.withColumn(
			"wow_change_pct",
			F.round(F.try_divide(F.col("peak_players") - F.col("peak_week_ago"), F.col("peak_week_ago")) * 100, 2) # what does 2 do
		)
		.withColumn(
			"is_spike",
			F.coalesce(
				(F.col("prior_days") >= MIN_PRIOR_DAYS)
				& (F.col("peak_players") > spike_threshold * F.col("peak_7d_avg")),
				F.lit(False),
			),
		)
		.select("dt", "appid", "game_name", "peak_players", "peak_7d_avg", "wow_change_pct", "is_spike")
	)

def main() -> None:
	p = argparse.ArgumentParser()
	p.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
	p.add_argument("--lake", default="data")
	p.add_argument("--spike-threshold", type=float, default=1.5) # not sure what this means either other than obviously figuring out the spike threshold 
	args = p.parse_args()

	start = (date.fromisoformat(args.date) - timedelta(days=LOOKBACK_DAYS)).isoformat()

	spark = build_spark()
	daily = (
		spark.read.parquet(f"{args.lake}/curated/daily_game_stats")
		# Filter on partition column -> Spark only opens these 8 dt = folders
		.where(F.col("dt").between(F.lit(start).cast("date"), F.lit(args.date).cast("date")))
	)
	trends = compute_trends(daily, args.date, args.spike_threshold)

	(trends.write.mode("overwrite").partitionBy("dt")
		.parquet(f"{args.lake}/curated/game_trends"))

	n = trends.count()
	if n == 0:
		raise SystemExit(f"no daily_game_stats for dt={args.date}")  # non-zero exit -> Airflow task fails
	print(f"game_trends dt={args.date}: {n} games")
	spark.stop()

if __name__ == "__main__":
	main()