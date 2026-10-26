"""Hourly: snapshot current player counts for every game in games.yaml."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import Asset, dag, get_current_context, task

from stream_tracker.lake import raw_snapshot_path
from stream_tracker.steam_client import fetch_player_count

LAKE_ROOT = "/opt/airflow/data"
GAMES_FILE = "/opt/airflow/config/games.yaml"
MIN_OK_RATIO = 0.8 # what exactly is this 

# Daily DAG will be scheduled on this asset instead of a clock
RAW_PLAYER_COUNTS = Asset("file:///opt/airflow/data/raw/player_counts")

@dag(
	dag_id="steam_ingest_hourly",
	schedule="@hourly",
	start_date=pendulum.datetime(2026, 10, 22, tz="UTC"), # this should be updated to be any given day
	catchup=False, # dont create runs for every hour since start_date 
	max_active_runs=1, # one hour at a time
	default_args={
		"retries": 2,
		"retry_delay": timedelta(minutes=1),
		"retry_exponential_backoff": True, # i thought we defined this in the spark job?
	},
	tags=["steam"],
)

def steam_ingest_hourly():

	@task 
	def snapshot_hour() -> str:
		"""Pick the hour this run reprsents, once and pass it to everything downstream"""
		ctx = get_current_context()
		# Shceduled run: end of the interview
		# Manual run in airflow 3 may have no interval -> fall back to now
		t = ctx.get("data_interval_end") or pendulum.now("UTC")
		return t.in_timezone("UTC").replace(minute=0, second=0, microsecond=0).isoformat()


	@task
	def load_game_list() -> list[dict]:
		return yaml.safe_load(Path(GAMES_FILE).read_text())["games"]

	@task(map_index_template="{{ game_label }}", max_active_tis_per_dagrun = 4)
	def fetch(game: dict, hour_iso: str) -> dict:
		# Name each mapped instance after the game in the UI instead of 0 1 2 etc
		get_current_context()["game_label"] = game["name"]
		snap = fetch_player_count(game["appid"], game["name"], datetime.fromisoformat(hour_iso))
		return snap.to_dict()

	@task
	def write_raw_snapshot(snapshots: list[dict], hour_iso: str) -> dict:
		rows = list(snapshots) # mapped results arrive as a lazy sequence (what the hell does lazy mean here)
		out = raw_snapshot_path(LAKE_ROOT, datetime.fromisoformat(hour_iso))
		out.parent.mkdir(parents=True, exist_ok=True)
		out.write_text("".join(json.dumps(r) + "\n" for r in rows))

		failed = [r["game_name"] for r in rows if r["player_count"] is None]
		print(f"wrote {out}: {len(rows) - len(failed)/len(rows)} ok, failed={failed}")
		return {"path": str(out), "ok": len(rows) - len(failed), "total": len(rows)}

	@task(retries=0, outlets=[RAW_PLAYER_COUNTS]) # deterministic check:retrying wont help. what does this even mean
	def check_snapshot(summary: dict) -> None:
		ratio = summary["ok"] / summary["total"]
		if ratio < MIN_OK_RATIO:
			raise ValueError(f"only {ratio:.0%} of games fetched ({summary})")

	hour = snapshot_hour()
	games = load_game_list()
	snaps = fetch.partial(hour_iso=hour).expand(game=games)
	summary = write_raw_snapshot(snaps, hour)
	check_snapshot(summary)

steam_ingest_hourly()



