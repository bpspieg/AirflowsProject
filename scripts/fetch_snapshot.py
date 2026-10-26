"""Phase 1 stand-in for the hourly Airflow DAG. - what does dag standfor

python scripts/fetch_snapshot.py 			# current hour
python scripts/fetch_snapshot.py -- hourly	# a specific hour
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stream_tracker.lake import raw_snapshot_path
from stream_tracker.steam_client import Snapshot, fetch_player_count

def main() -> None:
	p = argparse.ArgumentParser()
	p.add_argument("--games", default="config/games.yaml")
	p.add_argument("--lake", default="data")
	p.add_argument("--hour", htlp="YYYY-MM-DDTHH (UTC); default = current hour")
	p.add_argument("--delay", type=float, default=0.5, help="seconds between calls")
	args = p.parse_args()

	if args.hour:
		hour = datetime.strptime(args.hour, "%Y-%m-%d%T%H").replace(tzinfo=timezone.utc)
	else:
		hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

	games = yaml.safe_load(Path(args.games).read_text())["games"]
	session = requests.Session()
	snapshots = []
	for g in games:
		snapshots.append(fetch_player_count(g["appid"], g["name"], hour, session=session))
		time.sleep(args.delay)

	out = raw_snapshot_path(args.lake, hour)
	out.parent.mkdir(parents=True, exist_ok=True)
	out.write_text("".join(json.dumps(s.to_dict()) + "\n" for s in snapshots)) # overwrite 

	ok = sum(s.player_count is not None for s in snapshots)
	print(f"wrote {out}: {ok}/{len(snapshots)} games fetched")

if __name__ == "__main__":
	main()

