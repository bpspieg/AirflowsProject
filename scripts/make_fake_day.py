"""Generate a full day of fake hourly snapshots so the Spark job has 24 hours to chew on.

  python scripts/make_fake_day.py --date 2026-10-20
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stream_tracker.lake import raw_snapshot_path  # noqa: E402

BASELINES = {1091500: 25_000, 730: 1_200_000, 570: 600_000}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--games", default="config/games.yaml")
    p.add_argument("--lake", default="data")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    random.seed(args.seed)
    games = yaml.safe_load(Path(args.games).read_text())["games"]
    day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    for h in range(24):
        hour = day + timedelta(hours=h)
        diurnal = 0.7 + 0.3 * math.sin((h - 8) / 24 * 2 * math.pi)  # evening peak
        lines = []
        for g in games:
            base = BASELINES.get(g["appid"], 10_000)
            failed = random.random() < 0.03  # ~3% simulated API failures
            lines.append(json.dumps({
                "appid": g["appid"], "game_name": g["name"],
                "player_count": None if failed else int(base * diurnal * random.uniform(0.9, 1.1)),
                "api_result": 42 if failed else 1,
                "fetched_at": (hour + timedelta(minutes=1)).isoformat(),
                "logical_hour": hour.isoformat(),
            }) + "\n")
        out = raw_snapshot_path(args.lake, hour)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("".join(lines))
    print(f"wrote 24 fake snapshots for {args.date}")


if __name__ == "__main__":
    main()