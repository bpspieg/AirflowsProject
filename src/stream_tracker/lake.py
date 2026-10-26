"""Path builders for the data lake. Every path is keyed by date/hour, so reruns overwrite."""
from __future__ import annotations 

from datetime import datetime
from pathlib import Path

# i assume this means string OR path 
# key=value are hive style partitioning and spark understands them natively
def raw_snapshot_path(lake_root: str | Path, logical_hour: datetime) -> Path:
	return (Path(lake_root) / "raw" / "player_counts" / f"dt={logical_hour:%Y-%m-%d}" / f"hour={logical_hour:%H}" / "snapshot.jsonl")