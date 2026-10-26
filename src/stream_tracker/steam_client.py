"""Thin Steam Web API client: one call is one games current player count."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import requests

BASE_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

@dataclass
class Snapshot:
	appid: int
	game_name: str
	player_count: int | None 	# None is for failed fetch
	api_result: int | None
	fetched_at: str				# this is for the partition key 
	logical_hour: str

	def to_dict(self) -> dict:
		return asdict(self)

def fetch_player_count(
	appid: int,
	game_name: str,
	logical_hour: datetime,
	session: requests.Session | None = None,
	max_retries: int = 3,
	backoff_s: float = 2.0,
	timeout_s: float = 10.0,
) -> Snapshot:
	""" Fetch one games count, Retries 429 / 5xx / timeouts with eponential backoff.

	Never raises for API-level failures: return None"""
	http = session or requests.Session()
	result: int | None = None
	count: int | None = None
	for attempt in range(max_retries + 1):
		try:
			resp = http.get(BASE_URL, params={"appid": appid}, timeout=timeout_s)
			if resp.status_code == 429 or resp.status_code >= 500:
				raise requests.HTTPError(f"HTTP {resp.status_code}")
			resp.raise_for_status()
			body = resp.json().get("response", {})
			result = body.get("result")
			count = body.get("player_count") if result == 1 else None
			break
		except (requests.HTTPError, requests.ConnectionError, requests.Timeout):
			if attempt == max_retries:
				break
			time.sleep(backoff_s * (2 ** attempt)) 

	return Snapshot(
		appid=appid,
		game_name=game_name,
		player_count=count,
		api_result=result,
		fetched_at=datetime.now(timezone.utc).isoformat(),
		logical_hour=logical_hour.astimezone(timezone.utc).isoformat(),
	)