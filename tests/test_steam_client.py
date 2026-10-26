from datetime import datetime, timezone

import requests

from stream_tracker.steam_client import BASE_URL, fetch_player_count

HOUR = datetime(2026, 10, 21, 14, tzinfo=timezone.utc)


def test_success(requests_mock):
    requests_mock.get(BASE_URL, json={"response": {"player_count": 41234, "result": 1}})
    s = fetch_player_count(1091500, "Cyberpunk 2077", HOUR)
    assert s.player_count == 41234
    assert s.api_result == 1
    assert s.logical_hour == "2026-10-21T14:00:00+00:00"


def test_bad_result_code_is_null(requests_mock):
    requests_mock.get(BASE_URL, json={"response": {"result": 42}})
    s = fetch_player_count(1, "Missing", HOUR)
    assert s.player_count is None
    assert s.api_result == 42


def test_retries_429_then_succeeds(requests_mock):
    requests_mock.get(BASE_URL, [
        {"status_code": 429},
        {"json": {"response": {"player_count": 7, "result": 1}}},
    ])
    s = fetch_player_count(1, "Game", HOUR, backoff_s=0)
    assert s.player_count == 7
    assert requests_mock.call_count == 2


def test_gives_up_after_retries(requests_mock):
    requests_mock.get(BASE_URL, exc=requests.Timeout)
    s = fetch_player_count(1, "Game", HOUR, max_retries=2, backoff_s=0)
    assert s.player_count is None
    assert requests_mock.call_count == 3