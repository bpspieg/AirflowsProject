from daily_stats import RAW_SCHEMA, compute_daily_stats


def row(appid, name, count, hour, result=1):
    return (appid, name, count, result,
            f"2026-10-21T{hour:02d}:01:00+00:00", f"2026-10-21T{hour:02d}:00:00+00:00")


def test_aggregates_and_coverage(spark):
    raw = spark.createDataFrame([
        row(1, "A", 100, 0),
        row(1, "A", 300, 1),
        row(1, "A", 200, 2),
        row(1, "A", None, 3, result=42),  # failed fetch: excluded
        row(2, "B", 50, 0),
    ], RAW_SCHEMA)

    out = {r.appid: r for r in compute_daily_stats(raw, "2026-10-21").collect()}

    a = out[1]
    assert (a.peak_players, a.min_players, a.avg_players) == (300, 100, 200.0)
    assert a.snapshot_count == 3
    assert a.coverage_pct == round(3 / 24, 4)
    assert str(a.dt) == "2026-10-21"
    assert out[2].snapshot_count == 1


def test_duplicate_hour_counts_once(spark):
    raw = spark.createDataFrame([row(1, "A", 100, 5), row(1, "A", 110, 5)], RAW_SCHEMA)
    (r,) = compute_daily_stats(raw, "2026-10-21").collect()
    assert r.snapshot_count == 1