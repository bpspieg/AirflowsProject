CREATE TABLE IF NOT EXISTS daily_game_stats (
    dt              date             NOT NULL,
    appid           bigint           NOT NULL,
    game_name       text             NOT NULL,
    peak_players    bigint,
    avg_players     double precision,
    min_players     bigint,
    snapshot_count  int,
    coverage_pct    double precision,
    loaded_at       timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, appid)
);

CREATE TABLE IF NOT EXISTS game_trends (
    dt              date             NOT NULL,
    appid           bigint           NOT NULL,
    game_name       text             NOT NULL,
    peak_players    bigint,
    peak_7d_avg     double precision,
    wow_change_pct  double precision,
    is_spike        boolean          NOT NULL,
    loaded_at       timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, appid)
);