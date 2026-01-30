from __future__ import annotations
from functools import reduce

import pandas as pd
from sqlalchemy import create_engine

from config import DBConnectionConfig, DataFetcherConfig

CHARGER_VEHICLE_SQL = """
WITH events AS (
    WITH 
cv as (
    SELECT c.id as charger_id, v.id as vehicle_id, c.price_area, c.grid_area, c.battery_kwh as user_battery_kwh 
    FROM charger c
    LEFT JOIN vehicle v
    ON v.account_id = c.account_id
    WHERE c.removed_at IS NULL AND v.removed_at IS NULL
),
ve as (
    SELECT 
        created_at as event_at, 
        vehicle_id, 
        is_plugged_in, 
        is_charging, 
        is_fully_charged, 
        battery_level_pct as enode_soc,
        battery_capacity_kwh as enode_battery_kwh
    FROM vehicle_event
    WHERE event_at >= NOW() - INTERVAL '%(history_days)s days'
)
SELECT ve.event_at, cv.charger_id, cv.vehicle_id, cv.price_area, cv.grid_area, cv.user_battery_kwh,
    ve.is_plugged_in, ve.is_charging, ve.is_fully_charged, ve.enode_soc, ve.enode_battery_kwh
FROM ve
LEFT JOIN cv
ON cv.vehicle_id = ve.vehicle_id
)
SELECT 
    -- Snaps the timestamp to the start of the 15-minute block
    to_timestamp(floor(extract(epoch FROM event_at) / 900) * 900) AT TIME ZONE 'UTC' AS time_block,
    charger_id,
    vehicle_id,
    
    -- Rule 1: Get the last value in the block
    (ARRAY_AGG(price_area ORDER BY event_at))[1] AS price_area,
    (ARRAY_AGG(grid_area ORDER BY event_at))[1] AS grid_area,
    (ARRAY_AGG(user_battery_kwh ORDER BY event_at))[1] AS user_battery_kwh,
    (ARRAY_AGG(enode_soc ORDER BY event_at))[1] AS enode_soc,
    (ARRAY_AGG(enode_battery_kwh ORDER BY event_at))[1] AS enode_battery_kwh,
    
    -- Rule 2: Pick the most frequent boolean value
    MODE() WITHIN GROUP (ORDER BY is_plugged_in) AS is_plugged_in,
    MODE() WITHIN GROUP (ORDER BY is_charging) AS is_charging,
    MODE() WITHIN GROUP (ORDER BY is_fully_charged) AS is_fully_charged

FROM events
GROUP BY 1, 2, 3
ORDER BY time_block, charger_id, vehicle_id;
"""

ALGO_SQL = """
WITH algo_event as (
    SELECT created_at, charger_id, ready_time, target_kwh 
    FROM charger_record
    WHERE created_at >= NOW() - INTERVAL '%(history_days)s days'
)
SELECT DISTINCT ON (time_block, charger_id)
    date_bin('15 minutes', created_at, TIMESTAMP '2025-01-01') AS time_block,
    charger_id,
    ready_time,
    target_kwh
FROM algo_event
ORDER BY time_block, charger_id, created_at;
"""

STATUSES_SQL = """
WITH RECURSIVE
charger_events as (
    SELECT updated_at, charger_id, connector_id, new_status 
    FROM evse_status
),
-- 1. Prepare Intervals: Get the start and end time for each status
intervals AS (
    SELECT
        charger_id,
        connector_id,
        new_status,
        updated_at AS valid_from,
        -- The status is valid until the next update, or NOW() if it's the latest
        LEAD(updated_at, 1, NOW()) OVER (
            PARTITION BY charger_id, connector_id 
            ORDER BY updated_at
        ) AS valid_to
    FROM charger_events
    WHERE updated_at >= NOW() - INTERVAL '%(history_days)s days'
),
-- 2. Define Boundaries: Find the total time range to generate the 15-min grid
bounds AS (
    SELECT 
        date_trunc('hour', MIN(valid_from)) AS min_time,
        date_trunc('hour', MAX(valid_to)) + interval '1 hour' AS max_time
    FROM intervals
),
-- 3. Generate Grid: Create 15-minute buckets (00, 15, 30, 45)
buckets AS (
    SELECT generate_series(min_time, max_time, interval '15 minutes') AS bucket_start
    FROM bounds
),
-- 4. Slice & Calculate: Join intervals to buckets and calculate overlap duration
bucket_stats AS (
    SELECT
        b.bucket_start,
        i.charger_id,
        i.connector_id,
        i.new_status,
        -- Calculate how many seconds this status existed within this specific 15m bucket
        EXTRACT(EPOCH FROM (
            LEAST(i.valid_to, b.bucket_start + interval '15 minutes') - 
            GREATEST(i.valid_from, b.bucket_start)
        )) AS duration_seconds
    FROM intervals i
    JOIN buckets b 
      -- Join only if the status interval overlaps with the bucket
      ON i.valid_from < b.bucket_start + interval '15 minutes'
     AND i.valid_to > b.bucket_start
),
-- 5. Aggregate: Sum durations (in case a status toggles back and forth within 15m)
aggregated_stats AS (
    SELECT
        bucket_start,
        charger_id,
        connector_id,
        new_status,
        SUM(duration_seconds) AS total_seconds
    FROM bucket_stats
    GROUP BY 1, 2, 3, 4
),
-- 6. Rank: Pick the status with the longest duration per bucket/charger/connector
ranked_stats AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY bucket_start, charger_id, connector_id 
            ORDER BY total_seconds DESC
        ) as rn
    FROM aggregated_stats
)
-- Final Result: Filter for the top ranked status
SELECT
    bucket_start AS time_block,
    charger_id,
    connector_id,
    new_status as status
FROM ranked_stats
WHERE rn = 1
ORDER BY time_block, charger_id, connector_id;
"""

HB_SQL = """
WITH
bounds AS (
  SELECT
    date_bin('15 minutes', MIN(recorded_at), '2000-01-01 00:00:00+00'::timestamptz) AS start_block,
    date_bin('15 minutes', MAX(recorded_at), '2000-01-01 00:00:00+00'::timestamptz) AS end_block
  FROM heartbeat
  WHERE recorded_at >= NOW() - INTERVAL '%(history_days)s days'
),
chargers AS (
  SELECT DISTINCT charger_id
  FROM heartbeat
),
blocks AS (
  SELECT
    gs AS time_block,
    gs + interval '15 minutes' AS time_block_end
  FROM bounds b
  CROSS JOIN LATERAL generate_series(b.start_block, b.end_block, interval '15 minutes') AS gs
),

-- Each heartbeat keeps the charger "connected" for 30s after it.
hb_intervals AS (
  SELECT
    charger_id,
    recorded_at AS start_time,
    recorded_at + interval '30 seconds' AS end_time
  FROM heartbeat
),

-- Merge overlapping 30s intervals per charger (gap-and-island).
ordered AS (
  SELECT
    *,
    max(end_time) OVER (
      PARTITION BY charger_id
      ORDER BY start_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_max_end
  FROM hb_intervals
),
grouped AS (
  SELECT
    *,
    sum(
      CASE WHEN prev_max_end IS NULL OR start_time > prev_max_end THEN 1 ELSE 0 END
    ) OVER (PARTITION BY charger_id ORDER BY start_time) AS island_id
  FROM ordered
),
connected_spans AS (
  SELECT
    charger_id,
    min(start_time) AS connected_start,
    max(end_time)   AS connected_end
  FROM grouped
  GROUP BY charger_id, island_id
),

per_block AS (
  SELECT
    bl.time_block,
    c.charger_id,
    coalesce(
      sum(
        extract(epoch FROM
          least(cs.connected_end,   bl.time_block_end)
          - greatest(cs.connected_start, bl.time_block)
        )
      ),
      0
    ) AS connected_seconds
  FROM blocks bl
  CROSS JOIN chargers c
  LEFT JOIN connected_spans cs
    ON cs.charger_id = c.charger_id
   AND cs.connected_end   > bl.time_block
   AND cs.connected_start < bl.time_block_end
  GROUP BY bl.time_block, c.charger_id
)

SELECT
  time_block,
  charger_id,
  (connected_seconds >= 450) AS is_connected   -- 7.5 minutes = 450 seconds
FROM per_block
ORDER BY time_block, charger_id;
"""


class DataFetcher:
    def __init__(self, cfg: DataFetcherConfig):
        DataFetcher._validate_cfg(cfg.evpdb_cfg)
        DataFetcher._validate_cfg(cfg.logdb_cfg)
        self.cfg = cfg

    def _load_data(self, history_days: int | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if history_days is None:
            history_days = 365 * 10

        logdb_engine = create_engine(self.cfg.logdb_cfg.connection_str())
        evpdb_engine = create_engine(self.cfg.evpdb_cfg.connection_str())

        # Static Info
        veh_events = pd.read_sql(CHARGER_VEHICLE_SQL, evpdb_engine, params={"history_days": history_days})
        algo = pd.read_sql(ALGO_SQL, evpdb_engine, params={"history_days": history_days})
        statuses = pd.read_sql(STATUSES_SQL, logdb_engine, params={"history_days": history_days})
        hb = pd.read_sql(HB_SQL, logdb_engine, params={"history_days": history_days})

        return veh_events, algo, statuses, hb

    def process_charging_data(self, history_days: int | None):
        # Load Data
        df_evp, df_schedule, df_status, df_conn = self._load_data(history_days)

        for df in [df_evp, df_schedule, df_status, df_conn]:
            if not df.empty and "charger_id" in df.columns:
                df["charger_id"] = pd.to_numeric(df["charger_id"], errors="coerce").astype("Int64")

        # Ensure Datetime types
        for df in [df_evp, df_schedule, df_status, df_conn]:
            if not df.empty:
                df["time_block"] = pd.to_datetime(df["time_block"], utc=True)

        df_evp.to_csv("evp.csv", index=False, sep="\t")
        df_schedule.to_csv("schedule.csv", index=False, sep="\t")
        df_status.to_csv("status.csv", index=False, sep="\t")
        df_conn.to_csv("conn.csv", index=False, sep="\t")

        dfs = [df_evp, df_schedule, df_status, df_conn]

        final_df = reduce(lambda left, right: pd.merge(left, right, on=["time_block", "charger_id"], how="outer"), dfs)

        # battery_level_pct mapped from enode_soc
        final_df["battery_level_pct"] = final_df["enode_soc"]

        # target_kwh duplication
        final_df["target_kwh_min"] = final_df["target_kwh"]
        final_df["target_kwh_max"] = final_df["target_kwh"]

        # Rename is_connected to is_charger_connected
        final_df = final_df.rename(columns={"is_connected": "is_charger_connected"})
        final_df["connector_id"] = pd.to_numeric(final_df["connector_id"], errors="coerce").astype("Int64")

        # Select specific output columns in order
        output_columns = [
            "time_block",
            "charger_id",
            "connector_id",
            "vehicle_id",
            "price_area",
            "grid_area",
            "user_battery_kwh",
            "enode_battery_kwh",
            "is_plugged_in",
            "is_charging",
            "is_fully_charged",
            "battery_level_pct",
            "ready_time",
            "target_kwh_min",
            "target_kwh_max",
            "status",
            "is_charger_connected",
        ]

        return final_df[output_columns]

    @staticmethod
    def _validate_cfg(cfg: DBConnectionConfig):
        def not_empty(key: str):
            if not getattr(cfg, key):
                raise ValueError(f"Config key {key} is empty")

        not_empty("host")
        not_empty("port")
        not_empty("user")
        not_empty("password")
        not_empty("database")
