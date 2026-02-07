from __future__ import annotations
from datetime import datetime
import gzip
import logging
from pathlib import Path
import re
from typing import Callable, TextIO

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import create_engine
from tqdm import tqdm

from config import DBConnectionConfig, DataFetcherConfig

CHARGER_VEHICLE_SQL = """
WITH 
cv as (
    SELECT c.id as charger_id, c.serial_id, v.id as vehicle_id, c.price_area, c.grid_area, c.battery_kwh as user_battery_kwh 
    FROM charger c
    LEFT JOIN vehicle v
    ON v.account_id = c.account_id
    WHERE c.removed_at IS NULL AND v.removed_at IS NULL and c.id != -1
),
ve as (
    SELECT 
        created_at as event_at, 
        vehicle_id, 
        is_plugged_in, 
        is_charging, 
        is_fully_charged, 
        battery_level_pct,
        battery_capacity_kwh as enode_battery_kwh
    FROM vehicle_event
    WHERE event_at >= %(from_time)s::timestamp AND event_at <= %(to_time)s::timestamp
)
SELECT ve.event_at as at, cv.charger_id, cv.serial_id, cv.vehicle_id, cv.price_area, cv.grid_area, cv.user_battery_kwh,
    ve.is_plugged_in, ve.is_charging, ve.is_fully_charged, ve.battery_level_pct, ve.enode_battery_kwh
FROM ve
LEFT JOIN cv
ON cv.vehicle_id = ve.vehicle_id
WHERE cv.charger_id IS NOT NULL
ORDER BY at;
"""

STATUSES_SQL = """
SELECT updated_at as at, charger_id, connector_id, new_status as status
FROM evse_status
WHERE updated_at >= %(from_time)s::timestamp AND updated_at <= %(to_time)s::timestamp AND charger_id != -1
ORDER BY at;
"""

HB_SQL = """
WITH heartbeat_analysis AS (
    SELECT
        charger_id,
        recorded_at,
        LAG(recorded_at) OVER w AS prev_heartbeat,
        LEAD(recorded_at) OVER w AS next_heartbeat
    FROM heartbeat
    -- Expanded Window: Look back and forward to capture context
    -- Buffering by 30s is the bare minimum; slightly larger is safer to catch the "bridge" heartbeats.
    WHERE recorded_at >= %(from_time)s::timestamp AND recorded_at <= %(to_time)s::timestamp AND charger_id != -1
    WINDOW w AS (PARTITION BY charger_id ORDER BY recorded_at)
)
SELECT event_at as at, charger_id, is_connected as is_charger_connected
FROM (
    SELECT
        charger_id,
        recorded_at AS event_at,
        true AS is_connected
    FROM heartbeat_analysis
    WHERE prev_heartbeat IS NULL 
       OR recorded_at > prev_heartbeat + interval '30 seconds'

    UNION ALL

    SELECT
        charger_id,
        recorded_at + interval '30 seconds' AS event_at,
        false AS is_connected
    FROM heartbeat_analysis
    WHERE next_heartbeat IS NULL 
       OR next_heartbeat > recorded_at + interval '30 seconds'
) events
-- Final Filter: Only show events that actually happened within the requested window
ORDER BY event_at, charger_id;
"""


class DataFetcher:
    def __init__(self, cfg: DataFetcherConfig):
        DataFetcher._validate_cfg(cfg.evpdb_cfg)
        DataFetcher._validate_cfg(cfg.logdb_cfg)
        self.cfg = cfg

    def _ffill_schedule_partition(self, df: dd.DataFrame) -> dd.DataFrame:
        sdf: dd.DataFrame = df.sort_values("time_block")
        sdf[["ready_time", "target_soc"]] = sdf.groupby("serial_id")[["ready_time", "target_soc"]].ffill()
        return sdf

    def _fill_partition(self, df: dd.DataFrame) -> dd.DataFrame:
        # This runs on each partition individually
        ids = ["charger_id", "vehicle_id"]
        if df["is_charger_connected"].count() > 0:
            df["is_charger_connected"] = df["is_charger_connected"].fillna(False)
        # Sort inside the partition to ensure time is correct for ffill
        df = df.sort_values("time_block")
        cols = list(df.columns.difference(ids))
        df[cols] = df.groupby(ids)[cols].ffill()
        df[cols] = df.groupby(ids)[cols].bfill()
        return df

    def _agg_last(self, column: pd.Series):
        if column.empty:
            return None
        return column.iloc[-1]

    def _agg_mode(self, column: pd.Series):
        m = column.mode()
        if m.empty:
            return None
        return m.iloc[0]

    def _as_s_utc(self, s: pd.Series) -> pd.Series:
        s = pd.to_datetime(s, utc=True)
        # pandas 2.x supports unit conversion on datetime64[*, tz]
        if hasattr(s.dt, "as_unit"):
            return s.dt.as_unit("s")  # type: ignore
        # fallback
        return s.astype("datetime64[s]")

    def _aggregate(
        self, df: pd.DataFrame, ids: list[str], agg: dict[str, Callable], time_col: str = "at"
    ) -> pd.DataFrame:
        if df.empty:
            df = df.rename(columns={time_col: "time_block"})
            # force dtype even for empty frames
            df["time_block"] = self._as_s_utc(df["time_block"])
            return df

        # normalize input time column BEFORE resample
        df = df.copy()
        df[time_col] = self._as_s_utc(df[time_col])

        resampled = (df.groupby(ids).resample("15min", on=time_col).agg(agg)).reset_index()  # type: ignore
        group = resampled.groupby(ids)
        cols = df.columns.difference(ids)
        resampled[cols] = group[cols].ffill()

        resampled = resampled.rename(columns={time_col: "time_block"})
        resampled["time_block"] = self._as_s_utc(resampled["time_block"])
        return resampled

    def _parse_eco_log(self, log_line: str) -> tuple[datetime, str, str, int] | None:
        if "Updated eco mode data fetched successfully" not in log_line:
            return None

        pattern = r"^\[(?P<ts>[\d. :]+)\].*?chid:\s(?P<chid>\w+).*?\)\s(?P<time>\d{1,2}:\d{2}).*?int32=(?P<soc>\d{1,3})"

        match = re.search(pattern, log_line)

        if match:
            # Extract raw strings
            raw_ts = match.group("ts")
            chid = match.group("chid")
            ready_time = match.group("time")
            target_soc = match.group("soc")
            try:
                timestamp = datetime.strptime(raw_ts, "%d.%m.%Y %H:%M:%S")
                return timestamp, chid, ready_time, int(target_soc)
            except ValueError as e:
                logging.error(e)
        return None

    def _parse_file(self, file: TextIO, data: dict[str, list[str | datetime | int]]):
        for line in file:
            result = self._parse_eco_log(line)
            if result:
                data["at"].append(result[0])
                data["serial_id"].append(result[1])
                data["ready_time"].append(result[2])
                data["target_soc"].append(result[3])

    def _load_logs(self, root: Path) -> pd.DataFrame:
        logging.info("Loading logs...")
        data = {
            "at": [],
            "serial_id": [],
            "ready_time": [],
            "target_soc": [],
        }
        files = list(root.glob("*.gz"))
        for zipped in tqdm(files):
            with gzip.open(zipped, "rt") as f:
                self._parse_file(f, data)
        return pd.DataFrame(data)

    def _load_data(
        self,
        from_time: datetime | None,
        to_time: datetime | None,
        log_root: Path,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logdb_engine = create_engine(self.cfg.logdb_cfg.connection_str())
        evpdb_engine = create_engine(self.cfg.evpdb_cfg.connection_str())

        from_time = from_time or datetime.min
        to_time = to_time or datetime.now()
        params = {"from_time": from_time, "to_time": to_time}

        logging.info("Quering databases...")
        # Static Info
        veh_events = pd.read_sql(
            CHARGER_VEHICLE_SQL,
            evpdb_engine,
            params=params,
            parse_dates=["at"],
            dtype={"charger_id": "Int16", "vehicle_id": "Int16", "grid_area": "Int16"},
        )
        statuses = pd.read_sql(
            STATUSES_SQL,
            logdb_engine,
            params=params,
            parse_dates=["at"],
            dtype={"charger_id": "Int16", "connector_id": "Int8"},
        )
        hb = pd.read_sql(HB_SQL, logdb_engine, params=params, parse_dates=["at"], dtype={"charger_id": "Int16"})
        algo = self._load_logs(log_root)

        return veh_events, algo, statuses, hb

    def process_charging_data(
        self, from_time: datetime | None, to_time: datetime | None, mas_log_root: Path
    ) -> dd.DataFrame:
        # Load Data
        df_evp, df_schedule, df_status, df_conn = self._load_data(from_time, to_time, mas_log_root)

        logging.info("Aggregating the data into 15m blocks...")

        # Ensure Datetime types
        for df in [df_evp, df_schedule, df_status, df_conn]:
            if not df.empty:
                df["at"] = pd.to_datetime(df["at"], utc=True)

        # raw_dest = Path("raw")
        # raw_dest.mkdir(exist_ok=True)
        # df_evp.to_csv(raw_dest / "evp.csv", index=False, sep="\t")
        # df_schedule.to_csv(raw_dest / "schedule.csv", index=False, sep="\t")
        # df_status.to_csv(raw_dest / "status.csv", index=False, sep="\t")
        # df_conn.to_csv(raw_dest / "conn.csv", index=False, sep="\t")

        df_evp = self._aggregate(
            df_evp,
            ["charger_id", "vehicle_id"],
            {
                "serial_id": self._agg_last,
                "price_area": self._agg_last,  # type: ignore
                "grid_area": self._agg_last,
                "user_battery_kwh": self._agg_last,
                "is_plugged_in": self._agg_mode,
                "is_charging": self._agg_mode,
                "is_fully_charged": self._agg_mode,
                "battery_level_pct": self._agg_last,
                "enode_battery_kwh": self._agg_last,
            },
        )
        df_schedule = self._aggregate(
            df_schedule,
            ["serial_id"],
            {
                "ready_time": self._agg_last,
                "target_soc": self._agg_last,
            },
        )
        df_status = self._aggregate(
            df_status,
            ["charger_id", "connector_id"],
            {"status": self._agg_mode},
        )
        df_conn = self._aggregate(
            df_conn,
            ["charger_id"],
            {"is_charger_connected": self._agg_mode},
        )

        # df_evp.to_csv(raw_dest / "evp_resampled.csv", index=False, sep="\t")
        # df_schedule.to_csv(raw_dest / "schedule_resampled.csv", index=False, sep="\t")
        # df_status.to_csv(raw_dest / "status_resampled.csv", index=False, sep="\t")
        # df_conn.to_csv(raw_dest / "conn_resampled.csv", index=False, sep="\t")

        logging.info("Merging the tables...")
        dd_evp: dd.DataFrame = dd.from_pandas(df_evp, npartitions=4)
        dd_schedule: dd.DataFrame = dd.from_pandas(df_schedule, npartitions=4)
        dd_status: dd.DataFrame = dd.from_pandas(df_status, npartitions=4)
        dd_conn: dd.DataFrame = dd.from_pandas(df_conn, npartitions=4)

        final_dd: dd.DataFrame = dd_evp.merge(dd_schedule, on=["time_block", "serial_id"], how="outer")
        # Put all rows for the same serial_id in the same partition, then ffill locally
        final_dd["serial_id"] = final_dd["serial_id"].fillna("X")
        final_dd = final_dd.set_index("serial_id", drop=True, shuffle="tasks")
        final_dd = final_dd.map_partitions(self._ffill_schedule_partition, meta=final_dd._meta)
        final_dd = final_dd[final_dd["charger_id"].notnull()]
        final_dd = final_dd.reset_index()

        final_dd: dd.DataFrame = final_dd.merge(dd_status, on=["time_block", "charger_id"], how="left")
        final_dd: dd.DataFrame = final_dd.merge(dd_conn, on=["time_block", "charger_id"], how="left")

        logging.info("Filling the gaps...")
        # Now we can apply the fill logic to each partition independently without talking to other partitions
        final_dd = final_dd.map_partitions(self._fill_partition)

        # Only sort by time at the very end if strictly necessary for the CSV output
        # Warning: This is still expensive. If you can consume the CSV unsorted, remove this.
        final_dd = final_dd.sort_values(by="time_block")
        final_dd["target_soc_min"] = final_dd["target_soc"]
        final_dd["target_soc_max"] = final_dd["target_soc"]

        output_columns = [
            "time_block",
            "charger_id",
            "connector_id",
            "vehicle_id",
            "price_area",
            "grid_area",
            "user_battery_kwh",
            "enode_battery_kwh",
            "battery_level_pct",
            "is_plugged_in",
            "is_charging",
            "is_fully_charged",
            "ready_time",
            "target_soc_min",
            "target_soc_max",
            "status",
            "is_charger_connected",
        ]
        # if final_dd["is_charger_connected"].count().compute() > 0:
        #     output_columns.append("is_charger_connected")
        final_dd = final_dd[output_columns]
        return final_dd

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
