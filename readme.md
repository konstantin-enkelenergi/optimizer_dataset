# Optimizer dataset builder

## Prerequisites:

1. Python 3.12+
2. Access to EVPDB and LOGDB
3. Archive with historical MAS logs

## Setup

1. Create a virtual environment
    ```sh
    python3 -m venv .venv
    ```
2. Activate
    ```sh
    source .venv/bin/activate
    ```
3. Install dependencies
    ```sh
    pip install -r requirements.txt
    ```
4. Copy the .env file template and specify the DB credentials
    ```sh
    cp .env.example .env
    ```

## Exporting the data

The script 
1. pulls the raw data between `reference_date - history_days` and `reference_date` from the databases and MAS logs at `logs`; 
2. creates a dataset and saves it to the CSV file `out`. 

```sh
$ python data_tool.py -h
usage: data_tool.py [-h] --out OUT --logs LOGS [--history_days HISTORY_DAYS] [--reference_date REFERENCE_DATE]

Data fetching tool for the Optimizer project

options:
  -h, --help            show this help message and exit
  --out OUT             Path to the result csv file
  --logs LOGS           Path to the logs root dir with *.gz files
  --history_days HISTORY_DAYS
                        Number of days to fetch data for. If not set, all available data will be fetched.
  --reference_date REFERENCE_DATE
                        Reference date in ISO format (YYYY-MM-DDTHH:MM:SS). If not set, the current time is used.
```

Example:

```sh
python data_tool.py --out data.csv --reference_date 2025-12-16T00:00:00 --history_days 15 --logs ./mas_logs/
```

Typical work duration starts from ~3 minutes and depends on `history_days`.

## MAS logs

The script expects the data in the following format:
```sh
 tree mas_logs
./mas_logs/
├── 20250629080614-mas.log.gz
├── 20250701101819-mas.log.gz
├── 20250705134828-mas.log.gz
├── 20250708085037-mas.log.gz
...
└── 20251210231258-mas.log.gz
```

1. The files must have a `.gz` extension.
2. Be valid gzip archives with a text log file.
3. File names can be arbitrary.

## Columns

- `time_block` - timestamp of the 15-min block, `UTC timestamp`. the dataset is sorted from the oldest entries to the newest.
- `charger_id`- internal unique id of the charger, `int`.
- `connector_id` - index of the connector, `int`. One charger can have many connectors. Not always present. For now, only `1` is used.
- `vehicle_id` - internal unique vehicle id, `int`.
- `price_area` - "DK1" or "DK2", `string`.
- `grid_area` - `int`.
- `user_battery_kwh` - battery capacity provided by the user via the app, `float`.
- `enode_battery_kwh` - battery capacity from the vehicle API via Enode, `float`.
- `is_plugged_in` - Enode flag, `bool`.
- `is_charging` - Enode flag, `bool`.
- `is_fully_charged`  - Enode flag, `bool`.
- `battery_level_pct` - state of charge, `int`, takes value in `[0, 100]`.
- `ready_time` - user-provided desired hour and minute for 100% of charge, 24h format, `string`. Example: `"6:00"`. Can be empty.
- `target_kwh_min` - user-provided desired target SoC, `float`. Can be empty.
- `target_kwh_max` - duplicated from `target_kwh_min`.
- `status` - OCPP standard status, `string`. Only `Available` reliably indicates that the EV is not plugged. [Here is a good cheatsheet](https://www.getflipturn.com/blog/understanding-ocpp-statuses) for the meaning of the statuses. It is the "longest" status within the 15m block. Can be empty if no status update arrived during the block.
- `is_charger_connected` - this flag, `bool`, is derived from the heartbeat stream. If the charger didn't send heartbeats for 30s, it was considered disconnected. If the charger is disconnected for over 7.5 minutes within a 15m block, it is considered disconnected. **If the heartbeat data is not available, the column is empty**.

Target time and SoC can be empty. It means that we don't have records of these users setting the preferences. In this case, the defaults can be used: `08:00` and `80%`.   