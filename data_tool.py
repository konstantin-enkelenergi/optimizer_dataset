from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys
import time

from data_fetcher import DataFetcher, DataFetcherConfig


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Data fetching tool for the Optimizer project")
    parser.add_argument("--out", type=Path, required=True, help="Path to the result csv file")
    parser.add_argument("--logs", type=Path, required=True, help="Path to the logs root dir with *.gz files")
    parser.add_argument(
        "--history_days",
        type=int,
        default=None,
        help="Number of days to fetch data for. If not set, all available data will be fetched.",
    )
    parser.add_argument(
        "--reference_date",
        type=datetime.fromisoformat,
        default=datetime.now(),
        help="Reference date in ISO format (YYYY-MM-DDTHH:MM:SS). If not set, the current time is used.",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s", stream=sys.stdout)
    args = parse_args()
    if args.history_days is not None and args.history_days <= 0:
        raise ValueError("history_days must be a positive integer")

    cfg = DataFetcherConfig.load()
    from_time = args.reference_date - timedelta(days=args.history_days) if args.history_days is not None else None
    start = time.time()
    data = DataFetcher(cfg).process_charging_data(
        to_time=args.reference_date, from_time=from_time, mas_log_root=args.logs
    )
    data.to_csv(args.out, index=False, single_file=True)
    elapsed = timedelta(seconds=time.time() - start)
    logging.info(f"Done. Time elapsed: {elapsed}")


if __name__ == "__main__":
    main()
