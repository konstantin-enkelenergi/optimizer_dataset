from argparse import ArgumentParser, Namespace
from pathlib import Path

from data_fetcher import DataFetcher, DataFetcherConfig


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Data fetching tool for the Optimizer project")
    parser.add_argument("--out", type=Path, required=True, help="Path to the result csv file")
    parser.add_argument(
        "--history_days",
        type=int,
        default=None,
        help="Number of days to fetch data for. If not set, all available data will be fetched.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.history_days <= 0:
        raise ValueError("history_days must be a positive integer")

    cfg = DataFetcherConfig.load()
    data = DataFetcher(cfg).process_charging_data(args.history_days)
    print(data.head())
    data.to_csv(args.out, index=False)


if __name__ == "__main__":
    main()
