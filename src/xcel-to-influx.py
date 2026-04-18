"""
Bill ingestion CLI -- thin wrapper around BillStorage.

Parses Xcel PDF bills from a directory and writes them to InfluxDB.
Run with: python xcel-to-influx.py [--config config.yaml]
"""

import argparse
from config import AppConfig
from bill_storage import BillStorage


def _cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse Xcel PDF bills and write to InfluxDB.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    return parser


def main() -> None:
    parser = _cli()
    args = parser.parse_args()
    config = AppConfig.load(args.config)
    storage = BillStorage(config)
    storage.process_directory(config.bills_directory)


if __name__ == "__main__":
    main()
