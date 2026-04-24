"""
CLI for TOU billing comparison -- Xcel Energy South Dakota.

Compares actual costs under standard flat-rate billing against the
Residential Time-of-Day rate plan using interval energy data from
Home Assistant InfluxDB.

Usage:
    python tou_comparison.py --config config.yaml --discover
    python tou_comparison.py --config config.yaml --entity sensor.grid_consumption --start-date 2024-01-01 --end-date 2024-12-31
"""

import argparse
import sys
from datetime import datetime

from config import AppConfig
from ha_influx_provider import HaInfluxProvider
from solar_interval_provider import SolarIntervalProvider
from tou_analyzer import (
    IntervalRecord,
    TouAnalyzer,
    TouRateSchedule,
    format_tou_terminal,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Xcel Energy SD TOU Billing Comparison",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    p.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Analysis period start date",
    )
    p.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Analysis period end date",
    )
    p.add_argument(
        "--entity",
        help="Override HA consumption entity_id (run --discover to find available entities)",
    )
    p.add_argument(
        "--field",
        help="Override HA field name (default: from config, usually 'value')",
    )
    p.add_argument(
        "--output",
        choices=["terminal", "json", "both"],
        default="terminal",
        help="Output format (default: terminal)",
    )
    p.add_argument(
        "--json-out",
        help="Write JSON output to this file path",
    )
    p.add_argument(
        "--discover",
        action="store_true",
        help="Discover available energy entities in HA InfluxDB, then exit",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Show additional detail in output",
    )
    p.add_argument(
        "--source",
        choices=["ha", "solar"],
        default="ha",
        help="Data source: ha (live HA InfluxDB) or solar (curated solar DB) (default: ha)",
    )
    return p


def run_discover(config: AppConfig) -> None:
    provider = HaInfluxProvider(config)
    entities = provider.discover_energy_entities()
    if not entities:
        print("No energy-related entities found in Home Assistant InfluxDB.")
        print("Check that ha_influxdb settings are correct in config.yaml.")
        return
    rows = []
    for e in entities:
        rows.append([
            e["entity_id"],
            e["field"],
            e["sample_count"],
            e["first"],
            e["last"],
        ])
    from tabulate import tabulate
    print(
        tabulate(
            rows,
            headers=["Entity ID", "Field", "Samples", "First Reading", "Last Reading"],
            tablefmt="grid",
        )
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        config = AppConfig.load(args.config)
    except FileNotFoundError:
        print(f"Config not found: {args.config}. Create one from config.yaml.example.", file=sys.stderr)
        sys.exit(1)

    if args.discover:
        run_discover(config)
        return

    entity = args.entity or config.ha_consumption_entity
    if not entity:
        print(
            "No consumption entity configured. Run with --discover to find available entities,\n"
            "then set ha_influxdb.consumption_entity in config.yaml or pass --entity.",
            file=sys.stderr,
        )
        sys.exit(1)

    field = args.field or config.ha_consumption_field or "value"

    end_date = datetime.now()
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    start_date = datetime(end_date.year, 1, 1)
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")

    # Select data provider based on --source flag
    if args.source == "solar":
        print(
            f"Fetching interval data for {entity} from solar DB ({start_date.date()} to {end_date.date()})...",
            file=sys.stderr,
        )
        provider = SolarIntervalProvider(config)
        if not args.start_date and not args.end_date:
            date_range = provider.get_date_range(entity)
            if date_range:
                start_date, end_date = date_range
                print(
                    f"Auto date range: {start_date.date()} to {end_date.date()} (all available data)",
                    file=sys.stderr,
                )
        intervals = provider.get_interval_data(
            entity_id=entity,
            field=field,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        print(
            f"Fetching interval data for {entity} from HA InfluxDB ({start_date.date()} to {end_date.date()})...",
            file=sys.stderr,
        )
        provider = HaInfluxProvider(config)
        intervals = provider.get_interval_data(
            entity_id=entity,
            field=field,
            start_date=start_date,
            end_date=end_date,
        )

    if not intervals:
        print("No interval data found. Check entity name and date range.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(intervals)} interval records.", file=sys.stderr)

    rate_schedule = TouRateSchedule(config.tou_config)
    analyzer = TouAnalyzer(rate_schedule)

    classified_intervals: list[IntervalRecord] = []
    for iv in intervals:
        period = rate_schedule.classify(iv.timestamp)
        classified_intervals.append(
            IntervalRecord(timestamp=iv.timestamp, kwh=iv.kwh, period=period)
        )

    result = analyzer.analyze(classified_intervals)

    if args.output in ("terminal", "both"):
        print()
        print(format_tou_terminal(result))

    json_str = result.to_json()
    if args.output in ("json", "both"):
        if args.output == "both":
            print()
            print("=" * 60)
            print("   JSON OUTPUT")
            print("=" * 60)
        print(json_str)

    if args.json_out:
        with open(args.json_out, "w") as f:
            f.write(json_str)
        print(f"JSON written to {args.json_out}", file=sys.stderr)


if __name__ == "__main__":
    main()