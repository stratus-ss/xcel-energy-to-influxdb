"""
Syncs energy interval data from HA InfluxDB to the solar InfluxDB.

Supports live sync (HA -> solar DB) and backup extraction (via temp Docker
container on the remote host -> solar DB).
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from config import AppConfig
from ha_interval_fetcher import HaIntervalFetcher
from influx_batch_writer import InfluxBatchWriter

logger = logging.getLogger(__name__)


class EnergySyncService:
    """Syncs energy interval data from HA InfluxDB to solar DB."""

    def __init__(self, config: AppConfig) -> None:
        self._fetcher = HaIntervalFetcher(config)
        self._writer = InfluxBatchWriter(config)

    def sync_entity(self, entity_id: str, backfill_from: datetime | None = None) -> int:
        """
        Sync one entity: check high-water mark, fetch new points, write to solar DB.

        Args:
            entity_id: the entity to sync
            backfill_from: if set, ignore high-water mark and fetch from this date

        Returns:
            Number of points written.
        """
        since = backfill_from
        if since is None:
            hwm = self._writer.get_high_water_mark(entity_id)
            if hwm is not None:
                since = hwm
                logger.info("Resuming %s from high-water mark %s", entity_id, hwm)
            else:
                logger.info("No prior data for %s, fetching all available", entity_id)

        if since is not None:
            logger.info("Fetching HA data for %s since %s", entity_id, since)

        points = self._fetcher.fetch(entity_id, since=since)
        if not points:
            logger.info("No new points for %s", entity_id)
            return 0

        count = self._writer.write_points(points, entity_id, source="ha")
        logger.info("Synced %d points for %s", count, entity_id)
        return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync energy interval data from HA InfluxDB to solar DB."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "--entity",
        action="append",
        dest="entities",
        metavar="ENTITY_ID",
        help="Entity ID to sync (can be repeated). Defaults to sync.entities in config.",
    )
    parser.add_argument(
        "--backfill-from",
        metavar="ISO_DATE",
        help="Ignore high-water mark and fetch all data since this date (e.g. 2025-01-01T00:00:00Z).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch points but do not write to solar DB.",
    )
    return parser


def _parse_backfill_date(date_str: str) -> datetime:
    """Parse an ISO8601 date string into a datetime."""
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = AppConfig.load(args.config)
    service = EnergySyncService(config)

    entities = args.entities if args.entities else config.sync_entities
    if not entities:
        print("No entities to sync. Set sync.entities in config.yaml or use --entity.")
        sys.exit(1)

    backfill_from = None
    if args.backfill_from:
        backfill_from = _parse_backfill_date(args.backfill_from)

    total_points = 0
    for entity_id in entities:
        if args.dry_run:
            points = service._fetcher.fetch(entity_id, since=backfill_from)
            print(f"[dry-run] {entity_id}: {len(points)} points would be synced")
        else:
            count = service.sync_entity(entity_id, backfill_from=backfill_from)
            print(f"{entity_id}: {count} points written")
            total_points += count

    if not args.dry_run:
        print(f"\nTotal: {total_points} points written")


if __name__ == "__main__":
    main()
