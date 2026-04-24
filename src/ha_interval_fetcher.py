"""
Fetches raw cumulative Wh interval data from the Home Assistant InfluxDB.
"""

import logging
from datetime import datetime, timezone

from influxdb import InfluxDBClient

from config import AppConfig

logger = logging.getLogger(__name__)


class HaIntervalFetcher:
    """Fetches raw cumulative Wh points from HA InfluxDB."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._client = config.influx_client_ha()

    def fetch(
        self,
        entity_id: str,
        since: datetime | None,
        until: datetime | None = None,
    ) -> list[dict]:
        """
        Fetch raw points from HA InfluxDB Wh measurement.

        Args:
            entity_id: entity_id tag value to filter
            since: start time (exclusive), or None for all data
            until: end time (inclusive), or None for now

        Returns:
            List of {"time": "ISO8601Z", "value": float} sorted by time.
        """
        where_parts = [f'"entity_id" = \'{entity_id}\'']
        if since is not None:
            since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            where_parts.append(f'time > \'{since_str}\'')
        if until is not None:
            until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ")
            where_parts.append(f'time < \'{until_str}\'')

        where_clause = " AND ".join(where_parts)
        query = (
            f'SELECT time, "value" FROM "Wh" '
            f'WHERE {where_clause} '
            f'ORDER BY time'
        )

        results: list[dict] = []
        try:
            for point_list in self._client.query(query):
                for item in point_list:
                    if isinstance(item, dict) and "time" in item and "value" in item:
                        results.append({"time": item["time"], "value": float(item["value"])})
        except Exception as e:
            logger.error("Failed to fetch from HA InfluxDB for %s: %s", entity_id, e)

        return results
