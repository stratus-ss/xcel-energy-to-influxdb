"""
Reads energy_interval data from the solar InfluxDB and returns IntervalRecord objects.
"""

import logging
from datetime import datetime

from influxdb import InfluxDBClient

from config import AppConfig
from tou_analyzer import IntervalRecord, TouPeriod

logger = logging.getLogger(__name__)


class SolarIntervalProvider:
    """Reads energy_interval data from solar DB and returns IntervalRecords."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._client = config.influx_client_solar()
        self._measurement = config.sync_target_measurement

    def get_interval_data(
        self,
        entity_id: str,
        field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[IntervalRecord]:
        """
        Fetch interval data from solar DB energy_interval measurement.

        Handles both 'ha' and 'backup' source tags, computing deltas from cumulative Wh.
        """
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        query = (
            f'SELECT time, "value", "source" FROM "{self._measurement}" '
            f'WHERE "entity_id" = \'{entity_id}\' '
            f'AND time >= \'{start_str}\' '
            f'AND time < \'{end_str}\' '
            f'ORDER BY time'
        )

        raw: list[dict] = []
        try:
            for point_list in self._client.query(query):
                for item in point_list:
                    if isinstance(item, dict) and "time" in item and "value" in item:
                        raw.append({
                            "time": item["time"],
                            "value": float(item["value"]),
                        })
        except Exception as e:
            logger.error("Failed to fetch from solar DB for %s: %s", entity_id, e)

        if len(raw) < 2:
            return []

        records: list[IntervalRecord] = []
        for i in range(1, len(raw)):
            prev = raw[i - 1]
            curr = raw[i]
            delta_wh = max(0.0, curr["value"] - prev["value"])
            delta_kwh = delta_wh / 1000.0
            if delta_kwh <= 0:
                continue
            ts = datetime.fromisoformat(curr["time"].replace("Z", "+00:00"))
            records.append(IntervalRecord(timestamp=ts, kwh=delta_kwh, period=TouPeriod.OFF_PEAK))

        logger.info(
            "Fetched %d intervals for %s from solar DB (%d raw points)",
            len(records), entity_id, len(raw),
        )
        return records

    def get_date_range(self, entity_id: str) -> tuple[datetime, datetime] | None:
        """Return (earliest, latest) timestamps for entity, or None if no data."""
        query = (
            f'SELECT FIRST("value"), LAST("value") FROM "{self._measurement}" '
            f'WHERE "entity_id" = \'{entity_id}\''
        )
        try:
            for point_list in self._client.query(query):
                for row in point_list:
                    first_ts = row.get("first", {})
                    last_ts = row.get("last", {})
                    if isinstance(first_ts, dict):
                        first_str = first_ts.get("time") or first_ts.get("value")
                        last_str = last_ts.get("time") or last_ts.get("value")
                    else:
                        first_str = str(first_ts) if first_ts else None
                        last_str = str(last_ts) if last_ts else None
                    if first_str and last_str:
                        first_dt = datetime.fromisoformat(first_str.replace("Z", "+00:00"))
                        last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
                        return (first_dt, last_dt)
        except Exception as e:
            logger.debug("Date range query returned no data: %s", e)
        return None
