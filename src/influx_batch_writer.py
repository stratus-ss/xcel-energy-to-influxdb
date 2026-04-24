"""
Writes energy interval data points to the solar InfluxDB in batches.
"""

import logging
from datetime import datetime

from config import AppConfig

logger = logging.getLogger(__name__)


class InfluxBatchWriter:
    """Writes energy interval data points to solar DB in batches."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._client = config.influx_client_solar()
        self._measurement = config.sync_target_measurement
        self._batch_size = config.sync_batch_size

    def write_points(
        self,
        points: list[dict],
        entity_id: str,
        source: str,
    ) -> int:
        """
        Write points to energy_interval measurement.

        Args:
            points: list of {"time": "ISO8601Z", "value": float}
            entity_id: sensor entity_id tag value
            source: "ha" or "backup"

        Returns:
            Number of points written.
        """
        if not points:
            return 0

        total_written = 0
        for i in range(0, len(points), self._batch_size):
            chunk = points[i:i + self._batch_size]
            lines = [
                self._to_line_protocol(p, entity_id, source) for p in chunk
            ]
            body = "\n".join(lines)
            try:
                self._client.write_points(body, protocol="line")
                total_written += len(chunk)
            except Exception as e:
                logger.error("Write failed for chunk starting at %d: %s", i, e)
                raise

        logger.info("Wrote %d points for %s (source=%s)", total_written, entity_id, source)
        return total_written

    def _to_line_protocol(
        self,
        point: dict,
        entity_id: str,
        source: str,
    ) -> str:
        """Convert a single point dict to InfluxDB line protocol string."""
        ts_str = point["time"]
        # Parse ISO8601 timestamp and convert to nanoseconds since epoch
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        ns = int(ts.timestamp() * 1e9)
        value = float(point["value"])
        # Escape entity_id in case it contains special characters
        escaped_id = entity_id.replace(",", "\\,").replace(" ", "\\ ")
        return (
            f"{self._measurement},entity_id={escaped_id},source={source} "
            f"value={value} {ns}"
        )

    def get_high_water_mark(self, entity_id: str) -> datetime | None:
        """
        Return the latest timestamp for entity_id in energy_interval, or None if no data.
        """
        escaped_id = entity_id.replace(",", "\\,").replace(" ", "\\ ")
        query = (
            f'SELECT LAST("value") FROM "{self._measurement}" '
            f'WHERE "entity_id" = \'{escaped_id}\''
        )
        try:
            for point_list in self._client.query(query):
                for row in point_list:
                    ts_str = row.get("time")
                    if ts_str:
                        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception as e:
            logger.debug("High-water mark query returned no data: %s", e)
        return None
