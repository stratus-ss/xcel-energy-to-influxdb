"""
Home Assistant InfluxDB interval data provider for TOU billing analysis.

Reads sensor interval data from the Home Assistant InfluxDB database and
converts it to classified IntervalRecord objects.
"""

import logging
from datetime import datetime
from typing import Optional

from config import AppConfig
from tou_analyzer import IntervalRecord, TouPeriod

logger = logging.getLogger(__name__)

_ENERGY_KEYWORDS = {"energy", "power", "consumption", "kwh", "wh", "import", "export"}


class HaInfluxProvider:
    """
    Reads interval energy consumption data from the Home Assistant InfluxDB.

    Supports two common HA InfluxDB schemas:
      A) measurement = entity_id (e.g. "sensor.grid_consumption"), field = "value"
      B) measurement = "state", tag "entity_id" = sensor name, field = "value"
    """

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def discover_energy_entities(self) -> list[dict]:
        """
        Query HA InfluxDB and return energy-related measurements with stats.

        Returns list of dicts: {
            "measurement": str,
            "entity_id": str,
            "field": str,
            "sample_count": int,
            "first": str,
            "last": str
        }
        """
        client = self._config.influx_client_ha()
        results: list[dict] = []

        try:
            measurements = client.query("SHOW MEASUREMENTS")
            for row_list in measurements:
                for row in row_list:
                    meas = row.get("name") or row.get("measurement")
                    if not meas:
                        continue
                    meas_lower = meas.lower()
                    if not any(k in meas_lower for k in _ENERGY_KEYWORDS):
                        continue
                    field_keys = self._get_field_keys(client, meas)
                    for field in field_keys:
                        stats = self._get_sample_stats(client, meas, field)
                        if stats:
                            results.append({
                                "measurement": meas,
                                "entity_id": meas,
                                "field": field,
                                "sample_count": stats["count"],
                                "first": stats["first"],
                                "last": stats["last"],
                            })
        except Exception as e:
            logger.error("Failed to discover HA InfluxDB entities: %s", e)

        try:
            schema_b_fields = client.query("SHOW FIELD KEYS FROM state")
            for row_list in schema_b_fields:
                for row in row_list:
                    field = row.get("fieldKey") or row.get("field")
                    if not field:
                        continue
                    entity_tags = self._get_entity_ids_for_field(client, field)
                    for entity_id in entity_tags:
                        if not any(k in entity_id.lower() for k in _ENERGY_KEYWORDS):
                            continue
                        stats = self._get_sample_stats(client, "state", field, entity_id)
                        if stats:
                            results.append({
                                "measurement": "state",
                                "entity_id": entity_id,
                                "field": field,
                                "sample_count": stats["count"],
                                "first": stats["first"],
                                "last": stats["last"],
                            })
        except Exception as e:
            logger.debug("Schema B (state measurement) not applicable: %s", e)

        client.close()
        return sorted(results, key=lambda x: x.get("sample_count", 0), reverse=True)

    def get_interval_data(
        self,
        entity_id: str,
        field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[IntervalRecord]:
        """
        Fetch interval energy data for a specific entity.

        Handles two HA InfluxDB schemas:
          A) measurement = entity_id, field = "value"
          B) measurement = unit bucket (e.g. "Wh", "kWh"), tag entity_id = sensor name

        Detects cumulative vs instantaneous data and applies unit conversion.
        """
        client = self._config.influx_client_ha()
        measurement = entity_id
        unit_measurement = None

        raw = self._try_query(client, measurement, field, start_date, end_date, "")

        if not raw:
            for unit in ("Wh", "kWh", "W"):
                entity_filter = f" AND entity_id = '{entity_id}'"
                raw = self._try_query(client, unit, field, start_date, end_date, entity_filter)
                if raw:
                    unit_measurement = unit
                    break

        client.close()

        if len(raw) < 2:
            logger.warning("Not enough data points for %s", entity_id)
            return []

        raw_sorted = sorted(raw, key=lambda x: x["time"])
        is_cumulative = self._detect_cumulative(raw_sorted)

        records = self._build_interval_records(
            raw_sorted, is_cumulative, unit_measurement
        )

        logger.info(
            "Fetched %d intervals for %s (cumulative=%s, unit=%s)",
            len(records), entity_id, is_cumulative, unit_measurement or measurement,
        )
        return records

    def _build_interval_records(
        self,
        raw_sorted: list[dict],
        is_cumulative: bool,
        unit_measurement: str | None,
    ) -> list[IntervalRecord]:
        """Convert raw InfluxDB points to IntervalRecord list with correct kWh units."""
        records: list[IntervalRecord] = []
        for i in range(1, len(raw_sorted)):
            prev = raw_sorted[i - 1]
            curr = raw_sorted[i]
            delta_kwh = self._compute_delta(prev, curr, is_cumulative, unit_measurement)
            if delta_kwh <= 0:
                continue
            ts = datetime.fromisoformat(curr["time"].replace("Z", "+00:00"))
            records.append(IntervalRecord(timestamp=ts, kwh=delta_kwh, period=TouPeriod.OFF_PEAK))
        return records

    def _try_query(
        self,
        client,
        measurement: str,
        field: str,
        start: datetime,
        end: datetime,
        extra_filter: str,
    ) -> list[dict]:
        """Execute a query and return results, or [] on failure."""
        start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        query = (
            f'SELECT time, "{field}" '
            f'FROM "{measurement}" '
            f"WHERE time >= '{start_str}' "
            f"AND time < '{end_str}' "
            f"{extra_filter} "
            f"ORDER BY time"
        )
        try:
            results: list[dict] = []
            for point_list in client.query(query):
                for item in point_list:
                    if isinstance(item, dict) and "time" in item and field in item:
                        results.append(item)
            return results
        except Exception as e:
            logger.debug("Query failed for %s.%s: %s", measurement, field, e)
            return []

    def _get_field_keys(self, client, measurement: str) -> list[str]:
        keys: list[str] = []
        try:
            result = client.query(f"SHOW FIELD KEYS FROM \"{measurement}\"")
            for pl in result:
                for row in pl:
                    k = row.get("fieldKey") or row.get("field")
                    if k:
                        keys.append(k)
        except Exception:
            pass
        return keys

    def _get_sample_stats(
        self,
        client,
        measurement: str,
        field: str,
        entity_filter: str = "",
    ) -> Optional[dict]:
        try:
            q = (
                f'SELECT COUNT("{field}"), FIRST("{field}"), LAST("{field}") '
                f'FROM "{measurement}" {entity_filter}'
            )
            for pl in client.query(q):
                for row in pl:
                    first_val = row.get("first", {})
                    last_val = row.get("last", {})
                    if isinstance(first_val, dict):
                        first = first_val.get(field, "N/A")
                        last = last_val.get(field, "N/A")
                    else:
                        first = str(first_val)
                        last = str(last_val)
                    return {
                        "count": int(row.get("count", 0)),
                        "first": first,
                        "last": last,
                    }
        except Exception:
            pass
        return None

    def _get_entity_ids_for_field(self, client, field: str) -> list[str]:
        ids: list[str] = []
        try:
            q = f'SHOW TAG VALUES FROM "state" WITH KEY = entity_id WHERE "{field}" IS NOT NULL'
            for pl in client.query(q):
                for row in pl:
                    v = row.get("value") or row.get("entity_id")
                    if v:
                        ids.append(v)
        except Exception:
            pass
        return ids

    def _detect_cumulative(self, points: list[dict]) -> bool:
        if len(points) < 2:
            return False
        values = [float(p.get("value", 0)) for p in points]
        increases = sum(1 for i in range(1, len(values)) if values[i] >= values[i - 1])
        return increases / (len(values) - 1) > 0.8

    def _compute_delta(
        self,
        prev: dict,
        curr: dict,
        is_cumulative: bool,
        unit_measurement: str | None = None,
    ) -> float:
        """Compute interval energy in kWh between two consecutive points."""
        prev_val = float(prev.get("value", 0))
        curr_val = float(curr.get("value", 0))
        prev_ts = datetime.fromisoformat(prev["time"].replace("Z", "+00:00"))
        curr_ts = datetime.fromisoformat(curr["time"].replace("Z", "+00:00"))
        delta_hours = (curr_ts - prev_ts).total_seconds() / 3600.0

        if is_cumulative:
            delta = max(0.0, curr_val - prev_val)
            if unit_measurement == "Wh":
                return delta / 1000.0
            return delta

        avg_power = (prev_val + curr_val) / 2.0
        if unit_measurement in ("W", "Wh") or unit_measurement is None:
            return avg_power * delta_hours / 1000.0
        return avg_power * delta_hours
