"""
Parses InfluxDB line protocol text into structured data.

Used by extract_backup.py to process output from influx_inspect export.
"""

from datetime import datetime


class LineProtocolParser:
    """Parses InfluxDB line protocol output into structured dicts by entity_id."""

    def parse_line(self, line: str) -> dict | None:
        """
        Parse a single line protocol line.

        Format: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp

        Returns {"entity_id": str, "time": str, "value": float} or None if unparseable.
        """
        if not line or "," not in line:
            return None

        try:
            # Split into metric+tags, fields, timestamp
            space_idx = line.rfind(" ")
            if space_idx == -1:
                return None

            metric_tags = line[:space_idx]
            fields_part = line[space_idx + 1:]

            # Fields: value=X or value=Xi
            if "=" not in fields_part:
                return None
            field_kv = fields_part.split("=", 1)
            field_val_str = field_kv[1].rstrip("i")
            value = float(field_val_str)

            # Parse timestamp (nanoseconds since epoch -> ISO8601Z)
            timestamp_ns = int(fields_part.split(" ")[-1])
            ts_sec = timestamp_ns / 1e9
            dt = datetime.fromtimestamp(ts_sec, tz=datetime.timezone.utc)
            time_str = dt.isoformat().replace("+00:00", "Z")

            # Extract entity_id from metric_tags
            entity_id = None
            for tag in metric_tags.split(","):
                if tag.startswith("entity_id="):
                    entity_id = tag.split("=", 1)[1]
                    break

            if entity_id is None:
                return None

            return {"entity_id": entity_id, "time": time_str, "value": value}
        except Exception:
            return None

    def filter_and_parse(
        self, raw_output: str, entities: set[str]
    ) -> dict[str, list[dict]]:
        """
        Parse raw line protocol text, return points grouped by entity_id.

        Args:
            raw_output: newline-separated line protocol lines
            entities: set of entity_id strings to include

        Returns:
            {entity_id: [{"time": "ISO8601Z", "value": float}, ...]}
        """
        result: dict[str, list[dict]] = {e: [] for e in entities}

        for line in raw_output.split("\n"):
            if not line.strip():
                continue
            parsed = self.parse_line(line)
            if parsed and parsed["entity_id"] in entities:
                result[parsed["entity_id"]].append({
                    "time": parsed["time"],
                    "value": parsed["value"],
                })

        return result