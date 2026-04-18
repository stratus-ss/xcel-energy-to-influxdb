"""
Pluggable solar data providers for the payback calculator.

SolarDataProvider ABC and concrete implementations:
  - InfluxSolarProvider   -- reads pre-aggregated monthly data from InfluxDB
  - EnphaseSolarProvider  -- uses EnphaseEnergyMonitor as fallback

SolarDataResolver tries providers in order and returns the first available data.
"""

import logging
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import AppConfig

logger = logging.getLogger(__name__)


@dataclass
class MonthlyProduction:
    """Normalized monthly solar production data. All values in kWh."""
    month: str                       # "YYYY-MM"
    production_kwh: float            # total energy produced by panels
    export_kwh: float                # energy exported back to grid
    import_kwh: float                # energy imported from grid
    self_consumed_kwh: float         # production - export (energy used onsite)
    consumed_kwh: float = 0.0        # total household consumption
    battery_charged_kwh: float = 0.0
    battery_discharged_kwh: float = 0.0

    @property
    def net_import_kwh(self) -> float:
        """Grid import minus export (positive = net consumer)."""
        return max(0, self.import_kwh - self.export_kwh)


class SolarDataProvider(ABC):
    """Abstract base for any solar data source."""

    @abstractmethod
    def is_available(self) -> bool:
        """Quick check if this provider has data and credentials are configured."""

    @abstractmethod
    def get_monthly_data(self, start_date: datetime, end_date: datetime) -> list[MonthlyProduction]:
        """
        Fetch monthly solar production data in the given date range.
        Returns list sorted by month ascending.
        """

    @abstractmethod
    def source_name(self) -> str:
        """Human-readable name of this provider (for logs/output)."""


class InfluxSolarProvider(SolarDataProvider):
    """
    Reads pre-aggregated monthly solar data from InfluxDB.

    Schema: solar_monthly,metric=<name> value=<Wh> <timestamp>
    Metrics: produced, consumed, exported, imported, battery_charged, battery_discharged
    Each point is one month's total in Wh, timestamped at month-end.
    """

    _METRIC_MAP = {
        "produced": "production_wh",
        "consumed": "consumption_wh",
        "exported": "export_wh",
        "imported": "import_wh",
        "battery_charged": "battery_charged_wh",
        "battery_discharged": "battery_discharged_wh",
    }

    def __init__(self, config: AppConfig):
        self.config = config

    def source_name(self) -> str:
        return f"InfluxDB/{self.config.readonly_database}/{self.config.readonly_measurement}"

    def is_available(self) -> bool:
        if not self.config.readonly_password:
            return False
        try:
            client = self.config.influx_client_solar()
            client.query("SHOW DATABASES")
            return True
        except Exception as e:
            logger.debug("InfluxSolarProvider not available: %s", e)
            return False

    def get_monthly_data(self, start_date: datetime, end_date: datetime) -> list[MonthlyProduction]:
        client = self.config.influx_client_solar()
        measurement = self.config.solar_measurement

        monthly: dict[str, dict[str, float]] = {}
        for influx_metric, key in self._METRIC_MAP.items():
            rows = self._query_metric(client, measurement, influx_metric, start_date, end_date)
            for month_str, val_wh in rows.items():
                monthly.setdefault(month_str, {k: 0.0 for k in self._METRIC_MAP.values()})
                monthly[month_str][key] = val_wh

        productions = []
        for month_str in sorted(monthly):
            d = monthly[month_str]
            prod = d["production_wh"] / 1000.0
            exp = d["export_wh"] / 1000.0
            imp = d["import_wh"] / 1000.0
            self_consumed = max(0.0, prod - exp)
            bat_in = d["battery_charged_wh"] / 1000.0
            bat_out = d["battery_discharged_wh"] / 1000.0

            consumed = d["consumption_wh"] / 1000.0

            productions.append(MonthlyProduction(
                month=month_str,
                production_kwh=round(prod, 3),
                export_kwh=round(exp, 3),
                import_kwh=round(imp, 3),
                self_consumed_kwh=round(self_consumed, 3),
                consumed_kwh=round(consumed, 3),
                battery_charged_kwh=round(bat_in, 3),
                battery_discharged_kwh=round(bat_out, 3),
            ))

        return productions

    def _query_metric(
        self, client, measurement: str, metric: str,
        start_date: datetime, end_date: datetime,
    ) -> dict[str, float]:
        """Query one metric tag and return {month_str: value_wh}."""
        query = (
            f'SELECT value FROM "{measurement}" '
            f"WHERE metric = '{metric}' "
            f"AND time >= '{start_date.strftime('%Y-%m-%d')}' "
            f"AND time < '{end_date.strftime('%Y-%m-%d')}' "
            f"ORDER BY time"
        )
        results: dict[str, float] = {}
        try:
            for point_list in client.query(query):
                for item in point_list:
                    if not isinstance(item, dict):
                        continue
                    ts = item.get("time")
                    val = item.get("value")
                    if ts is None or val is None:
                        continue
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    results[dt.strftime("%Y-%m")] = float(val)
        except Exception as e:
            logger.debug("Query failed for metric=%s: %s", metric, e)
        return results


class EnphaseSolarProvider(SolarDataProvider):
    """Reads solar production data from Enphase Cloud API as fallback."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._monitor = None

    def source_name(self) -> str:
        return "Enphase API"

    def is_available(self) -> bool:
        if not self.config.enphase_client_id or not self.config.enphase_client_secret:
            return False
        return True

    def get_monthly_data(self, start_date: datetime, end_date: datetime) -> list[MonthlyProduction]:
        from enphase import EnphaseEnergyMonitor

        enphase_cfg = self.config.enphase_config
        try:
            self._monitor = EnphaseEnergyMonitor(
                use_cloud=enphase_cfg["use_cloud"],
                client_id=enphase_cfg["client_id"],
                client_secret=enphase_cfg["client_secret"],
                system_id=enphase_cfg["system_id"] or None,
                envoy_host=enphase_cfg["envoy_host"],
                bearer_token=enphase_cfg["bearer_token"] or None,
            )
        except Exception as e:
            logger.warning("Failed to initialize Enphase monitor: %s", e)
            return []

        productions = []
        current = datetime(start_date.year, start_date.month, 1)
        while current <= end_date:
            try:
                aggregates = self._monitor.get_monthly_aggregates(
                    month=current.month, year=current.year
                )
                if aggregates:
                    production = aggregates.get("grid_export", 0) + aggregates.get("grid_import", 0)
                    export_kwh = aggregates.get("grid_export", 0)
                    import_kwh = aggregates.get("grid_import", 0)
                    bat_in = aggregates.get("storage_charged", 0)
                    bat_out = aggregates.get("storage_discharged", 0)

                    productions.append(MonthlyProduction(
                        month=current.strftime("%Y-%m"),
                        production_kwh=production,
                        export_kwh=export_kwh,
                        import_kwh=import_kwh,
                        self_consumed_kwh=max(0, production - export_kwh),
                        consumed_kwh=production + import_kwh - export_kwh,
                        battery_charged_kwh=bat_in,
                        battery_discharged_kwh=bat_out,
                    ))
            except Exception as e:
                logger.warning("Failed to get Enphase aggregates for %s: %s", current.strftime("%Y-%m"), e)

            month = current.month + 1
            year = current.year + (month - 1) // 12
            month = (month - 1) % 12 + 1
            current = datetime(year, month, 1)

        productions.sort(key=lambda p: p.month)
        return productions


class SolarDataResolver:
    """
    Tries solar data providers in order and returns the first result.

    Provider order controlled by --solar-source / solar.source config:
      auto        -- try InfluxDB, then Enphase
      influx      -- InfluxDB only
      enphase     -- Enphase API only
      bills_only  -- skip solar data (credit-only mode, no production lookup)
      none        -- skip solar data entirely
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self._influx_provider = InfluxSolarProvider(config)
        self._enphase_provider = EnphaseSolarProvider(config)

    def resolve(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[list[MonthlyProduction], Optional[str]]:
        source = self.config.solar_source.lower()

        if source in ("none", "bills_only"):
            return [], None

        if source == "auto":
            providers = [
                ("InfluxDB", self._influx_provider),
                ("Enphase API", self._enphase_provider),
            ]
        elif source == "influx":
            providers = [("InfluxDB", self._influx_provider)]
        elif source == "enphase":
            providers = [("Enphase API", self._enphase_provider)]
        else:
            providers = [("InfluxDB", self._influx_provider), ("Enphase API", self._enphase_provider)]

        for name, provider in providers:
            if not provider.is_available():
                logger.info("Provider '%s' not available, trying next...", name)
                continue
            try:
                data = provider.get_monthly_data(start_date, end_date)
                if data:
                    logger.info("Solar data obtained from %s (%d months)", name, len(data))
                    return data, name
            except Exception as e:
                logger.warning("Provider '%s' failed: %s", name, e)
                continue

        return [], None
