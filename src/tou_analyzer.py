"""
TOU billing analysis for Xcel Energy South Dakota.

Classifies interval energy consumption data into on-peak / off-peak periods
using the Residential Time-of-Day rate schedule, computes costs under both
TOU and standard flat-rate plans, and produces a comparison report.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

from tabulate import tabulate

logger = logging.getLogger(__name__)
_LOCAL_TZ = ZoneInfo("America/Chicago")


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class TouPeriod(Enum):
    SUMMER_ON_PEAK = "summer_on_peak"
    WINTER_ON_PEAK = "winter_on_peak"
    OFF_PEAK = "off_peak"


@dataclass
class IntervalRecord:
    timestamp: datetime
    kwh: float
    period: TouPeriod


@dataclass
class MonthlyTouBreakdown:
    month: str
    on_peak_kwh: float
    off_peak_kwh: float
    total_kwh: float
    on_peak_pct: float
    tou_cost: float
    flat_cost: float
    tou_customer_charge: float
    flat_customer_charge: float
    savings: float


@dataclass
class TouComparisonResult:
    analysis_period_start: str
    analysis_period_end: str
    total_on_peak_kwh: float
    total_off_peak_kwh: float
    total_kwh: float
    overall_on_peak_pct: float
    total_tou_cost: float
    total_flat_cost: float
    total_savings: float
    annual_projected_savings: float
    monthly_breakdowns: list

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


# ---------------------------------------------------------------------------
# Rate Schedule
# ---------------------------------------------------------------------------

class TouRateSchedule:
    """
    Xcel Energy South Dakota TOU rate classifier and cost calculator.

    Config dict format (from AppConfig.tou_config):
        peak_start_hour, peak_end_hour, peak_days, holidays,
        summer_months, summer_on_peak, winter_on_peak, off_peak,
        flat_summer, flat_winter_first_1000, flat_winter_excess,
        fuel_surcharge, tou_customer_charge, flat_customer_charge
    """

    def __init__(self, config: dict) -> None:
        self._c = config
        self._peak_start = config.get("peak_start_hour", 9)
        self._peak_end = config.get("peak_end_hour", 21)
        self._peak_days = set(config.get("peak_days", [0, 1, 2, 3, 4]))
        self._holidays = set(config.get("holidays", []))
        self._summer_months = set(config.get("summer_months", [6, 7, 8, 9]))
        self._summer_on_peak = float(config.get("summer_on_peak", 0.21806))
        self._winter_on_peak = float(config.get("winter_on_peak", 0.17590))
        self._off_peak = float(config.get("off_peak", 0.04610))
        self._flat_summer = float(config.get("flat_summer", 0.11153))
        self._flat_winter_first = float(config.get("flat_winter_first_1000", 0.09585))
        self._flat_winter_excess = float(config.get("flat_winter_excess", 0.09327))
        self._fuel_surcharge = float(config.get("fuel_surcharge", 0.02634))
        self._tou_charge = float(config.get("tou_customer_charge", 10.30))
        self._flat_charge = float(config.get("flat_customer_charge", 8.30))

    def is_holiday(self, dt: datetime) -> bool:
        key = f"{dt.month:02d}-{dt.day:02d}"
        return key in self._holidays

    def classify(self, timestamp: datetime) -> TouPeriod:
        local = timestamp.astimezone(_LOCAL_TZ)
        weekday = local.weekday()
        hour = local.hour
        is_summer = local.month in self._summer_months

        if (
            weekday in self._peak_days
            and self._peak_start <= hour < self._peak_end
            and not self.is_holiday(local)
        ):
            return TouPeriod.SUMMER_ON_PEAK if is_summer else TouPeriod.WINTER_ON_PEAK
        return TouPeriod.OFF_PEAK

    def tou_rate(self, period: TouPeriod) -> float:
        if period == TouPeriod.SUMMER_ON_PEAK:
            return self._summer_on_peak + self._fuel_surcharge
        if period == TouPeriod.WINTER_ON_PEAK:
            return self._winter_on_peak + self._fuel_surcharge
        return self._off_peak + self._fuel_surcharge

    def flat_rate(self, month: int, cumulative_kwh: float) -> float:
        base = self._flat_summer if month in self._summer_months else (
            self._flat_winter_first if cumulative_kwh <= 1000 else self._flat_winter_excess
        )
        return base + self._fuel_surcharge


# ---------------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------------

class TouAnalyzer:
    """Computes TOU vs flat-rate cost comparison from classified interval data."""

    def __init__(self, rate_schedule: TouRateSchedule, bill_records: list = None) -> None:
        self._rate = rate_schedule
        self._bill_records = bill_records or []

    def analyze(self, intervals: list[IntervalRecord]) -> TouComparisonResult:
        if not intervals:
            raise ValueError("No interval data provided")

        by_month: dict[str, list[IntervalRecord]] = {}
        for iv in intervals:
            key = iv.timestamp.strftime("%Y-%m")
            by_month.setdefault(key, []).append(iv)

        breakdowns = []
        for month in sorted(by_month.keys()):
            breakdowns.append(self._build_monthly_breakdown(month, by_month[month]))

        total_kwh = sum(b.total_kwh for b in breakdowns)
        total_on = sum(b.on_peak_kwh for b in breakdowns)
        total_off = sum(b.off_peak_kwh for b in breakdowns)
        total_flat = sum(b.flat_cost for b in breakdowns)
        total_tou = sum(b.tou_cost for b in breakdowns)
        total_savings = sum(b.savings for b in breakdowns)
        months_count = len(breakdowns)

        annual_proj = (
            total_savings * (12 / months_count) if months_count > 0 else 0.0
        )

        start_dt = min(iv.timestamp for iv in intervals)
        end_dt = max(iv.timestamp for iv in intervals)

        return TouComparisonResult(
            analysis_period_start=start_dt.strftime("%Y-%m-%d"),
            analysis_period_end=end_dt.strftime("%Y-%m-%d"),
            total_on_peak_kwh=round(total_on, 1),
            total_off_peak_kwh=round(total_off, 1),
            total_kwh=round(total_kwh, 1),
            overall_on_peak_pct=round(total_on / total_kwh * 100, 1) if total_kwh else 0,
            total_tou_cost=round(total_tou, 2),
            total_flat_cost=round(total_flat, 2),
            total_savings=round(total_savings, 2),
            annual_projected_savings=round(annual_proj, 2),
            monthly_breakdowns=[asdict(b) for b in breakdowns],
        )

    def _build_monthly_breakdown(
        self, month: str, intervals: list[IntervalRecord]
    ) -> MonthlyTouBreakdown:
        on_kwh = sum(iv.kwh for iv in intervals if iv.period != TouPeriod.OFF_PEAK)
        off_kwh = sum(iv.kwh for iv in intervals if iv.period == TouPeriod.OFF_PEAK)
        total = on_kwh + off_kwh

        month_dt = datetime.strptime(month, "%Y-%m")
        m = month_dt.month

        tou_cost = sum(iv.kwh * self._rate.tou_rate(iv.period) for iv in intervals)
        tou_cost += self._rate._tou_charge

        flat_cost = 0.0
        cumulative = 0.0
        for iv in sorted(intervals, key=lambda x: x.timestamp):
            kwh = iv.kwh
            if cumulative < 1000 and cumulative + kwh > 1000:
                split1 = 1000 - cumulative
                split2 = kwh - split1
                flat_cost += split1 * self._rate.flat_rate(m, cumulative)
                cumulative += split1
                flat_cost += split2 * self._rate.flat_rate(m, cumulative)
                cumulative += split2
            else:
                flat_cost += kwh * self._rate.flat_rate(m, cumulative)
                cumulative += kwh
        flat_cost += self._rate._flat_charge

        return MonthlyTouBreakdown(
            month=month,
            on_peak_kwh=round(on_kwh, 1),
            off_peak_kwh=round(off_kwh, 1),
            total_kwh=round(total, 1),
            on_peak_pct=round(on_kwh / total * 100, 1) if total else 0,
            tou_cost=round(tou_cost, 2),
            flat_cost=round(flat_cost, 2),
            tou_customer_charge=round(self._rate._tou_charge, 2),
            flat_customer_charge=round(self._rate._flat_charge, 2),
            savings=round(flat_cost - tou_cost, 2),
        )


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def format_tou_summary(result: TouComparisonResult) -> str:
    rows = [
        ("Analysis Period", f"{result.analysis_period_start} to {result.analysis_period_end}"),
        ("Total Energy (kWh)", f"{result.total_kwh:,.1f}"),
        ("On-Peak kWh", f"{result.total_on_peak_kwh:,.1f} ({result.overall_on_peak_pct}%)"),
        ("Off-Peak kWh", f"{result.total_off_peak_kwh:,.1f} ({100 - result.overall_on_peak_pct}%)"),
        ("Flat-Rate Cost", f"${result.total_flat_cost:,.2f}"),
        ("TOU Cost", f"${result.total_tou_cost:,.2f}"),
        ("Savings", f"${result.total_savings:,.2f}"),
        ("Annual Projected", f"${result.annual_projected_savings:,.2f}"),
    ]
    return tabulate(rows, headers=["Metric", "Value"], tablefmt="grid")


def format_monthly_table(result: TouComparisonResult) -> str:
    rows = []
    for b in result.monthly_breakdowns:
        rows.append([
            b["month"],
            f"{b['on_peak_kwh']:,.1f}",
            f"{b['off_peak_kwh']:,.1f}",
            f"{b['on_peak_pct']:.1f}%",
            f"${b['flat_cost']:.2f}",
            f"${b['tou_cost']:.2f}",
            f"${b['savings']:.2f}",
        ])
    return tabulate(
        rows,
        headers=["Month", "On-Peak kWh", "Off-Peak kWh", "On%",
                 "Flat Cost", "TOU Cost", "Savings"],
        tablefmt="grid",
    )


def format_tou_terminal(result: TouComparisonResult) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("   TOU BILLING COMPARISON")
    lines.append("=" * 60)
    lines.append("")
    lines.append(format_tou_summary(result))
    lines.append("")
    lines.append("-" * 60)
    lines.append("   MONTHLY BREAKDOWN")
    lines.append("-" * 60)
    lines.append(format_monthly_table(result))
    return "\n".join(lines)