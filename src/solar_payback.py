"""
Solar Panel Payback Calculator.

Uses historical Xcel Energy bill data and solar production data to project
solar panel investment payback timelines. Supports:
  - Flat upfront cost with federal tax credit (ITC)
  - Rate escalation and panel degradation
  - Pre-bill gap estimation using seasonal averages
  - JSON and terminal table output (via tabulate)

Usage:
  python solar_payback.py --system-cost 18000 [--install-date 2022-06-01] [options]
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from statistics import median
from typing import Optional

from tabulate import tabulate

from config import AppConfig
from bill_parser import BillParser
from solar_data import SolarDataResolver, MonthlyProduction

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class BillRecord:
    """A parsed bill with computed fields."""
    statement_date: date
    total_energy_kwh: float       # grid consumption
    total_delivered_kwh: float    # exported to grid
    energy_payment_credit: float  # export credit ($)
    subtotal: float               # actual bill amount ($)
    effective_rate: float = 0.0   # subtotal / total_energy_kwh

    @property
    def month_key(self) -> str:
        return self.statement_date.strftime("%Y-%m")


@dataclass
class YearProjection:
    year: int
    annual_savings: float
    cumulative_savings: float
    net_position: float        # cumulative - effective_cost
    degradation_factor: float
    rate_escalation_factor: float


@dataclass
class PaybackResult:
    """Complete payback analysis results."""
    system_cost: float
    federal_tax_credit_pct: float
    federal_tax_credit_amount: float
    effective_cost: float
    annual_savings: float
    simple_payback_years: Optional[float]
    actual_payback_year: Optional[int]
    projection_years: int
    total_savings_lifetime: float
    roi_pct: float
    savings_mode: str           # "data-driven" or "credit-only"
    solar_data_source: Optional[str]
    bills_analyzed: int
    estimated_months: int
    data_period: dict
    avg_monthly_bill: float
    avg_effective_rate: float
    year_by_year: list = field(default_factory=list)
    bill_details: list = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


# ---------------------------------------------------------------------------
# Calculator
# ---------------------------------------------------------------------------

class PaybackCalculator:
    """Computes solar payback and ROI from parsed bills and solar data."""

    def __init__(
        self,
        bills: list[BillRecord],
        solar_data: list[MonthlyProduction],
        system_cost: float,
        federal_tax_credit: float = 0.30,
        rate_escalation: float = 0.03,
        panel_degradation: float = 0.005,
        projection_years: int = 25,
        estimated_months: int = 0,
    ):
        self.bills = bills
        self.solar_data = solar_data
        self.system_cost = system_cost
        self.federal_tax_credit = federal_tax_credit
        self.rate_escalation = rate_escalation
        self.panel_degradation = panel_degradation
        self.projection_years = projection_years
        self.estimated_months = estimated_months

    def effective_cost(self) -> float:
        return self.system_cost * (1 - self.federal_tax_credit)

    def compute_annual_savings(self) -> tuple[float, str]:
        """
        Calculate annual savings from bills + solar data.

        Returns (annual_savings, mode) where mode is "data-driven" or "credit-only".

        Data-driven: uses solar production data to compute self-consumption value.
        Credit-only: falls back to export credits + estimated self-consumption.
        """
        if not self.bills:
            return 0.0, "credit-only"

        # Build lookup: month_key → MonthlyProduction
        solar_by_month = {s.month: s for s in self.solar_data}

        # Compute effective rates from bills
        rates = [b.effective_rate for b in self.bills if b.effective_rate > 0]
        median_rate = median(rates) if rates else 0.12

        # First pass: compute savings for months WITH solar data
        data_driven_savings = {}
        for bill in self.bills:
            solar = solar_by_month.get(bill.month_key)
            if solar and solar.production_kwh > 0:
                self_consumed = solar.self_consumed_kwh
                rate = bill.effective_rate if bill.effective_rate > 0 else median_rate
                avoided = self_consumed * rate
                savings = avoided + bill.energy_payment_credit
                data_driven_savings[bill.month_key] = {
                    "savings": savings,
                    "production_kwh": solar.production_kwh,
                    "self_consumed_kwh": self_consumed,
                    "delivered_kwh": bill.total_delivered_kwh,
                }

        # Derive self-consumption ratio from data-driven months
        if data_driven_savings:
            total_prod = sum(d["production_kwh"] for d in data_driven_savings.values())
            total_self = sum(d["self_consumed_kwh"] for d in data_driven_savings.values())
            sc_ratio = total_self / total_prod if total_prod > 0 else 0.85
        else:
            sc_ratio = 0.85

        # Second pass: compute savings for ALL months
        monthly_savings = []
        for bill in self.bills:
            if bill.month_key in data_driven_savings:
                monthly_savings.append(data_driven_savings[bill.month_key]["savings"])
            else:
                estimated_production = bill.total_delivered_kwh / (1 - sc_ratio) if sc_ratio < 1 else bill.total_delivered_kwh * 2
                estimated_self_consumed = estimated_production - bill.total_delivered_kwh
                rate = bill.effective_rate if bill.effective_rate > 0 else median_rate
                avoided = max(0, estimated_self_consumed) * rate
                savings = avoided + bill.energy_payment_credit
                monthly_savings.append(savings)

        if not monthly_savings:
            return 0.0, "credit-only"

        mode = "data-driven" if data_driven_savings else "credit-only"

        # Annualize: scale to 12 months if we have less than a full year
        total_months = len(monthly_savings)
        total_savings = sum(monthly_savings)
        annual = total_savings * (12 / total_months) if total_months > 0 else 0.0

        return annual, mode

    def year_by_year_projection(self, base_annual_savings: float) -> list[YearProjection]:
        """Forward model: rate escalation + panel degradation."""
        projections = []
        cumulative = 0.0
        eff_cost = self.effective_cost()

        for yr in range(self.projection_years):
            deg = (1 - self.panel_degradation) ** yr
            esc = (1 + self.rate_escalation) ** yr
            annual = base_annual_savings * deg * esc
            cumulative += annual
            projections.append(YearProjection(
                year=yr + 1,
                annual_savings=round(annual, 2),
                cumulative_savings=round(cumulative, 2),
                net_position=round(cumulative - eff_cost, 2),
                degradation_factor=round(deg, 4),
                rate_escalation_factor=round(esc, 4),
            ))
        return projections

    def calculate(self, solar_data_source: Optional[str] = None) -> PaybackResult:
        """Run the full payback analysis."""
        annual_savings, mode = self.compute_annual_savings()
        eff_cost = self.effective_cost()
        tax_credit_amt = self.system_cost * self.federal_tax_credit

        payback_years = None
        if annual_savings > 0:
            payback_years = round(eff_cost / annual_savings, 1)

        projection = self.year_by_year_projection(annual_savings)

        actual_payback_year = None
        for p in projection:
            if p.net_position >= 0:
                actual_payback_year = p.year
                break

        total_lifetime = sum(p.annual_savings for p in projection)
        roi = ((total_lifetime - eff_cost) / eff_cost * 100) if eff_cost > 0 else 0.0

        avg_bill = sum(b.subtotal for b in self.bills) / len(self.bills) if self.bills else 0
        rates = [b.effective_rate for b in self.bills if b.effective_rate > 0]
        avg_rate = median(rates) if rates else 0.0

        dates = [b.statement_date for b in self.bills]
        period = {
            "start": min(dates).isoformat() if dates else None,
            "end": max(dates).isoformat() if dates else None,
        }

        bill_details = [
            {
                "date": b.statement_date.isoformat(),
                "energy_kwh": b.total_energy_kwh,
                "delivered_kwh": b.total_delivered_kwh,
                "credit": b.energy_payment_credit,
                "subtotal": b.subtotal,
                "effective_rate": round(b.effective_rate, 4),
            }
            for b in self.bills
        ]

        return PaybackResult(
            system_cost=self.system_cost,
            federal_tax_credit_pct=self.federal_tax_credit,
            federal_tax_credit_amount=round(tax_credit_amt, 2),
            effective_cost=round(eff_cost, 2),
            annual_savings=round(annual_savings, 2),
            simple_payback_years=payback_years,
            actual_payback_year=actual_payback_year,
            projection_years=self.projection_years,
            total_savings_lifetime=round(total_lifetime, 2),
            roi_pct=round(roi, 1),
            savings_mode=mode,
            solar_data_source=solar_data_source,
            bills_analyzed=len(self.bills),
            estimated_months=self.estimated_months,
            data_period=period,
            avg_monthly_bill=round(avg_bill, 2),
            avg_effective_rate=round(avg_rate, 4),
            year_by_year=[asdict(p) for p in projection],
            bill_details=bill_details,
        )


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def format_summary_table(result: PaybackResult) -> str:
    rows = [
        ("System Cost", f"${result.system_cost:,.2f}"),
        ("Federal Tax Credit", f"-${result.federal_tax_credit_amount:,.2f} ({result.federal_tax_credit_pct:.0%})"),
        ("Effective Cost", f"${result.effective_cost:,.2f}"),
        ("", ""),
        ("Annual Savings", f"${result.annual_savings:,.2f} ({result.savings_mode})"),
        ("Projected Payback Year", str(result.actual_payback_year) if result.actual_payback_year else "Not reached"),
        (f"{result.projection_years}-Year Total Savings", f"${result.total_savings_lifetime:,.2f}"),
        ("ROI", f"{result.roi_pct:.1f}%"),
        ("", ""),
        ("Bills Analyzed", str(result.bills_analyzed)),
    ]
    if result.estimated_months:
        rows.append(("Estimated Months", f"{result.estimated_months} (pre-bill gap)"))
    rows.extend([
        ("Data Period", f"{result.data_period.get('start', '?')} to {result.data_period.get('end', '?')}"),
        ("Avg Monthly Bill", f"${result.avg_monthly_bill:,.2f}"),
        ("Avg Rate/kWh", f"${result.avg_effective_rate:.4f}"),
    ])
    if result.solar_data_source:
        rows.append(("Solar Data Source", result.solar_data_source))

    return tabulate(rows, headers=["Metric", "Value"], tablefmt="grid")


def format_projection_table(result: PaybackResult) -> str:
    """Year-by-year projection table focused on the payback window."""
    projection = result.year_by_year
    if not projection:
        return ""

    key_years: set[int] = {1}

    if result.actual_payback_year:
        payback_yr = result.actual_payback_year
        # 3 years before payback through the payback year itself
        for yr in range(max(1, payback_yr - 3), payback_yr + 1):
            key_years.add(yr)
    else:
        # Never paid back within projection: show milestones up to end
        for milestone in (5, 10, 15, 20):
            if milestone < result.projection_years:
                key_years.add(milestone)

    rows = []
    for p in projection:
        if p["year"] not in key_years:
            continue
        status = "PAID OFF" if p["net_position"] >= 0 else ""
        rows.append([
            p["year"],
            f"${p['annual_savings']:,.2f}",
            f"${p['cumulative_savings']:,.2f}",
            f"${p['net_position']:,.2f}",
            status,
        ])

    return tabulate(
        rows,
        headers=["Year", "Annual Savings", "Cumulative", "vs. Cost", "Status"],
        tablefmt="grid",
    )


def format_bill_detail_table(result: PaybackResult) -> str:
    rows = []
    for b in result.bill_details:
        rows.append([
            b["date"],
            b["energy_kwh"],
            b["delivered_kwh"],
            f"${b['credit']:.2f}",
            f"${b['subtotal']:.2f}",
            f"${b['effective_rate']:.4f}",
        ])
    return tabulate(
        rows,
        headers=["Date", "Energy kWh", "Delivered kWh", "Credit", "Subtotal", "Rate/kWh"],
        tablefmt="grid",
    )


def format_terminal(result: PaybackResult, verbose: bool = False) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("   SOLAR PANEL PAYBACK ANALYSIS")
    lines.append("=" * 60)
    lines.append("")
    lines.append(format_summary_table(result))
    lines.append("")
    lines.append("-" * 60)
    lines.append("   YEAR-BY-YEAR PROJECTION")
    lines.append("-" * 60)
    lines.append(format_projection_table(result))
    if verbose:
        lines.append("")
        lines.append("-" * 60)
        lines.append("   PER-BILL BREAKDOWN")
        lines.append("-" * 60)
        lines.append(format_bill_detail_table(result))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bills_to_records(bill_dicts: list[dict]) -> list[BillRecord]:
    """Convert raw parsed bill dicts to BillRecords."""
    records = []
    seen = set()
    for b in bill_dicts:
        if not b.get("statement_date") or not b.get("subtotal"):
            continue
        sd = datetime.strptime(b["statement_date"], "%m/%d/%Y").date()
        key = sd.isoformat()
        if key in seen:
            continue
        seen.add(key)

        energy = b.get("total_energy_kwh", 0) or 0
        delivered = b.get("total_delivered_kwh", 0) or 0
        credit = b.get("energy_payment_credit", 0) or 0
        subtotal = b.get("subtotal", 0) or 0
        rate = (subtotal / energy) if energy > 0 else 0.0

        records.append(BillRecord(
            statement_date=sd,
            total_energy_kwh=energy,
            total_delivered_kwh=delivered,
            energy_payment_credit=credit,
            subtotal=subtotal,
            effective_rate=rate,
        ))
    return records


def compute_seasonal_averages(records: list[BillRecord]) -> dict[int, dict]:
    """Group bills by month-of-year and average kWh/credit values."""
    buckets: dict[int, list[BillRecord]] = {}
    for r in records:
        buckets.setdefault(r.statement_date.month, []).append(r)

    avgs: dict[int, dict] = {}
    for month, recs in buckets.items():
        n = len(recs)
        avgs[month] = {
            "total_energy_kwh": sum(r.total_energy_kwh for r in recs) / n,
            "total_delivered_kwh": sum(r.total_delivered_kwh for r in recs) / n,
            "energy_payment_credit": sum(r.energy_payment_credit for r in recs) / n,
        }
    return avgs


def backfill_bills(
    records: list[BillRecord],
    install_date: date,
    seasonal_avgs: dict[int, dict],
) -> list[BillRecord]:
    """Generate synthetic bill records from install_date to the month before the earliest bill."""
    if not records:
        return []

    earliest = min(r.statement_date for r in records)
    earliest_rate = min(
        (r.effective_rate for r in records if r.effective_rate > 0),
        default=0.12,
    )

    synthetics = []
    year, month = install_date.year, install_date.month
    first_bill_month = earliest.replace(day=1)

    while date(year, month, 1) < first_bill_month:
        avg = seasonal_avgs.get(month, {
            "total_energy_kwh": 600,
            "total_delivered_kwh": 100,
            "energy_payment_credit": 2.0,
        })
        energy = avg["total_energy_kwh"]
        delivered = avg["total_delivered_kwh"]
        credit = avg["energy_payment_credit"]
        subtotal = round(energy * earliest_rate, 2)

        synthetics.append(BillRecord(
            statement_date=date(year, month, 5),
            total_energy_kwh=round(energy, 1),
            total_delivered_kwh=round(delivered, 1),
            energy_payment_credit=round(credit, 2),
            subtotal=subtotal,
            effective_rate=earliest_rate,
        ))

        month += 1
        if month > 12:
            month = 1
            year += 1

    return synthetics


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Solar Panel Payback Calculator — Xcel Energy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--system-cost", type=float, required=True,
                   help="Total upfront cost of solar system ($)")
    p.add_argument("--config", default="config.yaml",
                   help="Path to config.yaml (default: config.yaml)")
    p.add_argument("--bills-dir",
                   help="Directory containing PDF bills (overrides config)")
    p.add_argument("--install-date",
                   help="System install date YYYY-MM-DD (for projection start)")
    p.add_argument("--solar-source", choices=["auto", "influx", "enphase", "bills_only", "none"],
                   help="Solar data source (overrides config)")
    p.add_argument("--federal-tax-credit", type=float,
                   help="Federal ITC %% as decimal, e.g. 0.30 (default: 0 / disabled)")
    p.add_argument("--no-tax-credit", action="store_true",
                   help="Disable federal tax credit entirely")
    p.add_argument("--rate-escalation", type=float,
                   help="Annual rate increase %% as decimal, e.g. 0.03 (overrides config)")
    p.add_argument("--panel-degradation", type=float,
                   help="Annual degradation %% as decimal, e.g. 0.005 (overrides config)")
    p.add_argument("--projection-years", type=int,
                   help="Years to project forward (overrides config)")
    p.add_argument("--output", choices=["terminal", "json", "both"], default="terminal",
                   help="Output format (default: terminal)")
    p.add_argument("--json-out",
                   help="Write JSON output to this file path")
    p.add_argument("--verbose", action="store_true",
                   help="Show per-bill breakdown table")
    p.add_argument("--setup-keyring", action="store_true",
                   help="Interactively store secrets in system keyring, then exit")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.setup_keyring:
        from config import setup_keyring_interactive
        setup_keyring_interactive()
        return

    # Load config
    try:
        config = AppConfig.load(args.config)
    except FileNotFoundError:
        print(f"Config not found: {args.config}. Create one from config.yaml.")
        sys.exit(1)

    # CLI overrides
    bills_dir = args.bills_dir or config.bills_directory
    federal_tc = 0.0 if args.no_tax_credit else (args.federal_tax_credit or 0.0)
    rate_esc = args.rate_escalation if args.rate_escalation is not None else config.rate_escalation / 100
    degradation = args.panel_degradation if args.panel_degradation is not None else 0.005
    proj_years = args.projection_years or config.panel_lifespan_years
    if args.solar_source:
        config._yaml.setdefault("solar", {})["source"] = args.solar_source

    # Parse bills
    bp = BillParser(bills_dir)
    bill_dicts = bp.parse_all()
    complete_dicts = [b for b in bill_dicts if all(
        v is not None for k, v in b.items() if not k.startswith("_")
    )]
    if not complete_dicts:
        print(f"No complete bills found in {bills_dir}")
        sys.exit(1)
    print(f"Parsed {len(complete_dicts)} bills from {bills_dir}", file=sys.stderr)

    records = bills_to_records(complete_dicts)

    # Backfill estimated bills if install date precedes earliest bill
    install_dt = None
    estimated_months = 0
    if args.install_date:
        install_dt = datetime.strptime(args.install_date, "%Y-%m-%d").date()

    if install_dt and records:
        earliest_bill = min(r.statement_date for r in records)
        if install_dt.replace(day=1) < earliest_bill.replace(day=1):
            seasonal_avgs = compute_seasonal_averages(records)
            synthetics = backfill_bills(records, install_dt, seasonal_avgs)
            if synthetics:
                estimated_months = len(synthetics)
                print(
                    f"Backfilled {estimated_months} months from "
                    f"{install_dt.isoformat()} to {earliest_bill.isoformat()} (estimated)",
                    file=sys.stderr,
                )
                records = synthetics + records

    # Fetch solar data
    dates = [r.statement_date for r in records]
    solar_start_date = install_dt or min(dates).replace(day=1)
    start = datetime.combine(solar_start_date.replace(day=1), datetime.min.time())
    end = datetime.combine(max(dates), datetime.max.time())

    resolver = SolarDataResolver(config)
    solar_data, solar_source = resolver.resolve(start, end)
    if solar_data:
        print(f"Solar data: {len(solar_data)} months from {solar_source}", file=sys.stderr)
    else:
        print("No solar production data available — using credit-only estimation", file=sys.stderr)

    # Calculate
    calc = PaybackCalculator(
        bills=records,
        solar_data=solar_data,
        system_cost=args.system_cost,
        federal_tax_credit=federal_tc,
        rate_escalation=rate_esc,
        panel_degradation=degradation,
        projection_years=proj_years,
        estimated_months=estimated_months,
    )
    result = calc.calculate(solar_data_source=solar_source)

    # Output
    if args.output in ("terminal", "both"):
        print()
        print(format_terminal(result, verbose=args.verbose))

    if args.output in ("json", "both"):
        json_str = result.to_json()
        if args.output == "both":
            print()
            print("=" * 60)
            print("   JSON OUTPUT")
            print("=" * 60)
        print(json_str)

    if args.json_out:
        json_str = result.to_json()
        with open(args.json_out, "w") as f:
            f.write(json_str)
        print(f"JSON written to {args.json_out}", file=sys.stderr)


if __name__ == "__main__":
    main()
