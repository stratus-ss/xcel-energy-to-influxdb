import re
import os
from datetime import datetime
from pypdf import PdfReader


class BillParser:
    """
    Parses Xcel Energy PDF bills and extracts energy usage + credit data.
    Handles multiple PDF format variations that have evolved over time.
    """

    def __init__(self, pdf_directory: str):
        self.pdf_directory = pdf_directory

    extraction_patterns = {
        "statement_date": [
            r"\d{9}\s+(\d{2}/\d{2}/\d{4})",
        ],
        "total_energy_kwh": [
            r"TotalEnergy \d+Actual \d+ Actual (\d+) kWh",
            r"TotalEnergy \d+Actual \d+ Actual (\d+)kWh",
            r"TotalEnergy \d+ Actual \d+ Actual (\d+) kWh",
            r"TotalEnergy \d+ Actual \d+ Actual (\d+)kWh",
            r"TotalEnergy \d+ Actual (\d+) kWh",
            r"TotalEnergy \d+ Actual (\d+)kWh",
            r"Total Energy \d+ Actual \d+Actual (\d+) kWh",
            r"Total Energy \d+ Actual \d+Actual (\d+)kWh",
            r"Total Energy \d+ Actual \d+ Actual (\d+) kWh",
            r"Total Energy \d+ Actual \d+ Actual (\d+)kWh",
            r"Total Energy \d+ Actual (\d+)kWh",
            r"Total Energy \d+ Actual (\d+) kWh",
            r"TotalEnergy Actual (\d+(?:\.\d+)?)\s*kWh",
            r"Total Energy Actual (\d+) kWh",
            r"Total Energy Actual (\d+)kWh",
        ],
        "total_delivered_kwh": [
            r"TotalDeliveredby Customer \d+ Actual \d+ Actual (\d+) kWh",
            r"TotalDeliveredby Customer \d+ Actual \d+ Actual (\d+)kWh",
            r"Total Deliveredby Customer \d+ Actual \d+ Actual (\d+) kWh",
            r"Total Deliveredby Customer \d+ Actual (\d+) kWh",
            r"Total Deliveredby Customer \d+ Actual (\d+)kWh",
            r"Total Deliveredby Customer Actual (\d+) kWh",
            r"Total Deliveredby Customer Actual (\d+)kWh",
            r"TotalEnergy \d+Actual \d+ Actual (\d+) kWh",
            r"TotalEnergy \d+Actual \d+ Actual (\d+)kWh",
            r"TotalEnergy \d+ Actual \d+ Actual (\d+) kWh",
            r"TotalEnergy \d+ Actual \d+ Actual (\d+)kWh",
            r"Total Energy \d+ Actual \d+ Actual (\d+) kWh",
            r"Total Energy \d+ Actual \d+ Actual (\d+)kWh",
            r"TotalEnergy Actual (\d+(?:\.\d+)?)\s*kWh",
        ],
        "energy_payment_credit": [
            r"EnergyPayment\s+[\d.]+\s*kWh\s+-\s*\$[\d.]+\s+-\s*\$([\d.]+)\s+CR",
            r"EnergyPayment\s+[\d.]+\s*kWh\s+-\s*\$[\d.]+\s+-\s*\$([\d.]+)CR",
            r"EnergyPayment\s+[\d.]+\s*kWh\s+-\s*\$[\d.]+\s+-\s*\$\s*([\d.]+)\s*CR",
        ],
        "subtotal": [
            r"Subtotal\s+\$([\d.]+)",
        ],
    }

    converters = {
        "statement_date": lambda x: x,
        "total_delivered_kwh": float,
        "total_energy_kwh": float,
        "energy_payment_credit": float,
        "subtotal": float,
    }

    def extract_bill_data(self, pdf_path: str) -> dict:
        """Extract energy bill data from a single PDF."""
        text = self._extract_text_from_pdf(pdf_path)
        results = {}
        for field, patterns in self.extraction_patterns.items():
            results[field] = self._extract_with_patterns(text, patterns, self.converters[field])
        return results

    def _extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract all text from a PDF file."""
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text

    def _extract_with_patterns(self, text: str, patterns: list, converter):
        """Try multiple regex patterns to extract a value. Returns converted value or None."""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    return converter(match.group(1))
                except (ValueError, IndexError):
                    continue
        return None

    def parse_all(self) -> list[dict]:
        """Parse all PDFs in the directory, return sorted list of bill dicts."""
        bills = []
        for filename in os.listdir(self.pdf_directory):
            if filename.lower().endswith(".pdf"):
                pdf_path = os.path.join(self.pdf_directory, filename)
                try:
                    data = self.extract_bill_data(pdf_path)
                    if all(v is not None for v in data.values()):
                        data["_filename"] = filename
                        bills.append(data)
                except Exception:
                    pass
        bills.sort(key=lambda b: datetime.strptime(b["statement_date"], "%m/%d/%Y"))
        return bills