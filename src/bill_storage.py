"""
Bill storage pipeline -- writes parsed Xcel bill data to InfluxDB.

Used by xcel-to-influx.py (the bill ingestion CLI).
The solar_payback.py does NOT use this -- it reads from InfluxDB directly.
"""

import os
from datetime import datetime
from typing import Optional

from config import AppConfig
from bill_parser import BillParser


class BillStorage:
    """Handles writing parsed bill data to InfluxDB."""

    MEASUREMENT = "energy_usage"

    def __init__(self, config: Optional[AppConfig] = None, config_path: str = "config.yaml"):
        self.config = config or AppConfig.load(config_path)

    def write_bill(self, data: dict) -> bool:
        """
        Write a single bill record to InfluxDB.
        Returns True on success, False on failure.
        """
        try:
            client = self.config.influx_client(self.config.bills_db)
            statement_date = datetime.strptime(
                data["statement_date"], "%m/%d/%Y"
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            json_body = [{
                "measurement": self.MEASUREMENT,
                "time": statement_date,
                "fields": {
                    "total_delivered_kwh": float(data["total_delivered_kwh"]),
                    "total_energy_kwh": float(data["total_energy_kwh"]),
                    "energy_payment_credit": float(data["energy_payment_credit"]),
                    "subtotal": float(data["subtotal"]),
                }
            }]
            client.write_points(json_body)
            return True
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")
            return False

    def process_directory(self, pdf_dir: str) -> None:
        """
        Parse all PDFs in a directory and write to InfluxDB.
        """
        parser = BillParser(pdf_dir)
        for filename in os.listdir(pdf_dir):
            if not filename.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(pdf_dir, filename)
            try:
                data = parser.extract_bill_data(pdf_path)
                if all(v is not None for v in data.values()):
                    if self.write_bill(data):
                        print(f"Processed {filename} successfully")
                    else:
                        print(f"Failed to write {filename} to InfluxDB")
                else:
                    print(f"Missing data in {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")


def main() -> None:
    """CLI entry point for bill ingestion."""
    config = AppConfig.load()
    storage = BillStorage(config)
    storage.process_directory(config.bills_directory)


if __name__ == "__main__":
    main()
