import re
import os
from datetime import datetime
from pypdf import PdfReader
from influxdb import InfluxDBClient

# Configuration (match your Garmin setup)
INFLUX_SERVER = "xxx"
INFLUX_PORT = 8086
INFLUX_USERNAME = "influx"
INFLUX_PASSWORD = "xxx"
INFLUX_DB = "xcel_bill"

def extract_bill_data(pdf_path):
    """
    Extract energy bill data from PDF with improved maintainability.
    Returns a dictionary with extracted values.
    """
    extraction_patterns = {
        "statement_date": [
            r"STATEMENT DATE.*?(\d{2}/\d{2}/\d{4})",
        ],
        "total_delivered_kwh": [
            r"Total Delivered by Customer\s*\d+\s*Actual\s*\d+\s*Actual\s*(\d+)\s*kWh",
            r"Total Delivered by Customer.*?(\d+)\s*kWh",
            r"Total Delivered by Customer Actual (\d+) kWh",
        ],
        "total_energy_kwh": [
            r"Total Energy\s*\d+\s*Actual\s*\d+\s*Actual\s*(\d+)\s*kWh",
            r"Total Energy.*?(\d+)\s*kWh",
            r"Total Energy Actual (\d+) kWh",
        ],
        "energy_payment_credit": [
            r"Energy Payment\s*\d+\s*kWh\s*-\s*\$\d+\.\d+\s*-\s*\$\s*([\d\.]+)\s*CR",
            r"Energy Payment.*?-\s*\$\s*([\d\.]+)\s*CR",
            r"Energy Payment \d+.\d+ kWh - \$\d.\d+ - \$(\d+.\d+) CR",
        ],
        "subtotal": [
            r"Subtotal\s*\$([\d\.]+)",
        ],
    }
    
    # Data type conversion functions
    converters = {
        "statement_date": lambda x: x,  # Keep as string for now
        "total_delivered_kwh": int,
        "total_energy_kwh": int,
        "energy_payment_credit": float,
        "subtotal": float,
    }
    
    # Extract text from PDF
    text = extract_text_from_pdf(pdf_path)
    
    # Extract all fields using the patterns
    results = {}
    for field, patterns in extraction_patterns.items():
        results[field] = extract_with_patterns(text, patterns, converters[field])
    
    return results

def extract_text_from_pdf(pdf_path):
    """Extract all text from a PDF file."""
    reader = PdfReader(pdf_path)
    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"
    return text

def extract_with_patterns(text, patterns, converter):
    """
    Try multiple regex patterns to extract a value.
    Returns converted value or None if no pattern matches.
    """
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            try:
                return converter(match.group(1))
            except (ValueError, IndexError):
                continue
    return None




def write_to_influxdb(data):
    """Write extracted data to InfluxDB"""
    client = InfluxDBClient(
        INFLUX_SERVER,
        INFLUX_PORT,
        INFLUX_USERNAME,
        INFLUX_PASSWORD,
        INFLUX_DB
    )
    statement_date = datetime.strptime(data["statement_date"], "%m/%d/%Y").strftime("%Y-%m-%dT%H:%M:%SZ")
    json_body = [{
        "measurement": "energy_usage",
        "time": statement_date,
        "fields": {
            "total_delivered_kwh": data["total_delivered_kwh"],
            "total_energy_kwh": data["total_energy_kwh"],
            "energy_payment_credit": data["energy_payment_credit"],
            "subtotal": data["subtotal"]
        }
    }]
    
    client.write_points(json_body)

def process_bills(pdf_directory):
    """Process all PDFs in a directory"""
    for filename in os.listdir(pdf_directory):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(pdf_directory, filename)
            try:
                data = extract_bill_data(pdf_path)
                if all(data.values()):
                    write_to_influxdb(data)
                    print(f"Processed {filename} successfully")
                else:
                    print(f"Missing data in {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    # Set your PDF directory path
    PDF_DIR = "/home/stratus/Downloads/xcel_bills"
    process_bills(PDF_DIR)
