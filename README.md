# Xcel Bill to InfluxDB

> [!WARNING]
> This tool is designed to extract structured energy usage and billing data from Xcel Energy PDF bills and store it in an InfluxDB time-series database. It is tailored to the standard Xcel PDF format—if your bill layout differs, you may need to adjust the extraction patterns.

> [!NOTE]
> I made this to help track my solar and the amount I export to the grid. It's possible that the script will error if you do not have certain fields in your bill that are related to this.

**Xcel Bill to InfluxDB** is a Python utility for automating the extraction of energy usage, credits, and billing subtotals from Xcel Energy PDF bills and importing them into InfluxDB. This enables easy time-series analysis and visualization of your household energy data using tools like Grafana.

## Features

- **Automated PDF Parsing:** Extracts key metrics (delivered kWh, total energy, energy payment credits, subtotal, statement date) from Xcel Energy bills.
- **Batch Processing:** Processes all PDF bills in a specified directory.
- **InfluxDB Integration:** Stores extracted data as time-series points for historical analysis and visualization.
- **Flexible Extraction:** Uses robust regex patterns to handle minor variations in bill layout.

## Installation

Clone this repository and install the required dependencies:

```bash
git clone https://github.com/yourusername/xcel-to-influx.git
cd xcel-to-influx
pip install -r requirements.txt
```

**Dependencies:**
- `pypdf`
- `influxdb`

## Configuration

Edit the InfluxDB connection settings at the top of `xcel-to-influx.py` to match your environment:

```python
INFLUX_SERVER = "localhost"
INFLUX_PORT = 8086
INFLUX_USERNAME = "influx"
INFLUX_PASSWORD = "yourpassword"
INFLUX_DB = "xcel_bill"
```

## Usage

Place all your Xcel PDF bills in a directory (e.g., `~/Downloads/xcel_bills`). Then run:

```bash
python xcel-to-influx.py
```

By default, the script processes all `.pdf` files in the `PDF_DIR` specified at the bottom of the script. You can change this path as needed.

### Example Output

For each bill, the script will extract and write to InfluxDB:

- Total Delivered by Customer (kWh)
- Total Energy (kWh)
- Energy Payment Credit ($)
- Subtotal ($)
- Statement Date (used as the InfluxDB timestamp)

Example InfluxDB point:

| time                | total_delivered_kwh | total_energy_kwh | energy_payment_credit | subtotal |
|---------------------|--------------------|------------------|----------------------|----------|
| 2024-05-06T00:00:00Z| 369                | 259              | 13.28                | 31.53    |

## Input File Format

The script expects standard Xcel Energy PDF bills. It uses regular expressions to locate and extract the following fields:

- **Statement Date:** e.g., `05/06/2024`
- **Total Delivered by Customer (kWh):** e.g., `369`
- **Total Energy (kWh):** e.g., `259`
- **Energy Payment Credit ($):** e.g., `13.28`
- **Subtotal ($):** e.g., `31.53`

If your bill format is significantly different, see the `extraction_patterns` dictionary in the script to adjust the regex patterns.

## Module Structure

| Function               | Description                                                  |
|------------------------|-------------------------------------------------------------|
| `extract_bill_data`    | Extracts metrics from a single PDF file                     |
| `extract_text_from_pdf`| Reads and concatenates text from all PDF pages              |
| `extract_with_patterns`| Tries multiple regex patterns to extract a single field     |
| `write_to_influxdb`    | Writes a data point to InfluxDB                             |
| `process_bills`        | Processes all PDFs in a directory                           |

## Example Grafana Query

Once your data is in InfluxDB, you can visualize it in Grafana with queries like:

```
SELECT mean("total_energy_kwh") FROM "energy_usage" GROUP BY time(30d)
```

## Contributing

Contributions are welcome! To add support for new bill formats or additional fields:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-field`)
3. Update the extraction patterns in `xcel-to-influx.py`
4. Submit a pull request

## License

AGPL v 3. See `LICENSE` for details.

---

> [!NOTE]
> This script is provided as-is and may require adjustments for non-standard Xcel bill layouts. For issues or feature requests, please open an issue on GitHub.
