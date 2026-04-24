# Xcel Bill to InfluxDB

> [!WARNING]
> This tool extracts energy usage and billing data from Xcel Energy PDF bills and stores it in InfluxDB. It is tailored to the standard Xcel PDF format—if your bill layout differs, you may need to adjust the extraction patterns.

> [!NOTE]
> Built to track solar production, grid export/import, and bill data. May error if your bill lacks solar-related fields.

## Features

- **PDF Bill Parsing** -- extracts delivered kWh, total energy, credits, subtotal, statement date from Xcel bills
- **InfluxDB Integration** -- stores parsed bills as `energy_usage` time-series points; optional solar data from Influx or Enphase API
- **Solar Payback Calculator** -- projects lifetime savings, simple payback, ROI with ITC, degradation, and rate escalation
- **Ops Scripts** -- pipelines for extracting solar history from Home Assistant InfluxDB backups and restoring to production
- **Secret Management** -- keyring-first, with env var and YAML fallbacks

## Installation

```bash
cd src
pip install -r requirements.txt
```

**Dependencies:** `pypdf`, `influxdb`, `tabulate`, `pyyaml`, `keyring`, `requests`

## Configuration

Copy and edit `config.yaml`:

```bash
cp src/config.yaml.example src/config.yaml
$EDITOR src/config.yaml
```

### config.yaml reference

```yaml
influxdb:
  host: localhost       # InfluxDB server
  port: 8086
  bills_username: xcel      # read-write for energy_usage writes
  bills_db: xcel_bill       # database for bill data
  readonly_username: throw-away  # read-only for solar data
  readonly_password: ""     # use keyring or env var
  readonly_database: solar   # database for monthly solar metrics
  readonly_measurement: solar_monthly

enphase:
  client_id: ""              # use keyring or XCEL_ENPHASE_CLIENT_ID env var
  client_secret: ""          # use keyring or XCEL_ENPHASE_CLIENT_SECRET env var
  bearer_token: ""           # use keyring or XCEL_ENPHASE_BEARER_TOKEN env var
  system_id: ""
  envoy_host: envoy.local
  use_cloud: true

solar:
  source: auto               # auto | influx | enphase | bills_only | none
  rate_escalation: 3.0       # annual electricity rate increase (%)
  panel_lifespan_years: 25

bills:
  directory: ./bills         # PDF bills directory
```

### Secret resolution order

Secrets are resolved in this order (most secure first):

1. **System keyring** -- `python config.py --setup-keyring` to store interactively
2. **Environment variables** -- `XCEL_INFLUX_BILLS_PASSWORD`, `XCEL_INFLUX_SOLAR_PASSWORD`, `XCEL_ENPHASE_CLIENT_ID`, `XCEL_ENPHASE_CLIENT_SECRET`, `XCEL_ENPHASE_BEARER_TOKEN`
3. **config.yaml plaintext** -- fallbacks for all secrets

## Bill Ingestion

Parse PDF bills and write to InfluxDB:

```bash
cd src
python xcel-to-influx.py --config config.yaml
```

## Solar Payback Calculator

Projects costs, savings, and payback timeline. **Does not write to InfluxDB** -- only reads PDFs and optionally reads solar data.

```bash
cd src
python solar_payback.py --system-cost 25000 --install-date 2023-06-01
```

### solar_payback.py flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--system-cost` | Yes | -- | Total upfront cost ($) |
| `--bills-dir` | No | from config | PDF bills directory |
| `--install-date` | No | -- | System install date (YYYY-MM-DD). Enables backfill of months before first bill. |
| `--solar-source` | No | from config | Data source: `auto`, `influx`, `enphase`, `bills_only`, `none` |
| `--federal-tax-credit` | No | 0.0 | Federal ITC as decimal (e.g. `0.30` for 30%) |
| `--no-tax-credit` | No | -- | Disable ITC entirely |
| `--rate-escalation` | No | from config | Annual rate increase as decimal (e.g. `0.03`) |
| `--panel-degradation` | No | 0.005 | Annual panel degradation (default 0.5%/yr) |
| `--projection-years` | No | from config | Years to project (default: panel_lifespan_years) |
| `--output` | No | terminal | Output format: `terminal`, `json`, `both` |
| `--json-out` | No | -- | Write JSON output to this file path |
| `--verbose` | No | -- | Show per-bill breakdown table |
| `--setup-keyring` | No | -- | Interactive keyring setup, then exit |
| `--config` | No | config.yaml | Path to config file |

## TOU Billing Comparison

Compares your actual energy costs under the standard flat-rate plan against Xcel Energy SD's Residential Time-of-Day (TOU) plan using interval data from Home Assistant InfluxDB.

### Quick Start

**1. Configure HA InfluxDB credentials** in `config.yaml`:

```yaml
ha_influxdb:
  host: localhost            # your InfluxDB host
  port: 8086
  database: homeassistant
  username: ""               # read-only credentials
  password: ""               # use keyring for better security
  consumption_entity: ""     # set after running --discover
  consumption_field: value
```

**2. Discover available energy entities:**

```bash
cd src
python tou_comparison.py --config config.yaml --discover
```

This queries HA InfluxDB for energy-related sensors and prints a table with entity names, sample counts, and date ranges.

**3. Set your consumption entity** in `config.yaml` based on the discovery output:

```yaml
ha_influxdb:
  consumption_entity: your_energy_entity_here  # from --discover output
```

**4. Run the comparison:**

```bash
python tou_comparison.py --config config.yaml \
  --start-date 2025-10-01 --end-date 2026-04-01
```

### tou_comparison.py flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--discover` | No | -- | List available energy entities in HA InfluxDB, then exit |
| `--entity` | No | from config | Override consumption entity_id |
| `--field` | No | from config | Override field name (usually `value`) |
| `--start-date` | No | Jan 1 of end year | Analysis start (YYYY-MM-DD) |
| `--end-date` | No | today | Analysis end (YYYY-MM-DD) |
| `--output` | No | terminal | Output format: `terminal`, `json`, `both` |
| `--json-out` | No | -- | Write JSON output to file |
| `--verbose` | No | -- | Show additional detail |
| `--config` | No | config.yaml | Path to config file |

### TOU rate configuration

Rates are in `config.yaml` under `tou_rates`. Update when the PUC approves new rate schedules:

```yaml
tou_rates:
  peak_start_hour: 9          # on-peak: 9 AM - 9 PM weekdays
  peak_end_hour: 21
  peak_days: [0, 1, 2, 3, 4]  # Mon-Fri
  summer_months: [6, 7, 8, 9]
  summer_on_peak: 0.21806      # $/kWh
  winter_on_peak: 0.17590
  off_peak: 0.04610
  flat_summer: 0.11153
  flat_winter_first_1000: 0.09585
  flat_winter_excess: 0.09327
  fuel_surcharge: 0.02634
  tou_customer_charge: 10.30   # $/month
  flat_customer_charge: 8.30
```

## Energy Data Pipeline

The `energy_interval` measurement in the `solar` InfluxDB stores raw cumulative Wh interval data from Home Assistant, enabling long-term TOU analysis across multiple years.

### Schema

```
energy_interval,entity_id=<sensor>,source=<ha|backup> value=<cumulative_Wh>
```

### Live Sync (HA InfluxDB -> solar DB)

Syncs new HA interval data into `solar` DB. Idempotent -- uses high-water mark to resume.

```bash
cd src
# Dry-run -- see how many points would be synced
python energy_sync.py --config config.yaml --dry-run

# Run live sync (all configured entities)
python energy_sync.py --config config.yaml

# Sync specific entity
python energy_sync.py --config config.yaml --entity your_entity_id

# Backfill from a specific date
python energy_sync.py --config config.yaml --backfill-from 2024-01-01T00:00:00Z
```

Configure in `config.yaml`:

```yaml
sync:
  target_measurement: energy_interval
  container_host: containers        # SSH host for backup extraction
  backup_dir: /mnt               # backup tar location on container_host
  entities:
    - envoy_202150043328_lifetime_net_energy_consumption
  batch_size: 5000
```

### Backup Extraction (HA .tar -> solar DB)

Extracts interval data from HA backup tarballs on the container host, computing net consumption from gross consumption minus production where the direct net entity is unavailable.

```bash
# List viable backups (auto-skips encrypted and empty ones)
python extract_backup.py --config config.yaml --dry-run

# Extract all viable backups (one-time, slow for large backups)
python extract_backup.py --config config.yaml --all

# Extract a specific backup
python extract_backup.py --config config.yaml --backup 2fcb7cf5.tar

# Keep extracted temp dirs after processing (for debugging)
python extract_backup.py --config config.yaml --all --keep-extracted
```

Requires SSH access to `sync.container_host` with Docker available.

### Cron Example

Run live sync every 15 minutes:

```bash
*/15 * * * * cd /home/xcel/src && ./energy_sync.py --config config.yaml >> /var/log/energy_sync.log 2>&1
```

### TOU Analysis with Full History

```bash
# Use curated solar DB data instead of live HA InfluxDB
python tou_comparison.py --config config.yaml --source solar

# Auto-selects full available date range from solar DB
python tou_comparison.py --config config.yaml --source solar --start-date 2024-01-01
```

## Cost Projection Only (No InfluxDB)

To project costs with **zero InfluxDB interaction** -- only PDF parsing:

```bash
cd src
python solar_payback.py \
  --system-cost 25000 \
  --bills-dir ../bills \
  --solar-source none \
  --install-date 2023-06-01
```

`--solar-source none` skips all solar data fetch (Influx and Enphase). The tool still reads your PDF bills for baseline cost data.

## Ops Scripts

Solar history recovery and import pipelines (from HA InfluxDB backups):

| Script | Purpose |
|--------|---------|
| `extract_solar_history.py` | Extracts envoy solar data from HA backup `.tar` files into CSVs |
| `import_solar_history.py` | Imports CSV exports into production InfluxDB |
| `chunked_import.sh` | Batch-write large line-protocol files to `solar` DB |
| `full_solar_restore.sh` | Full extract → convert → import pipeline (v1) |
| `full_solar_restore_v2.sh` | Full extract → convert → import pipeline (v2) |
| `import_3_backups.sh` | Import three specific backups |
| `import_to_production.sh` | Import line-protocol files to `homeassistant` DB |

These scripts reference hard-coded paths and IP addresses. Review and adjust before use.

## Example Grafana Query

Once bill data is in InfluxDB:

```
SELECT mean("total_energy_kwh") FROM "energy_usage" GROUP BY time(30d)
```

## InfluxDB Schema

**Bill data** -- measurement: `energy_usage` (database: `xcel_bill`)

| Field | Type | Description |
|-------|------|-------------|
| `total_delivered_kwh` | float | Delivered by customer (kWh) |
| `total_energy_kwh` | float | Total energy (kWh) |
| `energy_payment_credit` | float | Export credit ($) |
| `subtotal` | float | Bill subtotal ($) |
| `statement_date` | timestamp | Bill date |

**Solar data** -- measurement: `solar_monthly` (database: `solar`), tag: `metric`

| Metric | Description |
|--------|-------------|
| `produced` | Total panel production (Wh) |
| `consumed` | Household consumption (Wh) |
| `exported` | Grid export (Wh) |
| `imported` | Grid import (Wh) |
| `battery_charged` | Battery charging (Wh) |
| `battery_discharged` | Battery discharge (Wh) |

## License

AGPL v3. See `src/LICENSE`.
