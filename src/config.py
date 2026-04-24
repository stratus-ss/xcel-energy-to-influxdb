"""
Unified configuration management.

Loads non-sensitive settings from config.yaml, secrets from system keyring
or environment variables with config.yaml plaintext as final fallback.

Secret resolution order:
  1. System keyring (most secure)
  2. Environment variable (convenient for CI/containers)
  3. config.yaml plaintext (convenient fallback)
"""

import os

import keyring
import yaml
from influxdb import InfluxDBClient
from pathlib import Path


SERVICE_NAME = "xcel-solar"
SECRET_KEYS = [
    "bills_password",
    "readonly_password",
    "enphase_client_id",
    "enphase_client_secret",
    "enphase_bearer_token",
    "ha_influxdb_password",
]

# Map secret key → (yaml_path segments, env var name)
_SECRET_DEFS = {
    "bills_password": (["influxdb", "bills_password"], "XCEL_INFLUX_BILLS_PASSWORD"),
    "readonly_password": (["influxdb", "readonly_password"], "XCEL_INFLUX_SOLAR_PASSWORD"),
    "enphase_client_id": (["enphase", "client_id"], "XCEL_ENPHASE_CLIENT_ID"),
    "enphase_client_secret": (["enphase", "client_secret"], "XCEL_ENPHASE_CLIENT_SECRET"),
    "enphase_bearer_token": (["enphase", "bearer_token"], "XCEL_ENPHASE_BEARER_TOKEN"),
    "ha_influxdb_password": (["ha_influxdb", "password"], "XCEL_HA_INFLUXDB_PASSWORD"),
}


class AppConfig:
    """
    Unified configuration for all GLM_merge tools.

    Secret resolution: keyring → env var → config.yaml plaintext.
    """

    def __init__(self, yaml_data: dict):
        self._yaml = yaml_data

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "AppConfig":
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"config.yaml not found at {path.resolve()}")
        with open(path) as f:
            yaml_data = yaml.safe_load(f)
        return cls(yaml_data)

    # -------------------------------------------------------------------------
    # Secrets (keyring > env var > config.yaml plaintext)
    # -------------------------------------------------------------------------

    def _get_secret(self, key: str) -> str:
        yaml_path, env_var = _SECRET_DEFS[key]

        # 1. Keyring (most secure)
        try:
            val = keyring.get_password(SERVICE_NAME, key)
            if val:
                return val
        except Exception:
            pass

        # 2. Environment variable
        env_val = os.environ.get(env_var, "")
        if env_val:
            return env_val

        # 3. config.yaml plaintext
        val = self._yaml
        for segment in yaml_path:
            val = val.get(segment, "") if isinstance(val, dict) else ""
        return val if val else ""

    # -------------------------------------------------------------------------
    # Bills credentials (read-write)
    # -------------------------------------------------------------------------

    @property
    def bills_username(self) -> str:
        return self._yaml.get("influxdb", {}).get("bills_username", "influx")

    @property
    def bills_password(self) -> str:
        return self._get_secret("bills_password")

    # -------------------------------------------------------------------------
    # Readonly credentials (solar data)
    # -------------------------------------------------------------------------

    @property
    def readonly_username(self) -> str:
        return self._yaml.get("influxdb", {}).get("readonly_username", "throw-away")

    @property
    def readonly_password(self) -> str:
        return self._get_secret("readonly_password")

    # -------------------------------------------------------------------------
    # Enphase secrets
    # -------------------------------------------------------------------------

    @property
    def enphase_client_id(self) -> str:
        return self._get_secret("enphase_client_id")

    @property
    def enphase_client_secret(self) -> str:
        return self._get_secret("enphase_client_secret")

    @property
    def enphase_bearer_token(self) -> str:
        return self._get_secret("enphase_bearer_token")

    # -------------------------------------------------------------------------
    # InfluxDB
    # -------------------------------------------------------------------------

    @property
    def influx_host(self) -> str:
        return self._yaml.get("influxdb", {}).get("host", "localhost")

    @property
    def influx_port(self) -> int:
        return int(self._yaml.get("influxdb", {}).get("port", 8086))

    @property
    def bills_db(self) -> str:
        return self._yaml.get("influxdb", {}).get("bills_db", "xcel_bill")

    @property
    def solar_db(self) -> str:
        return self._yaml.get("influxdb", {}).get("readonly_database", "solar")

    @property
    def solar_measurement(self) -> str:
        return self._yaml.get("influxdb", {}).get("readonly_measurement", "solar_monthly")

    def influx_client(self, db_name: str):
        """Create InfluxDBClient for specified database (bills credentials)."""
        return InfluxDBClient(
            self.influx_host, self.influx_port,
            self.bills_username, self.bills_password,
            db_name,
        )

    def influx_client_solar(self):
        """Create InfluxDBClient for solar database (readonly credentials)."""
        return InfluxDBClient(
            self.influx_host, self.influx_port,
            self.readonly_username, self.readonly_password,
            self.solar_db,
        )

    # -------------------------------------------------------------------------
    # Enphase
    # -------------------------------------------------------------------------

    @property
    def enphase_system_id(self) -> str:
        return self._yaml.get("enphase", {}).get("system_id", "")

    @property
    def enphase_envoy_host(self) -> str:
        return self._yaml.get("enphase", {}).get("envoy_host", "envoy.local")

    @property
    def enphase_use_cloud(self) -> bool:
        return bool(self._yaml.get("enphase", {}).get("use_cloud", True))

    @property
    def enphase_config(self) -> dict:
        return {
            "client_id": self.enphase_client_id,
            "client_secret": self.enphase_client_secret,
            "system_id": self.enphase_system_id,
            "envoy_host": self.enphase_envoy_host,
            "use_cloud": self.enphase_use_cloud,
            "bearer_token": self.enphase_bearer_token,
        }

    # -------------------------------------------------------------------------
    # Solar
    # -------------------------------------------------------------------------

    @property
    def solar_source(self) -> str:
        return self._yaml.get("solar", {}).get("source", "auto")

    @property
    def rate_escalation(self) -> float:
        return float(self._yaml.get("solar", {}).get("rate_escalation", 3.0))

    @property
    def panel_lifespan_years(self) -> int:
        return int(self._yaml.get("solar", {}).get("panel_lifespan_years", 25))

    # -------------------------------------------------------------------------
    # Bills
    # -------------------------------------------------------------------------

    @property
    def bills_directory(self) -> str:
        return self._yaml.get("bills", {}).get("directory", "./bills")

    # -------------------------------------------------------------------------
    # Home Assistant InfluxDB (interval sensor data for TOU analysis)
    # -------------------------------------------------------------------------

    @property
    def ha_influx_host(self) -> str:
        return self._yaml.get("ha_influxdb", {}).get("host", self.influx_host)

    @property
    def ha_influx_port(self) -> int:
        return int(self._yaml.get("ha_influxdb", {}).get("port", self.influx_port))

    @property
    def ha_influx_database(self) -> str:
        return self._yaml.get("ha_influxdb", {}).get("database", "homeassistant")

    @property
    def ha_influx_username(self) -> str:
        return self._yaml.get("ha_influxdb", {}).get("username", "")

    @property
    def ha_influx_password(self) -> str:
        return self._get_secret("ha_influxdb_password")

    @property
    def ha_consumption_entity(self) -> str:
        return self._yaml.get("ha_influxdb", {}).get("consumption_entity", "")

    @property
    def ha_consumption_field(self) -> str:
        return self._yaml.get("ha_influxdb", {}).get("consumption_field", "value")

    def influx_client_ha(self):
        """Create InfluxDBClient for Home Assistant database."""
        return InfluxDBClient(
            self.ha_influx_host, self.ha_influx_port,
            self.ha_influx_username, self.ha_influx_password,
            self.ha_influx_database,
        )

    # -------------------------------------------------------------------------
    # TOU Rates (South Dakota Residential Time-of-Day)
    # -------------------------------------------------------------------------

    @property
    def tou_config(self) -> dict:
        return self._yaml.get("tou_rates", {})

    # -------------------------------------------------------------------------
    # Sync (energy interval pipeline)
    # -------------------------------------------------------------------------

    @property
    def sync_target_measurement(self) -> str:
        return self._yaml.get("sync", {}).get("target_measurement", "energy_interval")

    @property
    def sync_container_host(self) -> str:
        return self._yaml.get("sync", {}).get("container_host", "containers")

    @property
    def sync_backup_dir(self) -> str:
        return self._yaml.get("sync", {}).get("backup_dir", "/mnt")

    @property
    def sync_entities(self) -> list[str]:
        return self._yaml.get("sync", {}).get("entities", [])

    @property
    def sync_batch_size(self) -> int:
        return int(self._yaml.get("sync", {}).get("batch_size", 5000))

    @property
    def sync_entity_pairs(self) -> dict[str, dict[str, str]]:
        """
        Get entity pairs for net energy computation.
        
        Returns:
            Dict mapping target entity to {consumption: str, production: str}
        """
        return self._yaml.get("sync", {}).get("entity_pairs", {})


def setup_keyring_interactive() -> None:
    """Interactively prompt the user to store secrets in the system keyring."""
    print("Interactive keyring setup for xcel-solar")
    print("Leave password empty to skip a field.\n")

    keyring_labels = {
        "bills_password": "InfluxDB password (bills db, read-write)",
        "readonly_password": "InfluxDB password (readonly solar db)",
        "enphase_client_id": "Enphase client_id",
        "enphase_client_secret": "Enphase client_secret",
        "enphase_bearer_token": "Enphase bearer_token",
    }

    for key, label in keyring_labels.items():
        current = keyring.get_password(SERVICE_NAME, key) or ""
        prompt = f"  {key} ({label}) [current: {'set' if current else 'not set'}]: "
        val = input(prompt).strip()
        if not val and current:
            print(f"    Kept existing value (hidden)")
        elif val:
            keyring.set_password(SERVICE_NAME, key, val)
            print(f"    Saved.")
        else:
            print(f"    Skipped (no value)")

    print("\nKeyring setup complete.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--setup-keyring":
        setup_keyring_interactive()
    else:
        print("Usage: python config.py --setup-keyring")
