"""
Extracts energy interval data from HA backup tarballs on the container host.

Uses influx_inspect export (read-only Docker) instead of a running InfluxDB server.
Selective tar extraction extracts homeassistant DB only, leaving data on NFS.
"""

import argparse
import hashlib
import logging
import sys
import time
from datetime import datetime, timezone

from config import AppConfig
from influx_batch_writer import InfluxBatchWriter
from line_protocol_parser import LineProtocolParser
from ssh_runner import SshRunner

try:
    import securetar
except ImportError:
    securetar = None

logger = logging.getLogger(__name__)

INFLUX_ADDON = "a0d7b954_influxdb.tar.gz"
ENCRYPTED_BACKUPS = {"3e9f1886", "b5f7580d", "28Apr2025-Full"}

# Entity pairs are now configured in config.yaml under sync.entity_pairs


def derive_ha_backup_key(password: str) -> bytes:
    """Derive 16-byte AES key from HA backup password using SHA256x100."""
    key = password.encode()
    for _ in range(100):
        key = hashlib.sha256(key).digest()
    return key[:16]


def aggregate_hourly(points: list[dict]) -> list[dict]:
    """
    Aggregate points to hourly by taking LAST value per hour.
    
    Args:
        points: list of {"time": "ISO8601Z", "value": float}
        
    Returns:
        list of {"time": "YYYY-MM-DDTHH:00:00Z", "value": float} with one per hour
    """
    if not points:
        return []
    
    # Group by hour, keep LAST value per hour
    hour_groups: dict[str, list[dict]] = {}
    
    for point in points:
        # Extract hour from timestamp
        hour_key = point["time"][:13] + ":00:00Z"
        if hour_key not in hour_groups:
            hour_groups[hour_key] = []
        hour_groups[hour_key].append(point)
    
    # Take the chronologically last point in each hour
    hourly_points = []
    for hour_key, hour_points in hour_groups.items():
        # Sort by timestamp and take the last
        sorted_points = sorted(hour_points, key=lambda p: p["time"])
        last_point = sorted_points[-1]
        hourly_points.append({"time": hour_key, "value": last_point["value"]})
    
    return sorted(hourly_points, key=lambda p: p["time"])


class BackupLister:
    """Lists and filters HA backup tarballs on the container host."""

    def __init__(self, ssh: SshRunner, backup_dir: str) -> None:
        self._ssh = ssh
        self._backup_dir = backup_dir

    def list_all(self) -> list:
        """Return sorted list of .tar filenames (with full path) in backup_dir."""
        output = self._ssh.run(f"ls {self._backup_dir}/*.tar 2>/dev/null")
        names = []
        for line in output.strip().split("\n"):
            if line:
                names.append(line.strip())
        return sorted(names)

    def has_influxdb(self, tar_path: str) -> bool:
        """Check if a tarball contains the InfluxDB addon archive."""
        return self._ssh.run_ok(
            f"tar -tf {tar_path} | grep -q a0d7b954_influxdb.tar.gz"
        )

    def list_viable(self) -> list:
        """Return list of tarballs with InfluxDB data that are not encrypted."""
        all_tars = self.list_all()
        viable = []
        for tar_path in all_tars:
            tar_name = tar_path.split("/")[-1]
            if any(enc in tar_name for enc in ENCRYPTED_BACKUPS):
                logger.debug("Skipping encrypted: %s", tar_name)
                continue
            if self.has_influxdb(tar_path):
                viable.append(tar_path)
            else:
                logger.debug("Skipping (no InfluxDB addon): %s", tar_name)
        return viable


class NfsExtractor:
    """
    Extracts homeassistant data from HA backup tarballs to local /var/tmp.

    Runs influx_inspect export via Docker with :ro mount of local dirs.
    No NFS extraction needed -- export runs against local /var/tmp copy.
    """

    def __init__(self, ssh: SshRunner, backup_dir: str) -> None:
        self._ssh = ssh
        self._backup_dir = backup_dir

    def extract(self, tar_name: str, decrypt_key: str = None) -> str:
        """
        Extract homeassistant data/wal from tarball to local /var/tmp.

        Uses shell tar pipeline (avoids Python tarfile module silent drops).
        For encrypted backups, uses securetar to decrypt the inner InfluxDB addon.
        Returns /var/tmp path for Docker :ro mount (local dir, not NFS).
        
        Args:
            tar_name: backup tarball filename
            decrypt_key: HA backup password for encrypted backups (optional)
        """
        basename = tar_name.replace(".tar", "")
        local_dir = f"/var/tmp/tou_extract_{basename}"

        # Already extracted?
        if self._ssh.run_ok(f"test -d {local_dir}/data/influxdb/data/homeassistant/autogen"):
            logger.info("Already extracted, skipping: %s", basename)
            return local_dir

        self._ssh.run(f"mkdir -p {local_dir}", check=True, timeout=30)

        # Step 1: Extract inner tarball to local /var/tmp (pipeline avoids NFS chown issues)
        logger.info("Extracting inner tarball to /var/tmp...")
        inner_tar = f"{local_dir}/a0d7b954_influxdb.tar.gz"

        # Try both inner tar paths
        for inner_path in ("./a0d7b954_influxdb.tar.gz", "a0d7b954_influxdb.tar.gz"):
            cmd = f"tar -xOf {self._backup_dir}/{tar_name} {inner_path} > {inner_tar}"
            result = self._ssh.run(cmd, check=False, timeout=1200)
            if result.strip() == "" and self._ssh.run_ok(f"test -s {inner_tar}"):
                logger.info("Inner tarball extracted (%s MB)",
                            self._ssh.run(f"du -sh {inner_tar}").strip().split()[0][:-1])
                break

        if not self._ssh.run_ok(f"test -s {inner_tar}"):
            raise RuntimeError(f"Failed to extract inner tarball from {tar_name}")

        # Handle decryption if needed
        if decrypt_key:
            if securetar is None:
                raise RuntimeError("securetar package not available for encrypted backup decryption")
            
            logger.info("Decrypting inner tarball...")
            decrypted_tar = f"{local_dir}/a0d7b954_influxdb_decrypted.tar.gz"
            
            # Copy to local machine for decryption (securetar runs locally)
            temp_encrypted = f"/tmp/encrypted_{basename}.tar.gz" 
            self._ssh.run(f"scp {inner_tar} root@localhost:{temp_encrypted}", check=True, timeout=600)
            
            # Decrypt locally using securetar
            key = derive_ha_backup_key(decrypt_key)
            with securetar.SecureTarFile(temp_encrypted, "r", key=key) as tar:
                tar.extractall(f"/tmp/decrypted_{basename}")
            
            # Copy back to remote host
            self._ssh.run(f"scp -r /tmp/decrypted_{basename}/* {local_dir}/", check=True, timeout=600)
            
            # Cleanup local temp files
            self._ssh.run(f"rm -rf {temp_encrypted} /tmp/decrypted_{basename}", check=False, timeout=30)
        else:
            # Step 2: Extract only homeassistant data + wal dirs (streaming tar, no Python)
            logger.info("Extracting homeassistant data/wal locally...")
            for subdir, pattern in [
                ("data", "data/influxdb/data/homeassistant/*"),
                ("wal", "data/influxdb/wal/homeassistant/*"),
            ]:
                cmd = (
                    f"tar -xzf {inner_tar} -C {local_dir}/ "
                    f"--wildcards '{pattern}' 2>&1 | tail -1; echo OK_{subdir}"
                )
                result = self._ssh.run(cmd, check=False, timeout=600)
                logger.debug("Extract %s: %s", subdir, result[:80])

        # Verify
        if not self._ssh.run_ok(f"test -d {local_dir}/data/influxdb/data/homeassistant/autogen"):
            raise RuntimeError(f"Local extraction failed for {tar_name}")

        # Remove inner tarball to free space (~4GB)
        self._ssh.run(f"rm -f {inner_tar}", check=False, timeout=30)

        logger.info("Local extraction complete: %s", local_dir)
        return local_dir

    def cleanup(self, extract_path: str) -> None:
        """Remove local extraction directory."""
        self._ssh.run(f"rm -rf {extract_path}", check=False, timeout=120)


def compute_net_energy(
    consumption_pts: list[dict], production_pts: list[dict]
) -> list[dict]:
    """
    Compute net = consumption - production using 1-second bucket join.

    +/-2 second tolerance for timestamp alignment.
    Returns [{"time": "ISO8601Z", "value": float}, ...].
    """
    if not consumption_pts or not production_pts:
        return []

    prod_buckets: dict[int, list[tuple]] = {}
    for prod_ts, prod_val in production_pts:
        prod_dt = datetime.fromisoformat(prod_ts.replace("Z", "+00:00"))
        bucket = int(prod_dt.timestamp())
        if bucket not in prod_buckets:
            prod_buckets[bucket] = []
        prod_buckets[bucket].append((prod_ts, prod_val))

    points = []
    for cons_ts, cons_val in consumption_pts:
        cons_dt = datetime.fromisoformat(cons_ts.replace("Z", "+00:00"))
        cons_sec = int(cons_dt.timestamp())
        best_net: float | None = None
        best_ts: str | None = None
        best_delta: float | None = None

        for sec in range(cons_sec - 2, cons_sec + 3):
            bucket = prod_buckets.get(sec)
            if not bucket:
                continue
            for prod_ts, prod_val in bucket:
                prod_dt = datetime.fromisoformat(prod_ts.replace("Z", "+00:00"))
                delta = abs((cons_dt - prod_dt).total_seconds())
                if delta <= 2.0:
                    net = cons_val - prod_val
                    if net >= 0:
                        if best_delta is None or delta < best_delta:
                            best_delta = delta
                            best_net = net
                            best_ts = prod_ts
        if best_net is not None and best_ts is not None:
            points.append({"time": cons_ts, "value": best_net})

    return points


class InspectExporter:
    """
    Exports entity data from backup TSM/WAL dirs via influx_inspect.

    Uses read-only Docker container with local /var/tmp mounts.
    Pipes through grep on remote to filter entities before SSH transfer.
    """

    def __init__(self, ssh: SshRunner) -> None:
        self._ssh = ssh
        self._parser = LineProtocolParser()

    def export_entities(
        self, influx_dir: str, entities: list[str]
    ) -> dict[str, list[dict]]:
        """
        Run influx_inspect export via Docker with :ro mounts.

        influx_dir is /var/tmp path (local, not NFS).
        Writes to container /tmp then cats to avoid SSH streaming loss.
        Returns {entity_id: [{"time": "ISO8601Z", "value": float}, ...]}.
        """
        grep_pattern = "| grep -E '" + "|".join(f"entity_id={e}" for e in entities) + "'"

        # Write to /tmp then cat (avoids SSH streaming truncation)
        docker_cmd = (
            f"docker run --rm "
            f"-v {influx_dir}/data/influxdb/data:/data:ro "
            f"-v {influx_dir}/data/influxdb/wal:/wal:ro "
            f"influxdb:1.8 sh -c "
            f"'influx_inspect export -datadir /data -waldir /wal "
            f"-database homeassistant -lponly -out - > /tmp/export.lp {grep_pattern} && "
            f"cat /tmp/export.lp'"
        )
        logger.info("Exporting and filtering (may take several minutes)...")
        raw = self._ssh.run(docker_cmd, check=True, timeout=7200)

        # Parse, grouped by entity
        by_entity: dict[str, list[dict]] = {e: [] for e in entities}
        for line in raw.split("\n"):
            if not line.strip():
                continue
            parsed = self._parser.parse_line(line)
            if parsed and parsed["entity_id"] in entities:
                by_entity[parsed["entity_id"]].append({
                    "time": parsed["time"],
                    "value": parsed["value"],
                })

        return by_entity


# -------------------------------------------------------------------------
# CLI entry point
# -------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract energy interval data from HA backup tarballs."
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--backup", metavar="NAME",
        help="Process a specific backup tarball (e.g. 2fcb7cf5.tar).",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Process all viable backup tarballs.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List viable backups without extracting.",
    )
    parser.add_argument(
        "--keep-extracted", action="store_true",
        help="Do not clean up extracted directories after processing.",
    )
    parser.add_argument(
        "--decrypt-key", metavar="PASSWORD",
        help="HA backup password for encrypted backups.",
    )
    parser.add_argument(
        "--hourly", action="store_true",
        help="Aggregate data to hourly (LAST value per hour) before writing.",
    )
    return parser


def _expand_entities(entities: list[str], entity_pairs: dict[str, dict[str, str]]) -> list[str]:
    """Expand entity list to include all entities needed for net computation."""
    expanded = set(entities)
    for e in entities:
        if e in entity_pairs:
            pair = entity_pairs[e]
            expanded.add(pair.get("consumption", ""))
            expanded.add(pair.get("production", ""))
    return [e for e in expanded if e]  # Filter out empty strings


def process_backup(
    tar_path: str,
    extractor: NfsExtractor,
    exporter: InspectExporter,
    writer: InfluxBatchWriter,
    entities: list[str],
    keep_extracted: bool,
    decrypt_key: str = None,
    hourly: bool = False,
    entity_pairs: dict[str, dict[str, str]] = None,
) -> int:
    """
    Extract a backup, export entities, compute net if needed, write to solar DB.
    Returns total points written.
    """
    tar_name = tar_path.split("/")[-1]
    logger.info("Processing: %s", tar_name)

    influx_dir = extractor.extract(tar_name, decrypt_key)

    # Expand entities to include consumption/production pairs for net computation
    all_entities = _expand_entities(entities, entity_pairs or {})
    entity_points = exporter.export_entities(influx_dir, all_entities)

    total = 0
    for entity_id in entities:
        pts = entity_points.get(entity_id, [])

        # Compute net if entity is in entity_pairs config
        if entity_pairs and entity_id in entity_pairs:
            pair = entity_pairs[entity_id]
            cons_e = pair.get("consumption", "")
            prod_e = pair.get("production", "")
            cons_pts = entity_points.get(cons_e, [])
            prod_pts = entity_points.get(prod_e, [])
            if cons_pts and prod_pts:
                net_pts = compute_net_energy(
                    [(p["time"], p["value"]) for p in cons_pts],
                    [(p["time"], p["value"]) for p in prod_pts],
                )
                # Apply hourly aggregation if requested
                if hourly:
                    net_pts = aggregate_hourly(net_pts)
                count = writer.write_points(net_pts, entity_id, source="backup")
                logger.info("  %s: %d net points", entity_id, count)
                total += count
        elif pts:
            # Apply hourly aggregation if requested
            if hourly:
                pts = aggregate_hourly(pts)
            count = writer.write_points(pts, entity_id, source="backup")
            logger.info("  %s: %d points", entity_id, count)
            total += count

    if not keep_extracted:
        extractor.cleanup(influx_dir)

    return total


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = AppConfig.load(args.config)
    ssh = SshRunner(config.sync_container_host)

    lister = BackupLister(ssh, config.sync_backup_dir)
    extractor = NfsExtractor(ssh, config.sync_backup_dir)
    exporter = InspectExporter(ssh)
    writer = InfluxBatchWriter(config)

    if args.dry_run:
        viable = lister.list_viable()
        print(f"Viable backups ({len(viable)}):")
        for v in viable:
            print(f"  {v}")
        return

    if args.all:
        backups = lister.list_viable()
    elif args.backup:
        backups = [f"{config.sync_backup_dir}/{args.backup}"]
    else:
        print("Use --all or --backup. Run --dry-run first to see viable backups.")
        sys.exit(1)

    total_points = 0
    ok_count = 0
    fail_count = 0

    for tar_path in backups:
        try:
            count = process_backup(
                tar_path, extractor, exporter, writer,
                config.sync_entities, args.keep_extracted,
                args.decrypt_key, args.hourly,
                config.sync_entity_pairs,
            )
            print(f"  {tar_path.split('/')[-1]}: {count} points written")
            total_points += count
            ok_count += 1
        except Exception as e:
            print(f"  ERROR {tar_path.split('/')[-1]}: {e}")
            logger.error("Failed to process %s: %s", tar_path, e)
            fail_count += 1

    print(f"\nResults: {ok_count} OK, {fail_count} failed, {total_points} total points")


if __name__ == "__main__":
    main()