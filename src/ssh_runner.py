"""
Runs commands on a remote host via SSH.
"""

import logging
import subprocess

logger = logging.getLogger(__name__)


class SshRunner:
    """Runs commands on a remote host via SSH."""

    def __init__(self, host: str, user: str = "root") -> None:
        self._host = host
        self._user = user

    def run(self, command: str, check: bool = True, timeout: int = 120) -> str:
        """
        Execute command on remote host via ssh.

        Returns stdout as string. Raises subprocess.CalledProcessError on failure if check=True.
        """
        cmd = ["ssh", f"{self._user}@{self._host}", command]
        logger.debug("Running: %s", command)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )
        logger.debug("Exit code %d: %s", result.returncode, command[:60])
        return result.stdout

    def run_ok(self, command: str, timeout: int = 30) -> bool:
        """Run command, return True if exit code 0, False otherwise."""
        try:
            self.run(command, check=True, timeout=timeout)
            return True
        except subprocess.CalledProcessError:
            return False
