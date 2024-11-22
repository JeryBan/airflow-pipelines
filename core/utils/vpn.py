"""
Utility functions to manipulate vpn connections.
"""
import logging
import subprocess
from pathlib import Path

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

SUPPORTED_VPN_TYPES = [
    "openvpn"
]


class VPNHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn = self._validate_connection(conn_id)
        self.pid = None

    def _validate_connection(self, conn_id: str):
        conn = self.get_connection(conn_id)

        if not conn.conn_type == "generic":
            raise TypeError(f"Connection type {conn.conn_type} is not supported. Please use the Generic template.")

        if conn.host.lower() not in SUPPORTED_VPN_TYPES:
            raise ValueError(
                f"Vpn provider '{conn.host}' is not supported. Please use the host field to set the vpn provider to one of these: ({', '.join(SUPPORTED_VPN_TYPES)})."
            )

        try:
            config_path = conn.extra_dejson["config_path"]
            Path(config_path).is_file()
        except KeyError:
            raise ValueError("Config filepath is not set. Please set 'config_path' in the extras section.")
        except FileNotFoundError:
            raise ValueError("Config file does not exist. Please set 'config_path' in the extras section.")

        return conn

    def start_vpn(self):
        pid = None
        if self.conn.host == 'openvpn':
            pid = self._start_openvpn()

        if pid:
            self.pid = pid
            logger.info(f"{self.conn.host} VPN started with {self.conn.extra_dejson['config_path']}. PID: {self.pid}")

    def _start_openvpn(self):
        try:
            process = subprocess.Popen(["openvpn", "--config", self.conn.extra_dejson['config_path']])
            return process.pid
        except Exception:
            raise RuntimeError('Failed to start OpenVPN process')

    def stop_vpn(self):
        if self.pid:
            subprocess.run(["kill", str(self.pid)], check=True)
            logger.info(f"VPN stopped with PID {self.pid}")
            self.pid = None
        else:
            print("VPN process not started or already stopped.")
