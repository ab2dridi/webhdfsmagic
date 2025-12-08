"""
Configuration management for WebHDFS Magic.

This module handles loading and validating configuration from:
1. ~/.webhdfsmagic/config.json (priority)
2. ~/.sparkmagic/config.json (fallback)
"""

import json
import os
import urllib.parse
from typing import Any


class ConfigurationManager:
    """Manages configuration loading and validation for WebHDFS Magic."""

    def __init__(self):
        """Initialize with default values."""
        self.knox_url = "https://localhost:8443/gateway/default"
        self.webhdfs_api = "/webhdfs/v1"
        self.auth_user = ""
        self.auth_password = ""
        self.verify_ssl = False

    def load(self) -> dict[str, Any]:
        """
        Load configuration from config files.

        Priority order:
        1. ~/.webhdfsmagic/config.json
        2. ~/.sparkmagic/config.json
        3. Default values

        Returns:
            Dictionary with configuration values
        """
        config_path_webhdfsmagic = os.path.expanduser("~/.webhdfsmagic/config.json")
        config_path_sparkmagic = os.path.expanduser("~/.sparkmagic/config.json")

        if os.path.exists(config_path_webhdfsmagic):
            return self._load_webhdfsmagic_config(config_path_webhdfsmagic)
        elif os.path.exists(config_path_sparkmagic):
            return self._load_sparkmagic_config(config_path_sparkmagic)
        else:
            print("No configuration file found. Using default settings.")
            return self._get_default_config()

    def _load_webhdfsmagic_config(self, path: str) -> dict[str, Any]:
        """Load configuration from .webhdfsmagic/config.json."""
        try:
            with open(path) as f:
                config = json.load(f)
            print(f"Loading configuration from {path}")

            self.knox_url = config.get("knox_url", self.knox_url)
            self.webhdfs_api = config.get("webhdfs_api", self.webhdfs_api)
            self.auth_user = config.get("username", self.auth_user)
            self.auth_password = config.get("password", self.auth_password)
            self.verify_ssl = config.get("verify_ssl", False)

            self._validate_verify_ssl()

            return self._get_current_config()
        except Exception as e:
            print(f"Warning loading configuration file {path}: {str(e)}")
            return self._get_default_config()

    def _load_sparkmagic_config(self, path: str) -> dict[str, Any]:
        """
        Load configuration from .sparkmagic/config.json.

        Transforms Livy URL to WebHDFS URL by removing the last path segment
        and appending '/webhdfs/v1'.
        """
        try:
            with open(path) as f:
                config = json.load(f)
            print(f"Loading configuration from {path}")

            creds = config.get("kernel_python_credentials", {})
            sparkmagic_url = creds.get("url", "")

            if sparkmagic_url:
                self.knox_url = self._transform_sparkmagic_url(sparkmagic_url)

            self.auth_user = creds.get("username", self.auth_user)
            self.auth_password = creds.get("password", self.auth_password)
            self.verify_ssl = config.get("verify_ssl", False)

            self._validate_verify_ssl()

            return self._get_current_config()
        except Exception as e:
            print(f"Warning loading configuration file {path}: {str(e)}")
            return self._get_default_config()

    def _transform_sparkmagic_url(self, url: str) -> str:
        """
        Transform Sparkmagic Livy URL to WebHDFS URL.

        Examples:
            https://host:port/gateway/default/livy/v1
            -> https://host:port/gateway/default/webhdfs/v1

            https://host:port/livy_for_spark3
            -> https://host:port/webhdfs/v1
        """
        parsed = urllib.parse.urlsplit(url)
        path_parts = parsed.path.rstrip("/").split("/")

        if len(path_parts) > 1 and path_parts[-1]:
            base_path = "/".join(path_parts[:-1])
        else:
            base_path = parsed.path

        base_url = f"{parsed.scheme}://{parsed.netloc}{base_path}"
        return base_url + "/webhdfs/v1"

    def _validate_verify_ssl(self):
        """
        Validate and process verify_ssl configuration.

        Handles:
        - Boolean values (True/False)
        - String paths to certificate files
        - Expands ~ in paths
        - Falls back to False if certificate file doesn't exist
        """
        if isinstance(self.verify_ssl, bool):
            return
        elif isinstance(self.verify_ssl, str):
            # Expand user home directory (~) in certificate path
            expanded_path = os.path.expanduser(self.verify_ssl)
            if os.path.exists(expanded_path):
                self.verify_ssl = expanded_path
            else:
                print(
                    f"Warning: certificate file '{self.verify_ssl}' "
                    "does not exist. Falling back to False."
                )
                self.verify_ssl = False
        else:
            print("Warning: verify_ssl has an unexpected type. Using default value False.")
            self.verify_ssl = False

    def _get_current_config(self) -> dict[str, Any]:
        """Get current configuration as dictionary."""
        return {
            "knox_url": self.knox_url,
            "webhdfs_api": self.webhdfs_api,
            "auth_user": self.auth_user,
            "auth_password": self.auth_password,
            "verify_ssl": self.verify_ssl,
        }

    def _get_default_config(self) -> dict[str, Any]:
        """Get default configuration."""
        return self._get_current_config()
