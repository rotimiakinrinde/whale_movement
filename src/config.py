# src/config.py

import yaml
from pathlib import Path
from datetime import datetime
import os


class Config:
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize Config instance and load YAML configuration.

        Args:
            config_path (str): Path to the configuration YAML file.
        """
        self.config_path = config_path
        self.config_data = self._load_config()

    def _load_config(self) -> dict:
        """
        Load and parse the YAML configuration file.

        Returns:
            dict: Parsed configuration data.

        Raises:
            FileNotFoundError: If the config file does not exist.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)

    def get(self, *keys, default=None):
        """
        Retrieve a nested value from the config.

        Args:
            *keys: Variable number of keys to access nested values.
            default: Default value to return if any key is missing.

        Returns:
            Any: The resolved value or default.
        """
        val = self.config_data
        for key in keys:
            if isinstance(val, dict) and key in val:
                val = val[key]
            else:
                return default
        return val

    def get_path(self, *keys) -> Path | None:
        """
        Retrieve a Path object from the config.

        Args:
            *keys: Keys pointing to a string path in config.

        Returns:
            Path | None: Path object or None if not found.
        """
        path_str = self.get(*keys)
        return Path(path_str) if path_str else None

    def get_date(self, *keys, default=None) -> datetime | None:
        """
        Retrieve a datetime object parsed from a YYYY-MM-DD string.

        Args:
            *keys: Keys pointing to the date string.
            default: Fallback datetime if the value is missing.

        Returns:
            datetime | None: Parsed date or fallback.
        """
        date_str = self.get(*keys)
        if date_str:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format for key path: {' > '.join(keys)}")
        return default

    def get_env(self, *keys, fallback=None) -> str | None:
        """
        Resolve an environment variable whose name is defined in the config.

        Args:
            *keys: Keys pointing to the env var name in config.
            fallback: Value to return if env var is not set.

        Returns:
            str | None: The resolved env var value or fallback.

        Raises:
            KeyError: If no env var name is defined in config.
        """
        env_var = self.get(*keys)
        if not env_var:
            raise KeyError(f"Missing environment variable name in config for key path: {' > '.join(keys)}")
        return os.getenv(env_var, fallback)
