# src/config.py
import yaml
from pathlib import Path
from datetime import datetime
import os

class Config:
    def __init__(self, config_path="config.yaml"):
        with open(config_path, "r") as f:
            self.cfg = yaml.safe_load(f)

    def get(self, *keys, default=None):
        """Access nested config values like cfg.get('api', 'dune', 'query_id')"""
        val = self.cfg
        for key in keys:
            if key in val:
                val = val[key]
            else:
                return default
        return val

    def get_path(self, *keys):
        """Return a Path object"""
        path_str = self.get(*keys)
        return Path(path_str) if path_str else None

    def get_date(self, *keys, default=None):
        """Parse and return a datetime object from a config date string"""
        date_str = self.get(*keys)
        if date_str:
            return datetime.strptime(date_str, "%Y-%m-%d")
        return default

    def get_env(self, *keys, fallback=None):
        """Load env variable by key name specified in config"""
        env_var = self.get(*keys)
        return os.getenv(env_var, fallback)

