# src/env.py

import os
from dotenv import load_dotenv

def load_env():
    """
    Load environment variables from a .env file if it exists.
    Should be called at the very start of the program before importing config.
    """
    load_dotenv()

def get_api_key(service: str, cfg) -> str:
    """
    Retrieves the API key for the given service using environment variables.
    Requires the configuration object to be passed in.
    """
    env_var = cfg.get("api", service, "api_key_env")
    if env_var:
        api_key = os.getenv(env_var)
        if not api_key:
            raise EnvironmentError(f"Environment variable '{env_var}' for {service} API key is not set.")
        return api_key
    else:
        raise KeyError(f"No API key environment variable defined in config for service: {service}")

