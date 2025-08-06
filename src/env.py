# src/env.py

import os
from dotenv import load_dotenv


def load_env():
    """
    Load environment variables from a .env file into the OS environment.
    This should be called before any config or API interaction.
    """
    load_dotenv()


def get_api_key(service: str, cfg) -> str:
    """
    Retrieve the API key for a given service using environment variables.

    Args:
        service (str): The name of the API service ("helius", "coingecko", etc.).
        cfg (Config): Instance of the Config class to resolve environment variable name.

    Returns:
        str: The actual API key retrieved from environment.

    Raises:
        KeyError: If no API key environment variable name is defined in config.
        EnvironmentError: If the environment variable is not set.
    """
    try:
        env_var = cfg.get("api", service, "api_key_env")
        if not env_var:
            raise KeyError
    except Exception:
        raise KeyError(f"No API key environment variable defined in config for service: '{service}'")

    api_key = os.getenv(env_var)
    if not api_key:
        raise EnvironmentError(
            f"Environment variable '{env_var}' for '{service}' API key is not set. "
            f"Please define it in your .env file or system environment."
        )

    return api_key
