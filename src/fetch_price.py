# src/fetch_price.py

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

from src.config import Config
from src.env import get_api_key
from src.logger import logger

cfg = Config()

def fetch_token_price_history():
    logger.info("Fetching token price history from CoinGecko...")

    token_name = cfg.get("token", "name")
    vs_currency = cfg.get("token", "vs_currency")
    interval = cfg.get("token", "price_interval")

    start_date = cfg.get("analysis", "start_date")
    end_date = cfg.get("analysis", "end_date")

    base_url = cfg.get("api", "coingecko", "base_url")
    api_key = get_api_key("coingecko")  # From env.py

    url = f"{base_url}/coins/{token_name}/market_chart/range"

    # Convert date strings to UNIX timestamps (in seconds)
    from_timestamp = int(datetime.fromisoformat(start_date).timestamp())
    to_timestamp = int(datetime.fromisoformat(end_date).timestamp())

    params = {
        "vs_currency": vs_currency,
        "from": from_timestamp,
        "to": to_timestamp
    }

    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": api_key  # Optional: Only if using a paid key
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()

    if "prices" not in data:
        raise ValueError("Unexpected response structure from CoinGecko API")

    # Transform price data
    price_df = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    price_df["date"] = pd.to_datetime(price_df["timestamp"], unit="ms").dt.date
    price_df = price_df[["date", "price"]]
    price_df = price_df.groupby("date").mean().reset_index()

    # Save to raw_data folder
    raw_data_path = cfg.get_path("paths", "raw_data")
    raw_data_path.mkdir(parents=True, exist_ok=True)

    output_file = raw_data_path / f"{token_name}_price_data.csv"
    price_df.to_csv(output_file, index=False)

    logger.info(f"Token price data saved to {output_file}")
    return price_df
