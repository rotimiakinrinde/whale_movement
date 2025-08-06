# src/fetch_price.py

import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from src.env import get_api_key
from src.logger import logger

def fetch_token_price_history(cfg) -> pd.DataFrame:
    logger.info("Fetching token price history from CoinGecko...")

    try:
        # Load configuration
        token_name = cfg.get("token", "name")
        vs_currency = cfg.get("token", "vs_currency")
        start_date = cfg.get("analysis", "start_date")
        end_date = cfg.get("analysis", "end_date")
        base_url = cfg.get("api", "coingecko", "base_url")
        api_key = get_api_key("coingecko", cfg)

        if not all([token_name, vs_currency, start_date, end_date]):
            raise ValueError("Missing required token or date configuration values.")

        # Build headers
        headers = {"accept": "application/json"}
        if api_key:
            headers["x-cg-pro-api-key"] = api_key

        # Ping CoinGecko API to test key & base_url
        try:
            ping_url = f"{base_url}/ping"
            ping_response = requests.get(ping_url, headers=headers)
            ping_response.raise_for_status()
            logger.debug(f"Ping success: {ping_response.json()}")
        except Exception as e:
            logger.error(f"CoinGecko API key may be invalid or base_url is incorrect: {e}")
            return pd.DataFrame()

        # Convert date strings to UTC datetime
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Fetch prices in 90-day chunks
        all_prices = []
        current_start = start_dt

        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=90), end_dt)
            from_ts = int(current_start.timestamp())
            to_ts = int(current_end.timestamp())

            url = f"{base_url}/coins/{token_name}/market_chart/range"
            params = {
                "vs_currency": vs_currency,
                "from": from_ts,
                "to": to_ts
            }

            logger.info(f"Fetching prices from {current_start.date()} to {current_end.date()}...")
            debug_url = requests.Request("GET", url, params=params).prepare().url
            logger.debug(f"Requesting CoinGecko API URL: {debug_url}")

            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                prices = data.get("prices", [])
            except requests.exceptions.HTTPError as err:
                logger.warning(f"Range API failed: {err}. Trying fallback endpoint with 'days' param.")
                fallback_url = f"{base_url}/coins/{token_name}/market_chart"
                fallback_params = {"vs_currency": vs_currency, "days": "90"}
                fallback_debug_url = requests.Request("GET", fallback_url, params=fallback_params).prepare().url
                logger.debug(f"Requesting fallback CoinGecko API URL: {fallback_debug_url}")
                response = requests.get(fallback_url, params=fallback_params, headers=headers)
                response.raise_for_status()
                data = response.json()
                prices = data.get("prices", [])

            all_prices.extend(prices)
            current_start = current_end

        if not all_prices:
            logger.warning("No price data found in CoinGecko response.")
            return pd.DataFrame()

        # Process into DataFrame
        price_df = pd.DataFrame(all_prices, columns=["timestamp", "price"])
        price_df["date"] = pd.to_datetime(price_df["timestamp"], unit="ms").dt.strftime("%Y-%m-%d")
        price_df = price_df.groupby("date", as_index=False).mean()[["date", "price"]]

        # Save to CSV
        raw_data_path = cfg.get_path("paths", "raw_data")
        raw_data_path.mkdir(parents=True, exist_ok=True)
        output_file = raw_data_path / f"{token_name}_price_data.csv"
        price_df.to_csv(output_file, index=False)

        logger.info(f"Token price data saved to: {output_file}")
        return price_df

    except Exception as e:
        logger.error(f"Error fetching token prices: {e}")
        return pd.DataFrame()
