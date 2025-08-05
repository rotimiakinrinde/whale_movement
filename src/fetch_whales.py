# src/fetch_whales.py

import requests
import time
import pandas as pd
from pathlib import Path

from src.config import Config
from src.env import get_api_key
from src.logger import logger

cfg = Config()


def execute_dune_query():
    logger.info("Submitting Dune query for whale transactions...")

    base_url = cfg.get("api", "dune", "base_url")
    query_id = cfg.get("api", "dune", "query_id")
    api_key = get_api_key("dune", cfg)  # ✅ Fixed: passing cfg

    url = f"{base_url}/query/{query_id}/execute"
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.post(url, headers=headers)
    response.raise_for_status()

    execution_id = response.json().get("execution_id")
    logger.info(f"Dune query submitted. Execution ID: {execution_id}")
    return execution_id


def poll_dune_execution(execution_id, interval=5, timeout=300):
    logger.info("Polling Dune API for query results...")

    base_url = cfg.get("api", "dune", "base_url")
    api_key = get_api_key("dune", cfg)  # ✅ Fixed: passing cfg
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    url = f"{base_url}/execution/{execution_id}/results"
    start_time = time.time()

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info("Dune query execution complete.")
            return response.json()

        if time.time() - start_time > timeout:
            raise TimeoutError("Polling Dune query timed out.")

        logger.info("Query still running... waiting before retrying.")
        time.sleep(interval)


def fetch_whale_transactions():
    execution_id = execute_dune_query()
    results_json = poll_dune_execution(execution_id)

    rows = results_json.get("result", {}).get("rows", [])
    if not rows:
        logger.warning("No data returned from Dune query.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Filter whale transactions by USD amount threshold
    threshold = cfg.get("analysis", "whale_threshold_usd", default=100000)
    df = df[df["amount_usd"] >= threshold]

    # Save result
    processed_path = cfg.get_path("paths", "processed_data")
    processed_path.mkdir(parents=True, exist_ok=True)

    output_file = processed_path / "whale_transactions.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Filtered whale transactions saved to {output_file}")

    return df
