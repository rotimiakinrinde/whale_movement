# src/process_data.py

import pandas as pd
from pathlib import Path
from typing import Tuple
from src.logger import logger


def preprocess_price_data(raw_price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and formats token price data.

    Args:
        raw_price_df (pd.DataFrame): Raw token price DataFrame with 'date' and 'price'.

    Returns:
        pd.DataFrame: Cleaned price DataFrame with columns ['date', 'price'].
    """
    logger.info("Preprocessing token price data...")

    try:
        raw_price_df["date"] = pd.to_datetime(raw_price_df["date"])
        clean_price_df = raw_price_df[["date", "price"]].dropna()

        logger.info(f"Processed price data shape: {clean_price_df.shape}")
        return clean_price_df

    except Exception as e:
        logger.error(f"Error during price data preprocessing: {e}")
        return pd.DataFrame()


def preprocess_whale_data(raw_whale_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and formats whale transaction data.

    Args:
        raw_whale_df (pd.DataFrame): Raw whale DataFrame with 'timestamp' and 'usd_value'.

    Returns:
        pd.DataFrame: Cleaned whale DataFrame with daily totals.
    """
    logger.info("Preprocessing whale transaction data...")

    try:
        raw_whale_df["date"] = pd.to_datetime(raw_whale_df["timestamp"]).dt.strftime("%Y-%m-%d")
        raw_whale_df["date"] = pd.to_datetime(raw_whale_df["date"])
        clean_whale_df = raw_whale_df[["date", "usd_value"]].dropna()

        logger.info(f"Processed whale data shape: {clean_whale_df.shape}")
        return clean_whale_df

    except Exception as e:
        logger.error(f"Error during whale data preprocessing: {e}")
        return pd.DataFrame()


def preprocess_data(raw_whale_df: pd.DataFrame, raw_price_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Preprocess and align price and whale data by common dates.

    Args:
        raw_whale_df (pd.DataFrame): Raw whale transaction data.
        raw_price_df (pd.DataFrame): Raw token price data.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Aligned whale and price DataFrames.
    """
    logger.info("Aligning whale and price data by date...")

    whale_df = preprocess_whale_data(raw_whale_df)
    price_df = preprocess_price_data(raw_price_df)

    if whale_df.empty or price_df.empty:
        logger.warning("One or both datasets are empty after preprocessing.")
        return pd.DataFrame(), pd.DataFrame()

    # Align both DataFrames on shared 'date' values
    shared_dates = whale_df["date"].isin(price_df["date"])
    whale_df = whale_df[shared_dates]

    shared_dates = price_df["date"].isin(whale_df["date"])
    price_df = price_df[shared_dates]

    logger.info(f"Aligned data → Whale: {whale_df.shape}, Price: {price_df.shape}")
    return whale_df.reset_index(drop=True), price_df.reset_index(drop=True)


def save_processed_data(price_df: pd.DataFrame, whale_df: pd.DataFrame, cfg) -> None:
    """
    Save processed whale and price data to configured directory.

    Args:
        price_df (pd.DataFrame): Processed price data.
        whale_df (pd.DataFrame): Processed whale data.
        cfg (Config): Config object for path resolution.
    """
    try:
        logger.info("Saving processed data...")

        processed_path: Path = cfg.get_path("paths", "processed_data")
        processed_path.mkdir(parents=True, exist_ok=True)

        price_file = processed_path / "price_data.csv"
        whale_file = processed_path / "whale_data.csv"

        price_df.to_csv(price_file, index=False)
        whale_df.to_csv(whale_file, index=False)

        logger.info(f"Processed price data saved to: {price_file}")
        logger.info(f"Processed whale data saved to: {whale_file}")

    except Exception as e:
        logger.error(f"Error saving processed data: {e}")
