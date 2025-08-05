import pandas as pd
from pathlib import Path
from src.config import Config
from src.logger import logger

cfg = Config()

def preprocess_price_data(raw_price_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preprocessing price data...")
    raw_price_df["date"] = pd.to_datetime(raw_price_df["date"])
    clean_price_df = raw_price_df[["date", "price"]].dropna()
    logger.info(f"Processed price data shape: {clean_price_df.shape}")
    return clean_price_df

def preprocess_whale_data(raw_whale_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preprocessing whale transaction data...")
    raw_whale_df["date"] = pd.to_datetime(raw_whale_df["timestamp"]).dt.date
    raw_whale_df["date"] = pd.to_datetime(raw_whale_df["date"])
    clean_whale_df = raw_whale_df[["date", "amount_usd"]].dropna()
    logger.info(f"Processed whale data shape: {clean_whale_df.shape}")
    return clean_whale_df

def preprocess_data(raw_whale_df: pd.DataFrame, raw_price_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Unified preprocessing function to align both whale and price datasets by date.
    """
    whale_df = preprocess_whale_data(raw_whale_df)
    price_df = preprocess_price_data(raw_price_df)

    # Optional: align both datasets by date (inner join)
    common_dates = whale_df['date'].isin(price_df['date'])
    whale_df = whale_df[common_dates]

    common_dates = price_df['date'].isin(whale_df['date'])
    price_df = price_df[common_dates]

    logger.info(f"Aligned whale and price data to common dates. Whale: {whale_df.shape}, Price: {price_df.shape}")
    return whale_df, price_df

def save_processed_data(price_df: pd.DataFrame, whale_df: pd.DataFrame) -> None:
    processed_path = cfg.get_path("paths", "processed_data")
    processed_path.mkdir(parents=True, exist_ok=True)

    price_file = processed_path / "price_data.csv"
    whale_file = processed_path / "whale_data.csv"

    price_df.to_csv(price_file, index=False)
    whale_df.to_csv(whale_file, index=False)

    logger.info(f"Processed data saved to {processed_path}")

