# src/analyse.py

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Tuple, Optional

from src.logger import logger


def analyze_correlation(price_df: pd.DataFrame, whale_df: pd.DataFrame, cfg) -> Tuple[pd.DataFrame, Optional[float]]:
    """
    Merge token price and whale volume data on date,
    compute correlation between price and whale volume,
    and save both merged dataset and correlation score.

    Args:
        price_df (pd.DataFrame): Processed token price data.
        whale_df (pd.DataFrame): Processed whale transaction data.
        cfg (Config): Configuration object.

    Returns:
        Tuple[pd.DataFrame, Optional[float]]: Merged DataFrame and correlation value.
    """
    logger.info("Starting correlation analysis...")

    try:
        method = cfg.get("analysis", "correlation_method", default="pearson").lower()

        if method not in {"pearson", "spearman", "kendall"}:
            raise ValueError(f"Unsupported correlation method: {method}")

        results_path = cfg.get_path("paths", "results")
        results_path.mkdir(parents=True, exist_ok=True)

        # Merge price and whale data on 'date'
        merged_df = pd.merge(price_df, whale_df, on="date", how="inner")
        logger.info(f"Merged DataFrame shape: {merged_df.shape}")

        merged_df = merged_df.dropna(subset=["price", "usd_value"])
        if merged_df.empty:
            raise ValueError("Merged data is empty after dropping missing values.")

        correlation = merged_df["price"].corr(merged_df["usd_value"], method=method)
        logger.info(f"{method.title()} correlation: {correlation:.4f}")

        # Save results
        merged_df.to_csv(results_path / "merged_price_whale.csv", index=False)
        with open(results_path / "correlation.txt", "w") as f:
            f.write(f"{method.title()} correlation between price and whale volume: {correlation:.4f}\n")

        return merged_df, correlation

    except Exception as e:
        logger.error(f"Correlation analysis failed: {e}")
        return pd.DataFrame(), None


def plot_price_vs_whale(price_df: pd.DataFrame, whale_df: pd.DataFrame, cfg) -> None:
    """
    Plot token price and whale transaction volume over time and save the figure.

    Args:
        price_df (pd.DataFrame): Token price data.
        whale_df (pd.DataFrame): Whale transaction volume data.
        cfg (Config): Configuration object.
    """
    logger.info("Plotting token price vs whale activity...")

    try:
        figures_path = cfg.get_path("paths", "figures")
        figures_path.mkdir(parents=True, exist_ok=True)

        # Get plot settings from config
        figsize = tuple(cfg.get("plot", "figsize", default=[12, 6]))
        price_color = cfg.get("plot", "price_color", default="blue")
        whale_color = cfg.get("plot", "whale_color", default="orange")

        # Plotting
        plt.figure(figsize=figsize)
        plt.plot(price_df["date"], price_df["price"], label="Token Price", color=price_color)
        plt.bar(whale_df["date"], whale_df["usd_value"], label="Whale Volume", color=whale_color, alpha=0.5)

        plt.title("Token Price vs Whale Activity")
        plt.xlabel("Date")
        plt.ylabel("USD Value")
        plt.legend()
        plt.tight_layout()

        plot_file = figures_path / "price_vs_whale.png"
        plt.savefig(plot_file)
        plt.close()

        logger.info(f"Plot saved to: {plot_file}")

    except Exception as e:
        logger.error(f"Plotting failed: {e}")
