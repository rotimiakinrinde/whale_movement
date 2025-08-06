# src/plot.py

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional

from src.logger import logger


def plot_price_vs_whale(price_df: pd.DataFrame, whale_df: pd.DataFrame, cfg) -> Optional[Path]:
    """
    Plot token price and whale transaction volume over time and save the figure.

    Args:
        price_df (pd.DataFrame): Token price data with 'date' and 'price'.
        whale_df (pd.DataFrame): Whale transaction data with 'date' and 'usd_value'.
        cfg (Config): Configuration object.

    Returns:
        Optional[Path]: Path to saved plot file if successful, None if failed.
    """
    logger.info("Plotting token price vs whale activity...")

    try:
        figures_path: Path = cfg.get_path("paths", "figures")
        figures_path.mkdir(parents=True, exist_ok=True)

        # Get plot settings from config
        figsize = tuple(cfg.get("plot", "figsize", default=[12, 6]))
        price_color = cfg.get("plot", "price_color", default="blue")
        whale_color = cfg.get("plot", "whale_color", default="orange")

        # Initialize plot
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
        return plot_file

    except Exception as e:
        logger.error(f"Plotting failed: {e}")
        return None
