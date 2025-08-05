# src/analyse.py

import pandas as pd
import matplotlib.pyplot as plt
from src.config import Config
from src.logger import logger

# Load configuration
cfg = Config()

# Extract config values
figsize = cfg.get('plot', 'figsize', default=[12, 6])
price_color = cfg.get('plot', 'price_color', default='blue')
whale_color = cfg.get('plot', 'whale_color', default='orange')
figures_path = cfg.get_path('paths', 'figures')
results_path = cfg.get_path('paths', 'results')
correlation_method = cfg.get('analysis', 'correlation_method', default='pearson')

def analyze_correlation(price_df, whale_df):
    """
    Merge price and whale data, compute correlation between price and volume,
    and save results to file.
    """
    logger.info("Starting correlation analysis...")

    # Merge on date
    merged_df = pd.merge(price_df, whale_df, on='date', how='inner')
    logger.info(f"Merged data shape: {merged_df.shape}")

    # Drop missing or invalid values
    merged_df = merged_df.dropna(subset=['price', 'volume'])

    # Calculate correlation
    correlation = merged_df['price'].corr(merged_df['volume'], method=correlation_method)
    logger.info(f"Computed {correlation_method} correlation: {correlation:.4f}")

    # Save merged data and correlation result
    results_path.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(results_path / "merged_price_whale.csv", index=False)
    with open(results_path / "correlation.txt", "w") as f:
        f.write(f"Correlation between price and whale volume: {correlation:.4f}\n")

    return merged_df, correlation

def plot_price_vs_whale(price_df, whale_df):
    """
    Plot price and whale volume over time.
    """
    logger.info("Plotting price vs whale volume...")

    figures_path.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=figsize)
    plt.plot(price_df['date'], price_df['price'], color=price_color, label='Token Price')
    plt.bar(whale_df['date'], whale_df['volume'], color=whale_color, alpha=0.5, label='Whale Volume')

    plt.title("Price vs Whale Activity")
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.legend()
    plt.tight_layout()

    plot_file = figures_path / "price_vs_whale.png"
    plt.savefig(plot_file)
    plt.close()

    logger.info(f"Saved plot to: {plot_file}")
