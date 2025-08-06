# main.py

from src.env import load_env
load_env()  # Load environment variables first

from src.config import Config
from src.logger import logger
from src.fetch_whales import fetch_whale_transactions
from src.fetch_price import fetch_token_price_history
from src.process_data import preprocess_data
from src.analyse import analyze_correlation, plot_price_vs_whale
# from src.plot import plot_price_vs_whale_scatter  # Optional scatter plot


def main():
    logger.info("🚀 Starting Whale Activity Analysis Pipeline...")

    # Load configuration
    cfg = Config()

    # Step 1: Fetch token price history
    try:
        price_df = fetch_token_price_history(cfg)
        if price_df.empty:
            logger.warning("No token price data found. Exiting pipeline.")
            return
        logger.info(f"Loaded {len(price_df)} price records.")
    except Exception as e:
        logger.exception(f"Failed to fetch token price history: {e}")
        return

    # Step 2: Fetch whale transactions using Helius
    try:
        whale_df = fetch_whale_transactions(cfg, price_df)
        if whale_df.empty:
            logger.warning("No whale transactions retrieved. Exiting pipeline.")
            return
        logger.info(f"Retrieved {len(whale_df)} whale transactions.")
    except Exception as e:
        logger.exception(f"Failed to fetch whale transactions: {e}")
        return

    # Step 3: Preprocess and align data
    try:
        whale_df, price_df = preprocess_data(whale_df, price_df)
        if whale_df.empty or price_df.empty:
            logger.warning("One or both datasets are empty after preprocessing. Exiting.")
            return
        logger.info("Preprocessing and alignment successful.")
    except Exception as e:
        logger.exception(f"Error during preprocessing: {e}")
        return

    # Step 4: Analyze correlation
    try:
        merged_df, correlation = analyze_correlation(price_df, whale_df, cfg)
        method = cfg.get("analysis", "correlation_method", default="pearson")
        if correlation is not None:
            logger.info(f"{method.title()} correlation: {correlation:.4f}")
        else:
            logger.warning("Correlation analysis returned None.")
    except Exception as e:
        logger.exception(f"Error during correlation analysis: {e}")
        return

    # Step 5: Plot results
    try:
        plot_price_vs_whale(price_df, whale_df, cfg)
        # plot_price_vs_whale_scatter(price_df, whale_df, cfg)  # Optional scatter
        logger.info("Plot(s) generated successfully.")
    except Exception as e:
        logger.exception(f"Plotting failed: {e}")
        return

    logger.info("Whale Activity Analysis Pipeline completed successfully.")


if __name__ == "__main__":
    main()
