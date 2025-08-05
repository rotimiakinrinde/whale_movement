# main.py

from src.env import load_env
load_env()  # Ensure environment variables are loaded before anything else

from src.config import Config
from src.logger import logger
from src.fetch_whales import fetch_whale_transactions
from src.fetch_price import fetch_token_price_history
from src.process_data import preprocess_data
from src.analyse import analyze_correlation, plot_price_vs_whale

def main():
    logger.info("Starting Whale Activity Analysis Pipeline...")

    # Initialize configuration
    cfg = Config()

    # Fetch whale transactions from Dune
    try:
        whale_df = fetch_whale_transactions()
        logger.info("Whale transaction data fetched successfully.")
    except Exception as e:
        logger.error(f"Failed to fetch whale transactions: {e}")
        return

    # Fetch token price data from CoinGecko
    try:
        price_df = fetch_token_price_history(cfg)
        logger.info("Token price data fetched successfully.")
    except Exception as e:
        logger.error(f"Failed to fetch token prices: {e}")
        return
   
    # Preprocess and align both datasets
    try:
        whale_df, price_df = preprocess_data(whale_df, price_df)
        logger.info("Data preprocessing completed.")
    except Exception as e:
        logger.error(f"Error during preprocessing: {e}")
        return

    # Perform correlation analysis
    try:
        merged_df, correlation = analyze_correlation(price_df, whale_df)
        logger.info(f"Correlation analysis completed. Correlation: {correlation:.4f}")
    except Exception as e:
        logger.error(f"Error during correlation analysis: {e}")
        return

    # Generate and save plot
    try:
        plot_price_vs_whale(price_df, whale_df)
        logger.info("Plotting completed.")
    except Exception as e:
        logger.error(f"Error during plotting: {e}")
        return

    logger.info("Pipeline execution completed successfully.")

if __name__ == "__main__":
    main()
