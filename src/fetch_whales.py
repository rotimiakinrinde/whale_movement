# src/fetch_whales.py - FINAL FIXED VERSION

import requests
import pandas as pd
from src.env import get_api_key
from src.logger import logger
from datetime import datetime
from pathlib import Path


def get_token_price_usd(date_str: str, price_df: pd.DataFrame) -> float:
    """
    Get the token (SOL) price in USD for a specific date.
    If exact date not found, use the most recent available price.

    Args:
        date_str (str): Date in YYYY-MM-DD format.
        price_df (pd.DataFrame): DataFrame with columns ['date', 'price'].

    Returns:
        float: Price in USD or most recent price if date not found.
    """
    # Try exact date match first
    price_row = price_df[price_df["date"] == date_str]
    if not price_row.empty:
        return float(price_row["price"].iloc[0])
    
    # If exact date not found, use the most recent price
    if not price_df.empty:
        latest_price = float(price_df["price"].iloc[-1])
        logger.debug(f"No price data for {date_str}, using latest price: ${latest_price:.2f}")
        return latest_price
    
    logger.warning(f"No price data available at all!")
    return 0.0


def fetch_transactions_for_wallet(address: str, api_key: str, base_url: str, limit: int = 100) -> list:
    """
    Fetch transfer transactions for a wallet using the Helius API.

    Args:
        address (str): Solana wallet address.
        api_key (str): Helius API key.
        base_url (str): Base URL for Helius address endpoint.
        limit (int): Max number of transactions to fetch.

    Returns:
        list: Parsed JSON response with transactions.
    """
    url = f"{base_url}/addresses/{address}/transactions"
    params = {
        "api-key": api_key,
        "limit": limit
    }

    # Redact API key for logging
    safe_params = params.copy()
    safe_params["api-key"] = "***REDACTED***"
    logger.debug(f"Requesting Helius API URL: {url} with params: {safe_params}")

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def fetch_whale_transactions(cfg, price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch whale transactions (USD value > threshold) for top wallets.

    Args:
        cfg (Config): Config instance.
        price_df (pd.DataFrame): Token price history with ['date', 'price'].

    Returns:
        pd.DataFrame: Filtered whale transactions.
    """
    logger.info("Starting whale transaction extraction...")

    helius_key = get_api_key("helius", cfg)
    base_url = cfg.get("api", "helius", "base_url")
    
    whale_threshold = cfg.get("analysis", "whale_threshold_usd", default=100)
    logger.info(f"Using whale threshold: ${whale_threshold:,}")
    
    # Get date range for filtering (but make it optional for debugging)
    start_date = cfg.get_date("analysis", "start_date")
    end_date = cfg.get_date("analysis", "end_date")
    logger.info(f"Date range: {start_date.date() if start_date else 'None'} to {end_date.date() if end_date else 'None'}")
    
    address_file = Path(cfg.get("analysis", "top_addresses_file"))

    if not address_file.exists():
        logger.error(f"Whale address file not found: {address_file}")
        return pd.DataFrame()

    addresses = pd.read_csv(address_file)["address"].dropna().unique().tolist()
    logger.info(f"Processing {len(addresses)} whale addresses")
    
    all_whale_txs = []
    processed_addresses = 0

    for addr in addresses:
        try:
            logger.debug(f"Fetching transactions for address: {addr}")
            txs = fetch_transactions_for_wallet(addr, helius_key, base_url)
            processed_addresses += 1
            
            logger.debug(f"Retrieved {len(txs)} transactions for {addr}")

            for i, tx in enumerate(txs):
                try:
                    timestamp = datetime.utcfromtimestamp(tx["timestamp"])
                    date_str = timestamp.strftime("%Y-%m-%d")
                    
                    # Get SOL price (now uses fallback if exact date not found)
                    price_usd = get_token_price_usd(date_str, price_df)
                    
                    if price_usd == 0.0:
                        logger.debug(f"No price data available, skipping transaction")
                        continue

                    # Process NATIVE SOL transfers
                    native_transfers = tx.get("nativeTransfers", [])
                    if native_transfers:
                        logger.debug(f"Found {len(native_transfers)} native transfers in tx {i}")
                    
                    for transfer in native_transfers:
                        try:
                            # The amount is in lamports (confirmed from debug output)
                            amount_lamports = float(transfer.get("amount", 0))
                            amount_sol = amount_lamports / 1e9  # Convert lamports to SOL
                            usd_value = amount_sol * price_usd

                            logger.debug(f"Native transfer: {amount_sol:.4f} SOL = ${usd_value:.2f} USD (threshold: ${whale_threshold})")

                            if usd_value >= whale_threshold:
                                logger.info(f"WHALE NATIVE: ${usd_value:,.2f} ({amount_sol:.4f} SOL)")
                                all_whale_txs.append({
                                    "wallet": addr,
                                    "type": "NATIVE_SOL",
                                    "amount": amount_sol,
                                    "usd_value": usd_value,
                                    "from": transfer.get("fromUserAccount"),
                                    "to": transfer.get("toUserAccount"),
                                    "timestamp": timestamp,
                                    "date": date_str,
                                    "signature": tx.get("signature"),
                                    "token_address": "So11111111111111111111111111111111111111112"  # Native SOL
                                })
                        except Exception as e:
                            logger.warning(f"Error processing native transfer: {e}")
                            continue

                    # Process TOKEN transfers
                    token_transfers = tx.get("tokenTransfers", [])
                    if token_transfers:
                        logger.debug(f"Found {len(token_transfers)} token transfers in tx {i}")
                    
                    for transfer in token_transfers:
                        try:
                            mint = transfer.get("mint", "")
                            # The tokenAmount field is already in decimal format (confirmed from debug)
                            amount = float(transfer.get("tokenAmount", 0))
                            
                            # Handle different token types
                            if mint == "So11111111111111111111111111111111111111112":  # Wrapped SOL
                                token_symbol = "wSOL"
                                token_price_usd = price_usd
                            elif mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":  # USDC
                                token_symbol = "USDC"
                                token_price_usd = 1.0
                            elif mint == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB":  # USDT
                                token_symbol = "USDT"
                                token_price_usd = 1.0
                            else:
                                token_symbol = f"TOKEN-{mint[:8]}..."
                                token_price_usd = 1.0  # Assume $1 for unknown tokens
                                
                            usd_value = amount * token_price_usd

                            logger.debug(f"Token transfer: {amount:.4f} {token_symbol} = ${usd_value:.2f} USD (threshold: ${whale_threshold})")

                            if usd_value >= whale_threshold:
                                logger.info(f"WHALE TOKEN: ${usd_value:,.2f} ({amount:.4f} {token_symbol})")
                                all_whale_txs.append({
                                    "wallet": addr,
                                    "type": "TOKEN",
                                    "amount": amount,
                                    "usd_value": usd_value,
                                    "from": transfer.get("fromUserAccount"),
                                    "to": transfer.get("toUserAccount"),
                                    "timestamp": timestamp,
                                    "date": date_str,
                                    "signature": tx.get("signature"),
                                    "token_address": mint,
                                    "token_symbol": token_symbol
                                })
                        except Exception as e:
                            logger.warning(f"Error processing token transfer: {e}")
                            continue

                except Exception as e:
                    logger.warning(f"Error processing transaction {i} for {addr}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Failed to fetch transactions for {addr}: {e}")
            continue

    logger.info(f"Processed {processed_addresses}/{len(addresses)} addresses")
    logger.info(f"Found {len(all_whale_txs)} whale transactions")

    df = pd.DataFrame(all_whale_txs)

    if df.empty:
        logger.warning("No whale transactions found after filtering")
        logger.info("Debug suggestions:")
        logger.info("  - Most transactions may be from today (2025-08-06) which lacks price data")
        logger.info("  - Try setting end_date to yesterday (2025-08-05) in config")
        logger.info("  - Or check if whale addresses have historical activity in your date range")
        return df

    # Sort by USD value descending
    df = df.sort_values("usd_value", ascending=False)

    output_dir = cfg.get_path("paths", "processed_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "whale_transactions.csv"
    df.to_csv(output_file, index=False)
    
    logger.info(f"Whale transactions saved to: {output_file}")
    logger.info(f"Top whale transaction: ${df['usd_value'].max():,.2f}")
    logger.info(f"Total whale volume: ${df['usd_value'].sum():,.2f}")

    return df