import requests
import logging
import sqlite3
import os
import time
from datetime import datetime, timedelta
from collections import deque
import threading

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path configuration
RENDER_DB_DIR = '/opt/render/project/data'
LOCAL_DB_DIR = r'C:\Users\NEW USER\PycharmProjects\SafeSwapEscrowBot'

if os.path.exists(RENDER_DB_DIR):
    DB_PATH = os.path.join(RENDER_DB_DIR, 'escrow_bot.db')
else:
    DB_PATH = os.path.join(LOCAL_DB_DIR, 'escrow_bot.db')

# Dictionary mapping crypto symbols to their IDs in CoinGecko API
CRYPTO_ID_MAP = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'LTC': 'litecoin',
    'XMR': 'monero',
    'DASH': 'dash',
    'BCH': 'bitcoin-cash',
    'ZEC': 'zcash',
    # Add more cryptocurrencies as needed
}

# Rate limiting configuration
# CoinGecko free tier: 10-50 calls/minute, we'll be conservative with 10 calls/minute
MAX_CALLS_PER_MINUTE = 10
CACHE_FRESHNESS_SECONDS = 300  # 5 minutes - don't fetch if cache is fresher than this
MIN_INTERVAL_BETWEEN_CALLS = 6  # Minimum 6 seconds between calls (10 calls/minute)

# Rate limiter state
class RateLimiter:
    def __init__(self, max_calls_per_minute, min_interval):
        self.max_calls = max_calls_per_minute
        self.min_interval = min_interval
        self.call_times = deque()
        self.last_call_time = 0
        self.lock = threading.Lock()

    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        with self.lock:
            current_time = time.time()

            # Remove calls older than 1 minute
            while self.call_times and self.call_times[0] < current_time - 60:
                self.call_times.popleft()

            # Check if we've hit the max calls per minute
            if len(self.call_times) >= self.max_calls:
                oldest_call = self.call_times[0]
                wait_time = 60 - (current_time - oldest_call)
                if wait_time > 0:
                    logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                    current_time = time.time()
                    # Clean up old calls again
                    while self.call_times and self.call_times[0] < current_time - 60:
                        self.call_times.popleft()

            # Enforce minimum interval between calls
            time_since_last_call = current_time - self.last_call_time
            if time_since_last_call < self.min_interval:
                wait_time = self.min_interval - time_since_last_call
                logger.debug(f"Waiting {wait_time:.2f} seconds for minimum interval...")
                time.sleep(wait_time)
                current_time = time.time()

            # Record this call
            self.call_times.append(current_time)
            self.last_call_time = current_time

# Global rate limiter instance
rate_limiter = RateLimiter(MAX_CALLS_PER_MINUTE, MIN_INTERVAL_BETWEEN_CALLS)

def init_crypto_prices_table():
    """Initialize the crypto_prices table in escrow_bot.db if it doesn't exist."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crypto_prices (
                crypto_type TEXT PRIMARY KEY,
                price REAL NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database error in init_crypto_prices_table: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def save_price_to_db(crypto_type, price):
    """Save the price of a cryptocurrency to the database."""
    crypto_type = crypto_type.upper()
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO crypto_prices (crypto_type, price, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (crypto_type, price))
        conn.commit()
        logger.info(f"Saved {crypto_type} price: ${price} to database")
    except sqlite3.Error as e:
        logger.error(f"Database error in save_price_to_db: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def get_price_from_db(crypto_type):
    """Retrieve the last known price of a cryptocurrency from the database."""
    crypto_type = crypto_type.upper()
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT price FROM crypto_prices WHERE crypto_type = ?
        ''', (crypto_type,))
        result = cursor.fetchone()

        if result:
            return result[0]
        return None
    except sqlite3.Error as e:
        logger.error(f"Database error in get_price_from_db: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_price_with_age_from_db(crypto_type):
    """
    Retrieve the last known price and its age from the database.

    Returns:
        tuple: (price, seconds_old) or (None, None) if not found
    """
    crypto_type = crypto_type.upper()
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT price, last_updated FROM crypto_prices WHERE crypto_type = ?
        ''', (crypto_type,))
        result = cursor.fetchone()

        if result:
            price, last_updated_str = result
            try:
                last_updated = datetime.strptime(last_updated_str, '%Y-%m-%d %H:%M:%S')
                age_seconds = (datetime.now() - last_updated).total_seconds()
                return price, age_seconds
            except ValueError:
                return price, float('inf')  # Treat parsing errors as very old
        return None, None
    except sqlite3.Error as e:
        logger.error(f"Database error in get_price_with_age_from_db: {e}")
        return None, None
    finally:
        if conn:
            conn.close()


def is_cache_fresh(crypto_type, max_age_seconds=CACHE_FRESHNESS_SECONDS):
    """
    Check if the cached price is fresh enough to use without API call.

    Returns:
        bool: True if cache is fresh, False otherwise
    """
    price, age = get_price_with_age_from_db(crypto_type)
    if price is None:
        return False
    return age <= max_age_seconds

def get_crypto_price(crypto_type, force_refresh=False):
    """
    Get the current price of a cryptocurrency in USD.
    Uses cached price if fresh enough, otherwise fetches from API with rate limiting.

    Args:
        crypto_type (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
        force_refresh (bool): If True, always fetch from API (still rate limited)

    Returns:
        float: The current price in USD, or last known price if unavailable
    """
    crypto_type = crypto_type.upper()

    if crypto_type not in CRYPTO_ID_MAP:
        logger.error(f"Unsupported cryptocurrency: {crypto_type}")
        return None

    # Check if cached price is fresh enough
    if not force_refresh and is_cache_fresh(crypto_type):
        cached_price = get_price_from_db(crypto_type)
        if cached_price is not None:
            logger.debug(f"Using fresh cached {crypto_type} price: ${cached_price}")
            return cached_price

    crypto_id = CRYPTO_ID_MAP[crypto_type]

    try:
        # Wait for rate limiter before making API call
        rate_limiter.wait_if_needed()

        url = f"https://api.coingecko.com/api/v3/simple/price?ids={crypto_id}&vs_currencies=usd"
        response = requests.get(url, timeout=10)

        # Handle rate limiting response
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After', '60')
            logger.warning(f"CoinGecko rate limit hit. Retry after: {retry_after} seconds")
            # Return cached price if available
            fallback_price = get_price_from_db(crypto_type)
            if fallback_price is not None:
                logger.info(f"Using cached {crypto_type} price due to rate limit: ${fallback_price}")
                return fallback_price
            # Wait if no cached price available
            time.sleep(int(retry_after))
            response = requests.get(url, timeout=10)

        response.raise_for_status()
        data = response.json()

        if crypto_id in data and 'usd' in data[crypto_id]:
            price = data[crypto_id]['usd']
            logger.info(f"Fetched {crypto_type} price from API: ${price}")
            save_price_to_db(crypto_type, price)
            return price
        else:
            logger.warning(f"Failed to get price for {crypto_type} from API response")
            fallback_price = get_price_from_db(crypto_type)
            if fallback_price is not None:
                logger.info(f"Using last known {crypto_type} price: ${fallback_price}")
                return fallback_price
            return None
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching {crypto_type} price from API")
        fallback_price = get_price_from_db(crypto_type)
        if fallback_price is not None:
            logger.info(f"Using cached {crypto_type} price due to timeout: ${fallback_price}")
            return fallback_price
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request error fetching {crypto_type} price: {str(e)}")
        fallback_price = get_price_from_db(crypto_type)
        if fallback_price is not None:
            logger.info(f"Using cached {crypto_type} price: ${fallback_price}")
            return fallback_price
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching {crypto_type} price: {str(e)}")
        fallback_price = get_price_from_db(crypto_type)
        if fallback_price is not None:
            logger.info(f"Using cached {crypto_type} price: ${fallback_price}")
            return fallback_price
        logger.error(f"No price available for {crypto_type} and no cached price found")
        return None

def get_multiple_crypto_prices(crypto_types, force_refresh=False):
    """
    Get prices for multiple cryptocurrencies in a single API call.
    More efficient than calling get_crypto_price multiple times.

    Args:
        crypto_types (list): List of cryptocurrency symbols (e.g., ['BTC', 'ETH'])
        force_refresh (bool): If True, always fetch from API (still rate limited)

    Returns:
        dict: Dictionary mapping crypto_type to price {crypto_type: price}
    """
    crypto_types = [ct.upper() for ct in crypto_types]
    results = {}

    # Filter out unsupported cryptos
    supported_types = [ct for ct in crypto_types if ct in CRYPTO_ID_MAP]
    if not supported_types:
        logger.error("No supported cryptocurrencies in the list")
        return results

    # Check which prices are fresh in cache
    types_to_fetch = []
    if not force_refresh:
        for crypto_type in supported_types:
            if is_cache_fresh(crypto_type):
                cached_price = get_price_from_db(crypto_type)
                if cached_price is not None:
                    results[crypto_type] = cached_price
                    logger.debug(f"Using fresh cached {crypto_type} price: ${cached_price}")
                else:
                    types_to_fetch.append(crypto_type)
            else:
                types_to_fetch.append(crypto_type)
    else:
        types_to_fetch = supported_types

    # Fetch prices that are not fresh
    if types_to_fetch:
        crypto_ids = [CRYPTO_ID_MAP[ct] for ct in types_to_fetch]
        ids_param = ','.join(crypto_ids)

        try:
            # Wait for rate limiter before making API call
            rate_limiter.wait_if_needed()

            url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_param}&vs_currencies=usd"
            response = requests.get(url, timeout=10)

            # Handle rate limiting response
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', '60')
                logger.warning(f"CoinGecko rate limit hit. Retry after: {retry_after} seconds")
                # Return cached prices for all requested types
                for crypto_type in types_to_fetch:
                    if crypto_type not in results:
                        fallback_price = get_price_from_db(crypto_type)
                        if fallback_price is not None:
                            results[crypto_type] = fallback_price
                return results

            response.raise_for_status()
            data = response.json()

            # Parse results
            for crypto_type in types_to_fetch:
                crypto_id = CRYPTO_ID_MAP[crypto_type]
                if crypto_id in data and 'usd' in data[crypto_id]:
                    price = data[crypto_id]['usd']
                    logger.info(f"Fetched {crypto_type} price from API: ${price}")
                    save_price_to_db(crypto_type, price)
                    results[crypto_type] = price
                else:
                    # Try to get cached price
                    fallback_price = get_price_from_db(crypto_type)
                    if fallback_price is not None:
                        logger.info(f"Using cached {crypto_type} price: ${fallback_price}")
                        results[crypto_type] = fallback_price

        except Exception as e:
            logger.error(f"Error fetching multiple crypto prices: {str(e)}")
            # Fallback to cached prices
            for crypto_type in types_to_fetch:
                if crypto_type not in results:
                    fallback_price = get_price_from_db(crypto_type)
                    if fallback_price is not None:
                        results[crypto_type] = fallback_price

    return results


def convert_crypto_to_fiat(crypto_amount, crypto_type):
    """
    Convert a cryptocurrency amount to USD.
    
    Args:
        crypto_amount (float): The amount of cryptocurrency
        crypto_type (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
        
    Returns:
        float: The equivalent amount in USD
    """
    price = get_crypto_price(crypto_type)
    
    if price is None:
        return None
    
    usd_amount = float(crypto_amount) * price
    logger.info(f"{crypto_amount} {crypto_type} = ${usd_amount:.2f}")
    return usd_amount

def convert_fiat_to_crypto(usd_amount, crypto_type):
    """
    Convert a USD amount to cryptocurrency.
    
    Args:
        usd_amount (float): The amount in USD
        crypto_type (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
        
    Returns:
        float: The equivalent amount in the specified cryptocurrency
    """
    price = get_crypto_price(crypto_type)
    
    if price is None or price == 0:
        return None
    
    crypto_amount = float(usd_amount) / price
    logger.info(f"${usd_amount} = {crypto_amount} {crypto_type}")
    return crypto_amount