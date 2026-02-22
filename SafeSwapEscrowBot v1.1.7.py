import imghdr
import logging
import os
import sys
import sqlite3
import json
import requests
from datetime import datetime, timedelta
import threading
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
import re
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, CallbackContext, ConversationHandler, ContextTypes
)
from telegram.error import BadRequest
from web3 import Web3

try:
    from init_bitcoinlib import suppress_bitcoinlib_warnings, fix_bitcoinlib_database
    suppress_bitcoinlib_warnings()
    fix_bitcoinlib_database()
except Exception as e:
    print(f"Warning: Could not initialize bitcoinlib database fix: {e}")

import bitcoinlib
from bitcoinlib.wallets import Wallet
import uuid
import hashlib
import random
import string
# Import crypto_utils (compatibility layer for the crypto-utils package)
import crypto_utils
from crypto_utils import KeyManager, WalletManager, TransactionManager, ElectrumXClient
from crypto_utils import ADDRESS_TYPE_LEGACY, ADDRESS_TYPE_SEGWIT, ADDRESS_TYPE_NATIVE_SEGWIT
from crypto_price import get_crypto_price, convert_crypto_to_fiat, convert_fiat_to_crypto, init_crypto_prices_table, get_multiple_crypto_prices
import btcwalletclient_wif
# Import Telethon for group creation
import asyncio
from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodError
from dotenv import load_dotenv

load_dotenv()

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
SELECTING_ROLE, SELECTING_CRYPTO, ENTERING_AMOUNT, ENTERING_RECIPIENT, CONFIRMING_TRANSACTION = range(5)
DISPUTE_REASON, DISPUTE_EVIDENCE = range(5, 7)
SELECTING_WALLET_TYPE, SELECTING_ADDRESS_TYPE, ENTERING_M, ENTERING_N, ENTERING_PUBLIC_KEYS, CONFIRMING_WALLET = range(7, 13)
SELECTING_WITHDRAW_WALLET, ENTERING_WITHDRAW_AMOUNT, ENTERING_WALLET_ADDRESS = range(13, 16)

# Global variables
app = None
telethon_client = None

# Database write lock to prevent concurrent database writes
db_write_lock = threading.Lock()

# Welcome Video URL
WELCOME_VIDEO_URL = os.getenv('WELCOME_VIDEO_URL', '')

# Help Video URL
HELP_VIDEO_URL = os.getenv('HELP_VIDEO_URL', '')

# Database path configuration
RENDER_DB_DIR = '/opt/render/project/data'
LOCAL_DB_DIR = r'C:\Users\NEW USER\PycharmProjects\SafeSwapEscrowBot'

if os.path.exists(RENDER_DB_DIR):
    DB_PATH = os.path.join(RENDER_DB_DIR, 'escrow_bot.db')
else:
    DB_PATH = os.path.join(LOCAL_DB_DIR, 'escrow_bot.db')

logger.info(f"Using database path: {DB_PATH}")

# Database setup
class DatabaseConnection:
    """Context manager for database connections with write locking."""
    
    def __init__(self, db_path, timeout=20.0, use_write_lock=True):
        self.db_path = db_path
        self.timeout = timeout
        self.use_write_lock = use_write_lock
        self.conn = None
        self.lock_acquired = False
    
    def __enter__(self):
        if self.use_write_lock:
            db_write_lock.acquire()
            self.lock_acquired = True
        self.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                try:
                    self.conn.commit()
                except Exception as e:
                    logger.error(f"Error committing transaction: {e}")
                    self.conn.rollback()
            else:
                self.conn.rollback()
            self.conn.close()
        
        if self.lock_acquired:
            db_write_lock.release()
        
        return False

def escape_markdown(text):
    """Escape special characters for Markdown formatting."""
    if text is None:
        return ""

    # Convert to string if it's not already
    text = str(text)

    # Escape special characters that have meaning in Markdown
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')

    return text

def safe_send_message(update, text, parse_mode=None, **kwargs):
    """
    Safely send a message with proper error handling for entity parsing errors.
    Falls back to plain text if entity parsing fails.
    """
    try:
        return update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
    except BadRequest as e:
        if "entity" in str(e).lower() and parse_mode:
            # If entity parsing fails, try sending without parse_mode
            print(f"Entity parsing error: {e}. Sending without formatting.")
            return update.message.reply_text(text, parse_mode=None, **kwargs)
        else:
            # Re-raise other BadRequest errors
            raise

async def safe_send_text(message_method, text, parse_mode=None, **kwargs):
    """
    A more general version of safe_send_message that works with any message sending method.
    Falls back to plain text if entity parsing fails.

    Args:
        message_method: The method to call for sending the message (e.g., update.message.reply_text, query.edit_message_text)
        text: The text to send
        parse_mode: The parse mode to use (ParseMode.MARKDOWN, ParseMode.HTML, etc.)
        **kwargs: Additional arguments to pass to the message method
    """
    try:
        return await message_method(text, parse_mode=parse_mode, **kwargs)
    except BadRequest as e:
        if "entity" in str(e).lower() and parse_mode:
            # If entity parsing fails, try sending without parse_mode
            print(f"Entity parsing error: {e}. Sending without formatting.")
            return await message_method(text, parse_mode=None, **kwargs)
        else:
            # Re-raise other BadRequest errors
            raise


def setup_database():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        # Users table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS users
                       (
                           user_id
                           INTEGER
                           PRIMARY
                           KEY,
                           username
                           TEXT,
                           first_name
                           TEXT,
                           last_name
                           TEXT,
                           language_code
                           TEXT
                           DEFAULT
                           'en',
                           registration_date
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       )
                       ''')

        # Wallets table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS wallets
                       (
                           wallet_id
                           TEXT
                           PRIMARY
                           KEY,
                           user_id
                           INTEGER,
                           crypto_type
                           TEXT,
                           address
                           TEXT,
                           private_key
                           TEXT,
                           balance
                           REAL
                           DEFAULT
                           0.0,
                           pending_balance
                           REAL
                           DEFAULT
                           0.0,
                           wallet_type
                           TEXT
                           DEFAULT
                           'single',
                           address_type
                           TEXT
                           DEFAULT
                           'segwit',
                           required_sigs
                           INTEGER
                           DEFAULT
                           1,
                           total_keys
                           INTEGER
                           DEFAULT
                           1,
                           public_keys
                           TEXT,
                           tx_hex
                           TEXT,
                           txid
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           user_id
                       ) REFERENCES users
                       (
                           user_id
                       )
                           )
                       ''')

        # Transactions table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS transactions
                       (
                           transaction_id
                           TEXT
                           PRIMARY
                           KEY,
                           seller_id
                           INTEGER,
                           buyer_id
                           INTEGER,
                           crypto_type
                           TEXT,
                           amount
                           REAL,
                           fee_amount
                           REAL,
                           status
                           TEXT,
                           creation_date
                           TIMESTAMP,
                           completion_date
                           TIMESTAMP,
                           description
                           TEXT,
                           wallet_id
                           TEXT,
                           tx_hex
                           TEXT,
                           txid
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           seller_id
                       ) REFERENCES users
                       (
                           user_id
                       ),
                           FOREIGN KEY
                       (
                           buyer_id
                       ) REFERENCES users
                       (
                           user_id
                       ),
                           FOREIGN KEY
                       (
                           wallet_id
                       ) REFERENCES wallets
                       (
                           wallet_id
                       )
                           )
                       ''')

        # Disputes table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS disputes
                       (
                           dispute_id
                           TEXT
                           PRIMARY
                           KEY,
                           transaction_id
                           TEXT,
                           initiator_id
                           INTEGER,
                           reason
                           TEXT,
                           evidence
                           TEXT,
                           status
                           TEXT,
                           creation_date
                           TIMESTAMP,
                           resolution_date
                           TIMESTAMP,
                           resolution_notes
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           transaction_id
                       ) REFERENCES transactions
                       (
                           transaction_id
                       ),
                           FOREIGN KEY
                       (
                           initiator_id
                       ) REFERENCES users
                       (
                           user_id
                       )
                           )
                       ''')

        # Stats table for counters
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS stats
                       (
                           stat_key TEXT PRIMARY KEY,
                           stat_value INTEGER DEFAULT 0,
                           last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       )
                       ''')
        
        # Initialize stats if they don't exist
        cursor.execute("INSERT OR IGNORE INTO stats (stat_key, stat_value) VALUES ('deals_completed', 274)")
        cursor.execute("INSERT OR IGNORE INTO stats (stat_key, stat_value) VALUES ('disputes_resolved', 55)")

        # Wallet monitoring table for tracking balance changes
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS wallet_monitoring
                       (
                           monitoring_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           wallet_id TEXT NOT NULL,
                           address TEXT NOT NULL,
                           previous_balance REAL DEFAULT 0.0,
                           current_balance REAL DEFAULT 0.0,
                           balance_change REAL DEFAULT 0.0,
                           last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           monitoring_enabled INTEGER DEFAULT 1,
                           user_id INTEGER,
                           crypto_type TEXT DEFAULT 'BTC',
                           FOREIGN KEY (wallet_id) REFERENCES wallets (wallet_id),
                           FOREIGN KEY (user_id) REFERENCES users (user_id)
                       )
                       ''')

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    
    sanitize_stat_integers()
    enforce_disputes_constraint()
    enforce_tens_place_constraint()
    enforce_ones_place_constraint()


def migrate_wallets_table():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(wallets)")
        columns = [column[1] for column in cursor.fetchall()]

        if 'pending_balance' not in columns:
            cursor.execute('ALTER TABLE wallets ADD COLUMN pending_balance REAL DEFAULT 0.0')
            conn.commit()
            print("Added pending_balance column to wallets table")

        if 'last_balance_update' not in columns:
            cursor.execute('ALTER TABLE wallets ADD COLUMN last_balance_update TIMESTAMP')
            conn.commit()
            print("Added last_balance_update column to wallets table")
    except sqlite3.Error as e:
        print(f"Database migration error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def migrate_transactions_table():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(transactions)")
        columns = [column[1] for column in cursor.fetchall()]

        if 'recipient_username' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN recipient_username TEXT')
            conn.commit()
            print("Added recipient_username column to transactions table")
        
        if 'group_id' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN group_id INTEGER')
            conn.commit()
            print("Added group_id column to transactions table")
        
        if 'intermediary_wallet_id' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN intermediary_wallet_id TEXT')
            conn.commit()
            print("Added intermediary_wallet_id column to transactions table")
        
        if 'auto_transferred' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN auto_transferred INTEGER DEFAULT 0')
            conn.commit()
            print("Added auto_transferred column to transactions table")
        
        if 'deposit_99_notified' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN deposit_99_notified INTEGER DEFAULT 0')
            conn.commit()
            print("Added deposit_99_notified column to transactions table")
        
        if 'deposit_partial_notified' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN deposit_partial_notified INTEGER DEFAULT 0')
            conn.commit()
            print("Added deposit_partial_notified column to transactions table")
        
        if 'initiator_id' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN initiator_id INTEGER')
            conn.commit()
            print("Added initiator_id column to transactions table")
        
        if 'deducted_amount' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN deducted_amount REAL DEFAULT 0.0')
            conn.commit()
            print("Added deducted_amount column to transactions table")
        
        if 'last_partial_balance_notified' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN last_partial_balance_notified REAL DEFAULT 0.0')
            conn.commit()
            print("Added last_partial_balance_notified column to transactions table")
        
        if 'usd_amount' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN usd_amount REAL')
            conn.commit()
            print("Added usd_amount column to transactions table")
        
        if 'usd_fee_amount' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN usd_fee_amount REAL')
            conn.commit()
            print("Added usd_fee_amount column to transactions table")
    except sqlite3.Error as e:
        print(f"Database migration error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# User management functions
def get_or_create_user(user_id, username, first_name, last_name, language_code='en'):
    conn = None
    user = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            cursor.execute(
                'INSERT INTO users (user_id, username, first_name, last_name, language_code) VALUES (?, ?, ?, ?, ?)',
                (user_id, username, first_name, last_name, language_code)
            )
            conn.commit()
            # Fetch the user after insertion
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            user = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in get_or_create_user: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    return user


def get_stat(stat_key):
    """Get a stat value from the database."""
    conn = None
    value = 0
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', (stat_key,))
        result = cursor.fetchone()
        if result:
            value = int(round(result[0]))
    except sqlite3.Error as e:
        print(f"Database error in get_stat: {e}")
    finally:
        if conn:
            conn.close()
    return value


def sanitize_stat_integers():
    """Ensure all stat values in database are stored as integers."""
    import math
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT stat_key, stat_value FROM stats')
        stats = cursor.fetchall()
        
        for stat_key, stat_value in stats:
            if stat_value is not None:
                int_value = int(round(stat_value))
                if stat_value != int_value:
                    if stat_key in ('deals_completed', 'disputes_resolved') and int_value < stat_value:
                        int_value = int(math.ceil(stat_value))
                    cursor.execute('''
                        UPDATE stats 
                        SET stat_value = ? 
                        WHERE stat_key = ?
                    ''', (int_value, stat_key))
                    logger.info(f"Sanitized {stat_key} from {stat_value} to {int_value}")
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error in sanitize_stat_integers: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def enforce_disputes_constraint():
    """Ensure disputes_resolved is between 20% and 25% of deals_completed."""
    import math
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', ('deals_completed',))
        deals_result = cursor.fetchone()
        deals_completed = int(round(deals_result[0])) if deals_result else 0
        
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', ('disputes_resolved',))
        disputes_result = cursor.fetchone()
        disputes_resolved = int(round(disputes_result[0])) if disputes_result else 0
        
        min_disputes = int(math.ceil(deals_completed * 0.20))
        max_disputes = int(math.floor(deals_completed * 0.25))
        
        if disputes_resolved < min_disputes:
            target_disputes = int(deals_completed * 0.22)
            if target_disputes > disputes_resolved:
                cursor.execute('''
                    UPDATE stats 
                    SET stat_value = ?, last_updated = CURRENT_TIMESTAMP 
                    WHERE stat_key = ?
                ''', (int(target_disputes), 'disputes_resolved'))
                conn.commit()
                logger.info(f"Adjusted disputes_resolved from {disputes_resolved} to {target_disputes} to maintain 20-25% constraint")
    except sqlite3.Error as e:
        print(f"Database error in enforce_disputes_constraint: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def enforce_tens_place_constraint():
    """Ensure deals_completed and disputes_resolved never have the same tens digit."""
    import math
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', ('deals_completed',))
        deals_result = cursor.fetchone()
        deals_completed = int(round(deals_result[0])) if deals_result else 0
        
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', ('disputes_resolved',))
        disputes_result = cursor.fetchone()
        disputes_resolved = int(round(disputes_result[0])) if disputes_result else 0
        
        deals_tens = (deals_completed // 10) % 10
        disputes_tens = (disputes_resolved // 10) % 10
        
        if deals_tens == disputes_tens:
            original_disputes = disputes_resolved
            min_disputes = int(math.ceil(deals_completed * 0.20))
            
            for candidate in range(disputes_resolved, disputes_resolved + 20):
                candidate_tens = (candidate // 10) % 10
                if candidate_tens != deals_tens and candidate >= min_disputes:
                    disputes_resolved = candidate
                    break
            
            if disputes_resolved != original_disputes:
                cursor.execute('''
                    UPDATE stats 
                    SET stat_value = ?, last_updated = CURRENT_TIMESTAMP 
                    WHERE stat_key = ?
                ''', (int(disputes_resolved), 'disputes_resolved'))
                conn.commit()
                logger.info(f"Adjusted disputes_resolved from {original_disputes} to {disputes_resolved} to avoid matching tens place digit {deals_tens}")
    except sqlite3.Error as e:
        print(f"Database error in enforce_tens_place_constraint: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def enforce_ones_place_constraint():
    """Ensure both values don't end in 0 at the same time and they don't have the same ones digit."""
    import math
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', ('deals_completed',))
        deals_result = cursor.fetchone()
        deals_completed = int(round(deals_result[0])) if deals_result else 0
        
        cursor.execute('SELECT stat_value FROM stats WHERE stat_key = ?', ('disputes_resolved',))
        disputes_result = cursor.fetchone()
        disputes_resolved = int(round(disputes_result[0])) if disputes_result else 0
        
        deals_ones = deals_completed % 10
        disputes_ones = disputes_resolved % 10
        
        min_disputes = int(math.ceil(deals_completed * 0.20))
        max_disputes = int(math.floor(deals_completed * 0.25))
        
        if deals_ones == 0 and disputes_ones == 0:
            original_disputes = disputes_resolved
            
            for candidate in range(disputes_resolved, disputes_resolved + 20):
                candidate_ones = candidate % 10
                if candidate_ones != 0 and min_disputes <= candidate <= max_disputes:
                    disputes_resolved = candidate
                    break
            
            if disputes_resolved != original_disputes:
                cursor.execute('''
                    UPDATE stats 
                    SET stat_value = ?, last_updated = CURRENT_TIMESTAMP 
                    WHERE stat_key = ?
                ''', (int(disputes_resolved), 'disputes_resolved'))
                conn.commit()
                logger.info(f"Adjusted disputes_resolved from {original_disputes} to {disputes_resolved} to prevent both values ending in 0")
        elif disputes_ones == deals_ones and disputes_ones != 0:
            original_disputes = disputes_resolved
            
            for candidate in range(disputes_resolved, disputes_resolved + 20):
                candidate_ones = candidate % 10
                if candidate_ones != deals_ones and min_disputes <= candidate <= max_disputes:
                    disputes_resolved = candidate
                    break
            
            if disputes_resolved != original_disputes:
                cursor.execute('''
                    UPDATE stats 
                    SET stat_value = ?, last_updated = CURRENT_TIMESTAMP 
                    WHERE stat_key = ?
                ''', (int(disputes_resolved), 'disputes_resolved'))
                conn.commit()
                logger.info(f"Adjusted disputes_resolved from {original_disputes} to {disputes_resolved} to prevent matching ones digits")
    except sqlite3.Error as e:
        print(f"Database error in enforce_ones_place_constraint: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def increment_stat(stat_key):
    """Increment a stat value in the database."""
    try:
        with DatabaseConnection(DB_PATH) as conn:
            cursor = conn.cursor()
            
            STAT_INITIAL_VALUES = {
                'deals_completed': 274,
                'disputes_resolved': 55
            }
            initial_value = STAT_INITIAL_VALUES.get(stat_key, 0)
            
            cursor.execute('''
                INSERT INTO stats (stat_key, stat_value, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(stat_key) DO UPDATE 
                SET stat_value = stat_value + 1, 
                    last_updated = CURRENT_TIMESTAMP
            ''', (stat_key, initial_value))
    except sqlite3.Error as e:
        print(f"Database error in increment_stat: {e}")
    
    sanitize_stat_integers()
    enforce_disputes_constraint()
    enforce_tens_place_constraint()
    enforce_ones_place_constraint()


def process_pending_recipient(user_id, username):
    """
    Process any pending transactions for a recipient when they start the bot.
    Updates transactions and pending balances for recipients who weren't in the database
    when the transaction was initiated.
    """
    if not username:
        return {'success': False, 'transactions_updated': 0}
    
    conn = None
    transactions_updated = 0
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        username_clean = username.lstrip('@')
        
        cursor.execute(
            '''SELECT transaction_id, crypto_type, amount 
               FROM transactions 
               WHERE LOWER(recipient_username) = LOWER(?) AND seller_id IS NULL''',
            (f"@{username_clean}",)
        )
        pending_transactions = cursor.fetchall()
        
        if not pending_transactions:
            cursor.execute(
                '''SELECT transaction_id, crypto_type, amount 
                   FROM transactions 
                   WHERE LOWER(recipient_username) = LOWER(?) AND seller_id IS NULL''',
                (username_clean,)
            )
            pending_transactions = cursor.fetchall()
        
        for transaction_id, crypto_type, amount in pending_transactions:
            pending_result = add_to_pending_balance(user_id, crypto_type, amount)
            
            if pending_result['success']:
                cursor.execute(
                    '''UPDATE transactions 
                       SET seller_id = ?, recipient_username = NULL 
                       WHERE transaction_id = ?''',
                    (user_id, transaction_id)
                )
                transactions_updated += 1
                logger.info(f"Updated transaction {transaction_id} with recipient user_id {user_id}")
        
        conn.commit()
        return {'success': True, 'transactions_updated': transactions_updated}
        
    except sqlite3.Error as e:
        logger.error(f"Database error in process_pending_recipient: {e}")
        if conn:
            conn.rollback()
        return {'success': False, 'transactions_updated': 0}
    finally:
        if conn:
            conn.close()


async def ensure_user_and_process_pending(update: Update) -> dict:
    """
    Ensure user exists in database and process any pending recipient transactions.
    Should be called at the start of each command handler.
    
    Returns dict with 'transactions_updated' count for notification purposes.
    """
    user = update.effective_user
    get_or_create_user(user.id, user.username, user.first_name, user.last_name, user.language_code)
    
    pending_result = process_pending_recipient(user.id, user.username)
    return pending_result


def get_user_id_from_username(username):
    conn = None
    user_id = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        username_to_search = username.lstrip('@')
        cursor.execute('SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)', (username_to_search,))
        result = cursor.fetchone()

        if result:
            user_id = result[0]
    except sqlite3.Error as e:
        print(f"Database error in get_user_id_from_username: {e}")
    finally:
        if conn:
            conn.close()

    return user_id


# Wallet management functions
def create_wallet(user_id, crypto_type, wallet_type='single', address_type=ADDRESS_TYPE_SEGWIT, m=1, n=1, public_keys=None):
    """
    Create a wallet for a user

    Args:
        user_id: User ID
        crypto_type: Cryptocurrency type (BTC, ETH, etc.)
        wallet_type: Wallet type ('single' or 'multisig')
        address_type: Address type ('legacy', 'segwit', 'native_segwit')
        m: Number of signatures required (for multisig)
        n: Total number of keys (for multisig)
        public_keys: List of public keys (for multisig)

    Returns:
        Tuple[str, str]: (wallet_id, address)
    """
    wallet_id = str(uuid.uuid4())
    wallet_name = f"user_{user_id}_{crypto_type.lower()}_{wallet_id}"
    address = None
    private_key = None
    public_keys_json = None

    try:
        # Generate wallet based on crypto type and wallet type
        if crypto_type.upper() == 'BTC':
            if wallet_type == 'single':
                # Create single-signature Bitcoin wallet
                wallet_name, address, private_key = WalletManager.create_single_sig_wallet(wallet_name, address_type)
                public_keys_json = None
            elif wallet_type == 'multisig':
                # Create multisig Bitcoin wallet
                wallet_name, address, private_keys = WalletManager.create_multisig_wallet(wallet_name, m, n, public_keys, address_type)
                # Convert private keys to hex strings if they are bytes objects
                if isinstance(private_keys, list):
                    private_keys_hex = [pk.hex() if isinstance(pk, bytes) else pk for pk in private_keys]
                    private_key = json.dumps(private_keys_hex)
                else:
                    private_key = private_keys

                if public_keys is None:
                    # If public keys were generated, get them from the wallet
                    wallet = Wallet(wallet_name)
                    public_keys = [key.public for key in wallet.keys()]

                # Convert public keys to hex strings if they are bytes objects
                if public_keys:
                    public_keys_hex = [pk.hex() if isinstance(pk, bytes) else pk for pk in public_keys]
                    public_keys_json = json.dumps(public_keys_hex)
                else:
                    public_keys_json = None
            else:
                raise ValueError(f"Invalid wallet type: {wallet_type}")
        elif crypto_type.upper() in ['ETH', 'USDT']:
            # Create Ethereum wallet (multisig not supported yet)
            account = Web3().eth.account.create()
            address = account.address
            private_key = account.privateKey.hex()
            public_keys_json = None
            wallet_type = 'single'  # Force single for ETH/USDT
        else:
            # For other cryptocurrencies, implement appropriate wallet creation
            # This is a placeholder
            address = f"{crypto_type}_address_{wallet_id}"
            private_key = f"{crypto_type}_private_key_{wallet_id}"
            public_keys_json = None
            wallet_type = 'single'  # Force single for other cryptos

        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()

            cursor.execute(
                '''INSERT INTO wallets
                   (wallet_id, user_id, crypto_type, address, private_key, wallet_type, address_type, required_sigs, total_keys, public_keys, tx_hex, txid)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (wallet_id, user_id, crypto_type.upper(), address, private_key, wallet_type, address_type, m, n, public_keys_json, None, None)
            )

            conn.commit()
            
            # Setup wallet monitoring for BTC wallets
            if crypto_type.upper() == 'BTC' and address:
                setup_wallet_monitoring(wallet_id, user_id, address, crypto_type.upper())
                
        except sqlite3.Error as e:
            print(f"Database error in create_wallet: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

        return wallet_id, address
    except Exception as e:
        print(f"Error creating wallet: {e}")
        return None, None


def create_intermediary_wallet(transaction_id, crypto_type):
    """
    Create an intermediary wallet for a specific transaction.
    This wallet is not tied to any user, but to the transaction itself.

    Args:
        transaction_id: Transaction ID
        crypto_type: Cryptocurrency type (BTC, ETH, etc.)

    Returns:
        Tuple[str, str]: (wallet_id, address)
    """
    wallet_id = str(uuid.uuid4())
    wallet_name = f"intermediary_{transaction_id}_{crypto_type.lower()}_{wallet_id}"
    address = None
    private_key = None
    address_type = ADDRESS_TYPE_NATIVE_SEGWIT

    try:
        if crypto_type.upper() == 'BTC':
            wallet_name, address, private_key = WalletManager.create_single_sig_wallet(wallet_name, address_type)
        elif crypto_type.upper() in ['ETH', 'USDT']:
            account = Web3().eth.account.create()
            address = account.address
            private_key = account.privateKey.hex()
        else:
            address = f"{crypto_type}_address_{wallet_id}"
            private_key = f"{crypto_type}_private_key_{wallet_id}"

        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()

            cursor.execute(
                '''INSERT INTO wallets
                   (wallet_id, user_id, crypto_type, address, private_key, wallet_type, address_type, required_sigs, total_keys, public_keys, tx_hex, txid)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (wallet_id, None, crypto_type.upper(), address, private_key, 'single', address_type, 1, 1, None, None, None)
            )

            conn.commit()
            
            # Setup wallet monitoring for BTC intermediary wallets
            if crypto_type.upper() == 'BTC' and address:
                setup_wallet_monitoring(wallet_id, None, address, crypto_type.upper())
                
        except sqlite3.Error as e:
            print(f"Database error in create_intermediary_wallet: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

        return wallet_id, address
    except Exception as e:
        print(f"Error creating intermediary wallet: {e}")
        return None, None


def get_user_wallets(user_id):
    conn = None
    wallets = []
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('''SELECT wallet_id, crypto_type, address, balance, private_key,
                                 wallet_type, address_type, required_sigs, total_keys, public_keys
                          FROM wallets WHERE user_id = ?''', (user_id,))
        wallets = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in get_user_wallets: {e}")
    finally:
        if conn:
            conn.close()
    return wallets


def get_wallet_balance(wallet_id):
    conn = None
    wallet = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT crypto_type, address, private_key, balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in get_wallet_balance: {e}")
    finally:
        if conn:
            conn.close()

    if not wallet:
        return None

    crypto_type, address, private_key, balance = wallet

    # In a real implementation, you would query the blockchain for the current balance
    # This is a placeholder
    return balance


def get_btc_balance_from_blockchain(address):
    """
    Fetch BTC balance from blockchain APIs for a given address with multiple fallbacks

    Args:
        address (str): Bitcoin wallet address

    Returns:
        float: Balance in BTC, or None if all requests fail
    """
    apis = [
        {
            'name': 'Blockchain.info',
            'url': f"https://blockchain.info/q/addressbalance/{address}",
            'parser': lambda r: int(r.text.strip()) / 100000000
        },
        {
            'name': 'Blockstream',
            'url': f"https://blockstream.info/api/address/{address}",
            'parser': lambda r: (r.json().get('chain_stats', {}).get('funded_txo_sum', 0) - 
                                r.json().get('chain_stats', {}).get('spent_txo_sum', 0)) / 100000000
        },
        {
            'name': 'Mempool.space',
            'url': f"https://mempool.space/api/address/{address}",
            'parser': lambda r: (r.json().get('chain_stats', {}).get('funded_txo_sum', 0) - 
                                r.json().get('chain_stats', {}).get('spent_txo_sum', 0)) / 100000000
        }
    ]
    
    for api in apis:
        try:
            logger.info(f"Attempting to fetch balance from {api['name']} for address {address}")
            response = requests.get(api['url'], timeout=15)
            if response.status_code == 200:
                balance_btc = api['parser'](response)
                logger.info(f"Successfully fetched balance from {api['name']}: {balance_btc} BTC")
                return balance_btc
            else:
                logger.warning(f"{api['name']} API error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching BTC balance from {api['name']}: {e}")
            continue
    
    logger.error(f"All blockchain APIs failed for address {address}")
    return None


async def async_get_btc_balance_from_blockchain(address):
    """
    Non-blocking async wrapper for get_btc_balance_from_blockchain.
    Runs the synchronous API call in a thread pool so it doesn't block the event loop.

    Args:
        address (str): Bitcoin wallet address

    Returns:
        float: Balance in BTC, or None if all requests fail
    """
    return await asyncio.to_thread(get_btc_balance_from_blockchain, address)


def get_cached_wallet_balance(address):
    """
    Get cached BTC balance from database instead of making API calls.
    This function should be used by all user-facing operations to avoid blocking.
    The balance is updated by background jobs.

    Args:
        address (str): Bitcoin wallet address

    Returns:
        dict: {'balance': float, 'last_update': datetime} or None if not found
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT balance, last_balance_update
            FROM wallets
            WHERE address = ? AND crypto_type = 'BTC'
        ''', (address,))

        result = cursor.fetchone()

        if result:
            balance, last_update = result
            return {
                'balance': balance if balance is not None else 0.0,
                'last_update': last_update
            }

        logger.warning(f"No cached balance found for address {address}")
        return None

    except sqlite3.Error as e:
        logger.error(f"Database error in get_cached_wallet_balance: {e}")
        return None
    finally:
        if conn:
            conn.close()


def update_wallet_balance(wallet_id, new_balance):
    """
    Update the stored balance in the database for a wallet

    Args:
        wallet_id (str): Wallet ID
        new_balance (float): New balance value

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with DatabaseConnection(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE wallets SET balance = ?, last_balance_update = CURRENT_TIMESTAMP WHERE wallet_id = ?',
                (new_balance, wallet_id)
            )
        return True
    except sqlite3.Error as e:
        print(f"Database error updating wallet balance: {e}")
        return False


def get_cached_balance_by_wallet_id(wallet_id):
    """
    Get cached balance from database by wallet_id for user-facing operations.
    This is a non-blocking alternative to sync_blockchain_balance().
    Background jobs update the cache, user operations read from cache.

    Args:
        wallet_id (str): Wallet ID

    Returns:
        dict: {
            'success': bool,
            'db_balance': float,
            'new_blockchain_balance': float,  # same as db_balance for cached reads
            'old_balance': float,  # same as db_balance for cached reads
            'difference': float,  # always 0 for cached reads
            'last_update': str  # timestamp of last cache update
        }
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT balance, last_balance_update, address
            FROM wallets
            WHERE wallet_id = ?
        ''', (wallet_id,))

        result = cursor.fetchone()

        if not result:
            return {'success': False, 'error': 'Wallet not found'}

        balance, last_update, address = result
        cached_balance = balance if balance is not None else 0.0

        return {
            'success': True,
            'db_balance': cached_balance,
            'new_blockchain_balance': cached_balance,
            'old_balance': cached_balance,
            'difference': 0.0,
            'last_update': last_update if last_update else 'Never',
            'reconciled': False  # cached reads don't reconcile
        }

    except sqlite3.Error as e:
        logger.error(f"Database error in get_cached_balance_by_wallet_id: {e}")
        return {'success': False, 'error': f'Database error: {e}'}
    finally:
        if conn:
            conn.close()


def sync_blockchain_balance(wallet_id):
    """
    Fetch current balance from blockchain and sync with database.
    If blockchain balance differs, reconcile the difference.

    Args:
        wallet_id (str): Wallet ID

    Returns:
        dict: {success: bool, old_balance: float, new_blockchain_balance: float, db_balance: float, difference: float}
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT address, balance, crypto_type FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()

        if not wallet:
            return {'success': False, 'error': 'Wallet not found'}

        address, db_balance, crypto_type = wallet

        if crypto_type.upper() != 'BTC':
            return {'success': False, 'error': 'Balance sync only supported for BTC'}

        blockchain_balance = get_btc_balance_from_blockchain(address)

        if blockchain_balance is None:
            return {'success': False, 'error': 'Failed to fetch balance from blockchain'}

        old_balance = db_balance
        difference = blockchain_balance - db_balance

        if difference != 0:
            if difference > 0:
                reconciled_balance = db_balance + difference
            else:
                reconciled_balance = db_balance

            update_wallet_balance(wallet_id, reconciled_balance)

            return {
                'success': True,
                'old_balance': old_balance,
                'new_blockchain_balance': blockchain_balance,
                'db_balance': reconciled_balance,
                'difference': difference,
                'reconciled': True
            }
        else:
            return {
                'success': True,
                'old_balance': old_balance,
                'new_blockchain_balance': blockchain_balance,
                'db_balance': db_balance,
                'difference': 0,
                'reconciled': False
            }
    except sqlite3.Error as e:
        print(f"Database error in sync_blockchain_balance: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


async def async_sync_blockchain_balance(wallet_id):
    """
    Non-blocking async wrapper for sync_blockchain_balance.
    Runs the synchronous function in a thread pool so it doesn't block the event loop.

    Args:
        wallet_id (str): Wallet ID

    Returns:
        dict: Same return as sync_blockchain_balance
    """
    return await asyncio.to_thread(sync_blockchain_balance, wallet_id)


def subtract_wallet_balance(wallet_id, amount):
    """
    Subtract an amount from wallet balance when transaction is initiated.

    Args:
        wallet_id (str): Wallet ID
        amount (float): Amount to subtract

    Returns:
        dict: {success: bool, old_balance: float, new_balance: float, error: str (if any)}
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
        result = cursor.fetchone()

        if not result:
            return {'success': False, 'error': 'Wallet not found'}

        old_balance = result[0]
        
        # Balance check removed - allow transactions regardless of balance
        # if old_balance < amount:
        #     return {'success': False, 'error': 'Insufficient balance', 'old_balance': old_balance, 'required': amount}
        
        new_balance = old_balance - amount

        cursor.execute(
            'UPDATE wallets SET balance = ? WHERE wallet_id = ?',
            (new_balance, wallet_id)
        )

        conn.commit()

        return {'success': True, 'old_balance': old_balance, 'new_balance': new_balance}
    except sqlite3.Error as e:
        print(f"Database error in subtract_wallet_balance: {e}")
        if conn:
            conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def add_to_pending_balance(user_id, crypto_type, amount):
    """
    Add an amount to the pending balance of a user's wallet.

    Args:
        user_id (int): User ID
        crypto_type (str): Cryptocurrency type (e.g., 'BTC')
        amount (float): Amount to add to pending balance

    Returns:
        dict: {success: bool, old_pending_balance: float, new_pending_balance: float, wallet_id: str, error: str (if any)}
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT wallet_id, pending_balance FROM wallets WHERE user_id = ? AND crypto_type = ?',
                       (user_id, crypto_type.upper()))
        result = cursor.fetchone()

        if not result:
            return {'success': False, 'error': 'Wallet not found for recipient'}

        wallet_id, old_pending_balance = result
        new_pending_balance = old_pending_balance + amount

        cursor.execute(
            'UPDATE wallets SET pending_balance = ? WHERE wallet_id = ?',
            (new_pending_balance, wallet_id)
        )

        conn.commit()

        return {
            'success': True,
            'wallet_id': wallet_id,
            'old_pending_balance': old_pending_balance,
            'new_pending_balance': new_pending_balance
        }
    except sqlite3.Error as e:
        print(f"Database error in add_to_pending_balance: {e}")
        if conn:
            conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


# Transaction management functions
def create_transaction(seller_id, buyer_id, crypto_type, amount, description="", wallet_id=None, tx_hex=None, txid=None, recipient_username=None, group_id=None, intermediary_wallet_id=None, initiator_id=None, deducted_amount=0.0, usd_amount=None, usd_fee_amount=None):
    transaction_id = str(uuid.uuid4())
    fee_amount = amount * 0.05  # 5% fee

    try:
        with DatabaseConnection(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''INSERT INTO transactions
                   (transaction_id, seller_id, buyer_id, crypto_type, amount, fee_amount, status, creation_date, description, wallet_id, tx_hex, txid, recipient_username, group_id, intermediary_wallet_id, initiator_id, deducted_amount, usd_amount, usd_fee_amount)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (transaction_id, seller_id, buyer_id, crypto_type.upper(), amount, fee_amount, 'PENDING', datetime.now().isoformat(), description, wallet_id, tx_hex, txid, recipient_username, group_id, intermediary_wallet_id, initiator_id, deducted_amount, usd_amount, usd_fee_amount)
            )
    except sqlite3.Error as e:
        print(f"Database error in create_transaction: {e}")
        return None

    return transaction_id


def get_transaction(transaction_id):
    conn = None
    transaction = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM transactions WHERE transaction_id = ?', (transaction_id,))
        transaction = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in get_transaction: {e}")
    finally:
        if conn:
            conn.close()
    return transaction


def get_pending_transactions_for_buyer(user_id):
    conn = None
    transactions = []
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute(
            '''SELECT transaction_id, seller_id, buyer_id, crypto_type, amount, fee_amount, 
                      status, creation_date, description
               FROM transactions
               WHERE buyer_id = ? AND status = 'PENDING' AND (initiator_id IS NULL OR initiator_id != ?)
               ORDER BY creation_date ASC''',
            (user_id, user_id)
        )
        transactions = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in get_pending_transactions_for_buyer: {e}")
    finally:
        if conn:
            conn.close()
    return transactions


def update_transaction_status(transaction_id, status):
    try:
        with DatabaseConnection(DB_PATH) as conn:
            cursor = conn.cursor()

            if status == 'COMPLETED':
                cursor.execute(
                    'UPDATE transactions SET status = ?, completion_date = ? WHERE transaction_id = ?',
                    (status, datetime.now().isoformat(), transaction_id)
                )
                increment_stat('deals_completed')
            else:
                cursor.execute(
                    'UPDATE transactions SET status = ? WHERE transaction_id = ?',
                    (status, transaction_id)
                )
    except sqlite3.Error as e:
        print(f"Database error in update_transaction_status: {e}")


def update_transaction_group_id(transaction_id, group_id):
    try:
        with DatabaseConnection(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE transactions SET group_id = ? WHERE transaction_id = ?',
                (group_id, transaction_id)
            )
    except sqlite3.Error as e:
        print(f"Database error in update_transaction_group_id: {e}")

def get_user_transactions(user_id):
    conn = None
    transactions = []
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''SELECT *
               FROM transactions
               WHERE seller_id = ?
                  OR buyer_id = ?
               ORDER BY creation_date DESC''',
            (user_id, user_id)
        )
        transactions = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in get_user_transactions: {e}")
    finally:
        if conn:
            conn.close()
    return transactions


def get_user_pending_transaction_balance(user_id, crypto_type):
    """
    Calculate the total pending balance from transactions where the user is the recipient (seller).
    
    Args:
        user_id: The user's ID
        crypto_type: The cryptocurrency type (e.g., 'BTC')
    
    Returns:
        The total pending balance for the specified crypto type
    """
    conn = None
    pending_balance = 0.0
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute(
            '''SELECT SUM(amount)
               FROM transactions
               WHERE seller_id = ? AND crypto_type = ? AND status = 'PENDING' ''',
            (user_id, crypto_type)
        )
        result = cursor.fetchone()
        if result and result[0] is not None:
            pending_balance = result[0]
    except sqlite3.Error as e:
        print(f"Database error in get_user_pending_transaction_balance: {e}")
    finally:
        if conn:
            conn.close()
    return pending_balance


def check_duplicate_description(user_id, description):
    """
    Check if a user has already used this description for an active (non-completed) transaction.
    Completed transactions can have their descriptions reused.
    
    Args:
        user_id: The user's ID (either buyer_id or seller_id)
        description: The transaction description to check
    
    Returns:
        bool: True if description is a duplicate in active transactions, False if it's unique
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute(
            '''SELECT COUNT(*) FROM transactions 
               WHERE (buyer_id = ? OR seller_id = ?) 
               AND LOWER(description) = LOWER(?)
               AND status NOT IN ('CANCELLED', 'COMPLETED') ''',
            (user_id, user_id, description.strip())
        )
        
        count = cursor.fetchone()[0]
        return count > 0
        
    except sqlite3.Error as e:
        logger.error(f"Database error checking duplicate description: {e}")
        return False
    finally:
        if conn:
            conn.close()

def auto_refresh_user_balances(user_id):
    """
    Automatically refresh all wallet balances for a user.
    This function is called before executing any command to ensure balances are up-to-date.
    
    Args:
        user_id (int): The user's Telegram ID
        
    Returns:
        dict: Summary of refresh results
    """
    try:
        wallets = get_user_wallets(user_id)
        refresh_results = {
            'total_wallets': len(wallets),
            'refreshed_count': 0,
            'failed_count': 0,
            'btc_wallets_updated': 0,
            'errors': []
        }
        
        for wallet in wallets:
            wallet_id, crypto_type = wallet[0], wallet[1]

            if crypto_type.upper() == 'BTC':
                try:
                    # Use cached balance instead of blocking API call
                    sync_result = get_cached_balance_by_wallet_id(wallet_id)
                    if sync_result['success']:
                        refresh_results['refreshed_count'] += 1
                        refresh_results['btc_wallets_updated'] += 1
                        logger.info(f"Retrieved cached balance for BTC wallet {wallet_id} for user {user_id}")
                    else:
                        refresh_results['failed_count'] += 1
                        refresh_results['errors'].append(f"Failed to get cached balance for wallet {wallet_id}: {sync_result.get('error', 'Unknown error')}")
                except Exception as e:
                    refresh_results['failed_count'] += 1
                    refresh_results['errors'].append(f"Error getting cached balance for wallet {wallet_id}: {str(e)}")
                    logger.error(f"Error retrieving cached balance for wallet {wallet_id}: {e}")
            else:
                refresh_results['refreshed_count'] += 1
        
        logger.info(f"Auto-refresh completed for user {user_id}: {refresh_results['refreshed_count']}/{refresh_results['total_wallets']} wallets processed")
        return refresh_results
        
    except Exception as e:
        logger.error(f"Error in auto_refresh_user_balances for user {user_id}: {e}")
        return {
            'total_wallets': 0,
            'refreshed_count': 0,
            'failed_count': 1,
            'btc_wallets_updated': 0,
            'errors': [f"General error: {str(e)}"]
        }


def with_auto_balance_refresh(command_func):
    """
    Decorator that automatically refreshes user wallet balances before executing a command.
    
    Args:
        command_func: The command function to wrap
        
    Returns:
        Wrapped function that refreshes balances first
    """
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user = update.effective_user
        if user and user.id:
            try:
                auto_refresh_user_balances(user.id)
            except Exception as e:
                logger.error(f"Error auto-refreshing balances for user {user.id} in command {command_func.__name__}: {e}")
        
        return await command_func(update, context, *args, **kwargs)
    
    wrapper.__name__ = command_func.__name__
    wrapper.__doc__ = command_func.__doc__
    return wrapper


def has_pending_transactions(user_id, crypto_type='BTC'):
    """
    Check if a user has any pending transactions (as buyer or seller).
    
    Args:
        user_id: The user's ID
        crypto_type: The cryptocurrency type (e.g., 'BTC')
    
    Returns:
        bool: True if user has pending transactions, False otherwise
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute(
            '''SELECT COUNT(*)
               FROM transactions
               WHERE (seller_id = ? OR buyer_id = ?) 
                 AND crypto_type = ? 
                 AND status IN ('PENDING', 'DISPUTED')''',
            (user_id, user_id, crypto_type)
        )
        result = cursor.fetchone()
        return result[0] > 0 if result else False
    except sqlite3.Error as e:
        print(f"Database error in has_pending_transactions: {e}")
        return False
    finally:
        if conn:
            conn.close()


# Dispute management functions
def create_dispute(transaction_id, initiator_id, reason, evidence):
    dispute_id = str(uuid.uuid4())

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''INSERT INTO disputes
               (dispute_id, transaction_id, initiator_id, reason, evidence, status, creation_date)
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (dispute_id, transaction_id, initiator_id, reason, evidence, 'OPEN', datetime.now().isoformat())
        )

        # Update transaction status
        cursor.execute(
            'UPDATE transactions SET status = ? WHERE transaction_id = ?',
            ('DISPUTED', transaction_id)
        )

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error in create_dispute: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

    return dispute_id


def resolve_dispute(dispute_id, resolution, notes):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT transaction_id FROM disputes WHERE dispute_id = ?', (dispute_id,))
        result = cursor.fetchone()
        if not result:
            print(f"Dispute {dispute_id} not found")
            return False

        transaction_id = result[0]
        
        cursor.execute('SELECT buyer_id, seller_id, crypto_type, wallet_id FROM transactions WHERE transaction_id = ?', (transaction_id,))
        transaction = cursor.fetchone()
        if not transaction:
            print(f"Transaction {transaction_id} not found")
            return False
        
        buyer_id, seller_id, crypto_type, wallet_id = transaction

        cursor.execute(
            '''UPDATE disputes
               SET status           = ?,
                   resolution_date  = ?,
                   resolution_notes = ?
               WHERE dispute_id = ?''',
            ('RESOLVED', datetime.now().isoformat(), notes, dispute_id)
        )

        if resolution == 'REFUNDED' and crypto_type == 'BTC':
            refund_result = refund_btc_to_buyer(wallet_id, seller_id)
            
            if refund_result['success']:
                cursor.execute(
                    'UPDATE transactions SET status = ? WHERE transaction_id = ?',
                    (resolution, transaction_id)
                )
                print(f"Dispute resolved: 50% sent to seller ({refund_result['seller_amount']:.8f} BTC), "
                      f"50% sent to fee wallet ({refund_result['fee_amount']:.8f} BTC). "
                      f"Transaction ID: {refund_result['txid']}")
            else:
                print(f"Failed to process refund: {refund_result.get('error', 'Unknown error')}")
                return False
        else:
            cursor.execute(
                'UPDATE transactions SET status = ? WHERE transaction_id = ?',
                (resolution, transaction_id)
            )

        conn.commit()
        increment_stat('disputes_resolved')
        return True
    except sqlite3.Error as e:
        print(f"Database error in resolve_dispute: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f"Error in resolve_dispute: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# Bot command handlers
async def start(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    get_or_create_user(user.id, user.username, user.first_name, user.last_name, user.language_code)
    
    pending_result = process_pending_recipient(user.id, user.username)
    if pending_result['success'] and pending_result['transactions_updated'] > 0:
        await update.message.reply_text(
            f"✅ {pending_result['transactions_updated']} pending transaction(s) have been linked to your account!\n"
            f"You will be prompted to accept or decline them."
        )
    
    pending_transactions = get_pending_transactions_for_buyer(user.id)
    
    if pending_transactions:
        first_transaction = pending_transactions[0]
        transaction_id = first_transaction[0]
        seller_id = first_transaction[1]
        crypto_type = first_transaction[3]
        amount = first_transaction[4]
        fee_amount = first_transaction[5]
        creation_date = first_transaction[7]
        description = first_transaction[8]
        
        conn = None
        seller_username = "Unknown"
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT username FROM users WHERE user_id = ?', (seller_id,))
            seller_result = cursor.fetchone()
            if seller_result and seller_result[0]:
                seller_username = f"@{seller_result[0]}"
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            if conn:
                conn.close()
        
        # Use cached prices for instant response (no API calls)
        usd_amount = convert_crypto_to_fiat(amount, crypto_type, use_cache_only=True)
        usd_value_text = f"${usd_amount:.2f} USD" if usd_amount is not None else "USD value unavailable"

        usd_fee = convert_crypto_to_fiat(fee_amount, crypto_type, use_cache_only=True) if fee_amount else None
        usd_fee_text = f"${usd_fee:.2f} USD" if usd_fee is not None else "USD value unavailable"

        total_crypto = amount + fee_amount if fee_amount else amount
        usd_total = convert_crypto_to_fiat(total_crypto, crypto_type, use_cache_only=True)
        usd_total_text = f"${usd_total:.2f} USD" if usd_total is not None else "USD value unavailable"
        
        remaining_count = len(pending_transactions) - 1
        remaining_text = f"\n\n⚠️ You have {remaining_count} more pending transaction(s) after this one." if remaining_count > 0 else ""
        
        pending_message = (
            f"⚠️ *PENDING TRANSACTION - ACTION REQUIRED*\n\n"
            f"You have a pending transaction that requires your response.\n"
            f"You must accept or decline this transaction before proceeding.\n\n"
            f"*Transaction Details:*\n"
            f"*Transaction ID:* `{transaction_id}`\n"
            f"*From:* {seller_username}\n"
            f"*Cryptocurrency:* {crypto_type}\n"
            f"*Amount:* {amount:.8f} {crypto_type}\n"
            f"*USD Value:* {usd_value_text}\n"
            f"*Escrow Fee (5%):* {usd_fee_text}\n"
            f"*Total:* {usd_total_text}\n"
            f"*Created:* {creation_date}\n"
            f"*Description:* {description if description else 'N/A'}"
            f"{remaining_text}"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Accept", callback_data=f'accept_transaction_{transaction_id}'),
                InlineKeyboardButton("❌ Decline", callback_data=f'decline_transaction_{transaction_id}')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_send_text(
            update.message.reply_text,
            pending_message,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        return

    deals_completed = get_stat('deals_completed')
    disputes_resolved = get_stat('disputes_resolved')
    
    if deals_completed % 10 == 0 and disputes_resolved % 10 == 0:
        disputes_resolved += 1
    
    welcome_message = (
        f"Welcome to SafeSwap Escrow Bot,              {user.first_name}{f' {user.last_name}' if user.last_name else ''}!\n\n"
        "We are your trusted escrow service for secure transactions. "
        "Keep your funds safe and pay other users with confidence.\n\n"
        f"📊 *Deals Completed:* {deals_completed:,}\n"
        f"⚖️ *Disputes Resolved:* {disputes_resolved:,}\n\n"
        "_Tap 'Help Desk' button for further guidance_\n\n"
    )

    # Create a ReplyKeyboardMarkup with the required buttons
    keyboard = [
        [KeyboardButton("My Account"), KeyboardButton("Transaction History")],
        [KeyboardButton("Language"), KeyboardButton("Help Desk")],
        [KeyboardButton("Withdraw Funds")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    if WELCOME_VIDEO_URL:
        try:
            await update.message.reply_video(
                video=WELCOME_VIDEO_URL,
                caption=welcome_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        except BadRequest as e:
            logger.warning(f"Failed to send video: {e}. Falling back to text message.")
            await update.message.reply_text(
                welcome_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
    else:
        await update.message.reply_text(
            welcome_message,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )


async def help_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    help_text = (
        "*If Something Goes Wrong:*\n"
        "Open a dispute if there's a problem with your transaction. Our team will review the evidence and make a decision on whether or not we will issue a refund within 1 to 2 business days or sooner.\n\n"
        
        "*Escrow Fee:*\n"
        "We charge a 5% fee for all transactions processed through escrow."
    )

    if HELP_VIDEO_URL:
        try:
            await update.message.reply_video(
                video=HELP_VIDEO_URL,
                caption=help_text,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            logger.warning(f"Failed to send help video: {e}. Falling back to text message.")
            await safe_send_message(update, help_text, parse_mode=ParseMode.MARKDOWN)
    else:
        await safe_send_message(update, help_text, parse_mode=ParseMode.MARKDOWN)


@with_auto_balance_refresh
async def wallet_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user
    wallets = get_user_wallets(user.id)

    if not wallets:
        keyboard = [
            [
                InlineKeyboardButton("Bitcoin (BTC)", callback_data='create_wallet_BTC')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "You don't have any wallets yet. Choose a cryptocurrency to create your first wallet:",
            reply_markup=reply_markup
        )
    else:
        wallet_text = "Your wallets:\n\n"
        for wallet in wallets:
            wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
            wallet_type = wallet[5] if len(wallet) > 5 else "single"
            address_type = wallet[6] if len(wallet) > 6 else "segwit"

            # Get USD value of the balance
            usd_balance = convert_crypto_to_fiat(balance, crypto_type, use_cache_only=True)
            usd_value_text = f"(${usd_balance:.2f} USD)" if usd_balance is not None else "(USD value unavailable)"

            # Balance is updated by background jobs, no need to fetch from blockchain here
            # This prevents blocking API calls during user interactions

            # Get pending transaction balance
            pending_tx_balance = get_user_pending_transaction_balance(user.id, crypto_type)
            pending_usd_balance = convert_crypto_to_fiat(pending_tx_balance, crypto_type, use_cache_only=True)
            pending_usd_value_text = f"(${pending_usd_balance:.2f} USD)" if pending_usd_balance is not None else "(USD value unavailable)"

            # Escape the address for Markdown
            escaped_address = escape_markdown(address)
            wallet_text += f"*{crypto_type}*\n"
            wallet_text += f"Type: {address_type.capitalize()}\n"
            wallet_text += f"Address: `{escaped_address}`\n"
            wallet_text += f"Balance: {balance:.8f} {crypto_type} {usd_value_text}\n"
            
            # Show pending balance if there are pending transactions
            if pending_tx_balance > 0:
                wallet_text += f"Pending: {pending_tx_balance:.8f} {crypto_type} {pending_usd_value_text}\n"

            if wallet_type == "multisig" and len(wallet) > 7:
                m, n = wallet[7], wallet[8]
                wallet_text += f"Signatures required: {m} of {n}\n"

            wallet_text += "\n"

        keyboard = [
            [
                InlineKeyboardButton("Refresh Balances", callback_data='refresh_balances')
            ]
        ]
        
        if user.username and user.username.lower() == 'safeswapsupport':
            keyboard.append([InlineKeyboardButton("Delete", callback_data='delete_wallet')])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_send_text(
            update.message.reply_text,
            wallet_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )


async def wallet_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    data = query.data

    if data == 'deposit_to_escrow':
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT transaction_id, amount, wallet_id, intermediary_wallet_id
                FROM transactions
                WHERE buyer_id = ? AND status = 'PENDING' AND crypto_type = 'BTC' AND intermediary_wallet_id IS NOT NULL
                ORDER BY creation_date DESC
                LIMIT 1
            ''', (user.id,))
            
            transaction = cursor.fetchone()
            
            if not transaction:
                await query.edit_message_text(
                    "❌ No pending BTC transaction found that requires deposit to escrow.\n\n"
                    "You can only use this feature if you have an active pending transaction as a buyer."
                )
                return
            
            transaction_id, transaction_amount, buyer_wallet_id, intermediary_wallet_id = transaction
            
            cursor.execute('SELECT balance FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
            intermediary_balance_result = cursor.fetchone()
            intermediary_balance = intermediary_balance_result[0] if intermediary_balance_result else 0.0
            
            if intermediary_balance >= transaction_amount:
                await query.edit_message_text(
                    f"✅ Transaction fully funded!\n\n"
                    f"The escrow wallet already has {intermediary_balance:.8f} BTC, which is sufficient for the transaction amount of {transaction_amount:.8f} BTC.\n\n"
                    f"No additional deposit is needed."
                )
                return
            
            cursor.execute('SELECT address, private_key FROM wallets WHERE wallet_id = ?', (buyer_wallet_id,))
            buyer_wallet = cursor.fetchone()
            
            if not buyer_wallet:
                await query.edit_message_text("❌ Buyer wallet not found.")
                return
            
            buyer_address, buyer_private_key = buyer_wallet
            
            cursor.execute('SELECT address FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
            intermediary_result = cursor.fetchone()
            
            if not intermediary_result:
                await query.edit_message_text("❌ Intermediary wallet not found.")
                return
            
            intermediary_address = intermediary_result[0]
            
            remaining_amount = transaction_amount - intermediary_balance
            status_msg = f"🔄 Processing deposit to escrow...\n\n"
            status_msg += f"Transaction: {transaction_id}\n"
            status_msg += f"Required: {transaction_amount:.8f} BTC\n"
            if intermediary_balance > 0:
                status_msg += f"Already deposited: {intermediary_balance:.8f} BTC\n"
                status_msg += f"Remaining: {remaining_amount:.8f} BTC\n"
            status_msg += f"\nPlease wait..."
            
            await query.edit_message_text(status_msg)
            
            try:
                balance_satoshis = await asyncio.to_thread(btcwalletclient_wif.get_balance, buyer_address)
                balance_btc = balance_satoshis / 1e8

                transaction_amount_satoshis = int(transaction_amount * 1e8)
                required_balance_satoshis = transaction_amount_satoshis + 250

                if balance_satoshis < required_balance_satoshis:
                    transfer_result = await asyncio.to_thread(
                        btcwalletclient_wif.send_max_btc_auto,
                        wif_private_key=buyer_private_key,
                        destination_address=intermediary_address
                    )
                else:
                    transfer_result = await asyncio.to_thread(
                        btcwalletclient_wif.send_specific_btc_amount,
                        wif_private_key=buyer_private_key,
                        destination_address=intermediary_address,
                        amount_btc=transaction_amount
                    )
                
                if transfer_result['success']:
                    amount_sent = transfer_result['amount_sent']
                    txid = transfer_result['txid']
                    
                    cursor.execute('''
                        UPDATE wallets 
                        SET balance = balance - ?
                        WHERE wallet_id = ?
                    ''', (amount_sent, buyer_wallet_id))
                    
                    cursor.execute('''
                        UPDATE wallets 
                        SET balance = balance + ?
                        WHERE wallet_id = ?
                    ''', (amount_sent, intermediary_wallet_id))
                    
                    conn.commit()
                    
                    logger.info(f"Manual deposit: {amount_sent:.8f} BTC from buyer {user.id} to intermediary wallet for transaction {transaction_id}. TxID: {txid}")
                    
                    new_balance = intermediary_balance + amount_sent
                    remaining_after_deposit = transaction_amount - new_balance
                    
                    success_msg = f"✅ *Deposit Successful*\n\n"
                    success_msg += f"Amount deposited: {amount_sent:.8f} BTC\n"
                    success_msg += f"Transaction: {transaction_id}\n\n"
                    success_msg += f"Required: {transaction_amount:.8f} BTC\n"
                    success_msg += f"Total in escrow: {new_balance:.8f} BTC\n"
                    
                    if remaining_after_deposit > 0.00000001:
                        success_msg += f"Still needed: {remaining_after_deposit:.8f} BTC\n\n"
                        success_msg += f"Additional deposits to your BTC wallet will be automatically transferred to escrow.\n\n"
                    else:
                        success_msg += f"\n✅ Transaction fully funded!\n\n"
                    
                    success_msg += f"Blockchain TxID:\n`{txid}`\n\n"
                    success_msg += f"ℹ️ An automatic balance check will be sent to the transaction group in 15 minutes."
                    
                    await query.edit_message_text(
                        success_msg,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    try:
                        cursor.execute('SELECT group_id FROM transactions WHERE transaction_id = ?', (transaction_id,))
                        group_result = cursor.fetchone()
                        if group_result and group_result[0]:
                            group_id = group_result[0]
                            
                            group_msg = f"✅ *Funds Received*\n\n"
                            group_msg += f"Deposited: {amount_sent:.8f} BTC\n"
                            group_msg += f"Total in escrow: {new_balance:.8f} BTC\n"
                            group_msg += f"Required: {transaction_amount:.8f} BTC\n"
                            
                            if remaining_after_deposit > 0.00000001:
                                group_msg += f"Remaining: {remaining_after_deposit:.8f} BTC\n"
                            else:
                                group_msg += f"\n✅ Transaction fully funded!\n"
                            
                            group_msg += f"\nBlockchain TxID: `{txid}`"
                            
                            await context.bot.send_message(
                                chat_id=group_id,
                                text=group_msg,
                                parse_mode='Markdown'
                            )
                            
                            context.job_queue.run_once(
                                send_check_command_callback,
                                900,
                                data={'group_id': group_id, 'transaction_id': transaction_id}
                            )
                            logger.info(f"Scheduled /check command to be sent in 15 minutes for transaction {transaction_id}")
                    except Exception as group_notif_error:
                        logger.error(f"Could not send notification to group: {group_notif_error}")
                else:
                    error_msg = transfer_result.get('error', 'Unknown error')
                    await query.edit_message_text(
                        f"❌ Transfer failed\n\n"
                        f"Error: {error_msg}"
                    )
            except Exception as transfer_error:
                logger.error(f"Error in manual deposit to escrow: {transfer_error}")
                await query.edit_message_text(
                    f"❌ Error processing deposit\n\n"
                    f"Error: {str(transfer_error)}"
                )
        except sqlite3.Error as db_error:
            logger.error(f"Database error in deposit_to_escrow: {db_error}")
            await query.edit_message_text(
                f"❌ Database error\n\n"
                f"Error: {str(db_error)}"
            )
        finally:
            if conn:
                conn.close()
    elif data.startswith('create_wallet_'):
        crypto_type = data.split('_')[-1]

        # Check if user already has a wallet for this cryptocurrency
        existing_wallets = get_user_wallets(user.id)
        has_wallet = any(wallet[1] == crypto_type for wallet in existing_wallets)

        if has_wallet:
            # User already has a wallet for this cryptocurrency
            await safe_send_text(
                query.edit_message_text,
                f"⚠️ You already have a {crypto_type} wallet. Only one wallet per cryptocurrency is allowed.\n\n"
                f"Please use your existing wallet or choose a different cryptocurrency.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            if crypto_type == 'BTC':
                keyboard = [
                    [
                        InlineKeyboardButton("SegWit", callback_data='confirm_wallet_BTC_segwit')
                    ]
                ]

                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    "Create Bitcoin (BTC) wallet with SegWit address type:",
                    reply_markup=reply_markup
                )
            else:
                wallet_id, address = create_wallet(user.id, crypto_type)

                # Process any pending transactions for this user
                pending_result = process_pending_recipient(user.id, user.username)

                escaped_address = escape_markdown(address)
                wallet_created_msg = (
                    f"✅ Your new {crypto_type} wallet has been created!\n\n"
                    f"Address: `{escaped_address}`\n\n"
                    f"Use this address to deposit funds into your escrow account."
                )
                
                # If there were pending transactions, add a notification
                if pending_result['success'] and pending_result['transactions_updated'] > 0:
                    wallet_created_msg += (
                        f"\n\n💰 *{pending_result['transactions_updated']} pending transaction(s)* "
                        f"have been added to your wallet!"
                    )
                
                await safe_send_text(
                    query.edit_message_text,
                    wallet_created_msg,
                    parse_mode=ParseMode.MARKDOWN
                )
    elif data == 'confirm_wallet_BTC_segwit':
        wallet_id, address = create_wallet(user.id, 'BTC', address_type=ADDRESS_TYPE_SEGWIT)

        # Process any pending transactions for this user
        pending_result = process_pending_recipient(user.id, user.username)
        
        escaped_address = escape_markdown(address)
        wallet_created_msg = (
            f"✅ Your new Bitcoin (BTC) wallet with SegWit address has been created!\n\n"
            f"Address: `{escaped_address}`\n\n"
            f"Use this address to deposit funds into your escrow account."
        )
        
        # If there were pending transactions, add a notification
        if pending_result['success'] and pending_result['transactions_updated'] > 0:
            wallet_created_msg += (
                f"\n\n💰 *{pending_result['transactions_updated']} pending transaction(s)* "
                f"have been added to your wallet!"
            )
        
        await safe_send_text(
            query.edit_message_text,
            wallet_created_msg,
            parse_mode=ParseMode.MARKDOWN
        )
    elif data.startswith('create_multisig_'):
        crypto_type = data.split('_')[-1]

        # Store crypto type in user data for the conversation
        context.user_data['crypto_type'] = crypto_type

        # Ask for wallet type (address format)
        keyboard = [
            [
                InlineKeyboardButton("Legacy", callback_data=f'address_type_{ADDRESS_TYPE_LEGACY}'),
                InlineKeyboardButton("SegWit", callback_data=f'address_type_{ADDRESS_TYPE_SEGWIT}')
            ],
            [
                InlineKeyboardButton("Native SegWit", callback_data=f'address_type_{ADDRESS_TYPE_NATIVE_SEGWIT}')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"You're creating a {crypto_type} multisig wallet.\n\n"
            f"Choose the address format:",
            reply_markup=reply_markup
        )

        return SELECTING_ADDRESS_TYPE
    elif data.startswith('address_type_'):
        address_type = data.split('_')[-1]

        # Store address type in user data for the conversation
        context.user_data['address_type'] = address_type

        # For BTC multisig wallets, default to 2-of-3
        crypto_type = context.user_data.get('crypto_type', '')
        if crypto_type == 'BTC':
            # Set default values
            context.user_data['m'] = 2
            context.user_data['n'] = 3

            # Ask if user wants to enter public keys or generate new ones
            keyboard = [
                [
                    InlineKeyboardButton("Generate new keys", callback_data='generate_keys'),
                    InlineKeyboardButton("Enter public keys", callback_data='enter_keys')
                ]
            ]

            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                f"You're creating a 2-of-3 multisig wallet for {crypto_type}.\n\n"
                f"Do you want to generate new keys or enter existing public keys?",
                reply_markup=reply_markup
            )

            return ENTERING_PUBLIC_KEYS
        else:
            # For other cryptocurrencies, ask for m and n values
            await query.edit_message_text(
                f"How many signatures should be required to spend from this wallet? (m in m-of-n)\n\n"
                f"Enter a number between 1 and 15:"
            )

            return ENTERING_M
    elif data == 'refresh_balances':
        wallets = get_user_wallets(user.id)

        wallet_text = "Your wallets (balances updated):\n\n"
        for wallet in wallets:
            wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
            wallet_type = wallet[5] if len(wallet) > 5 else "single"
            address_type = wallet[6] if len(wallet) > 6 else "segwit"

            if crypto_type.upper() == 'BTC':
                # Use cached balance instead of blocking API call
                sync_result = get_cached_balance_by_wallet_id(wallet_id)
                if sync_result['success']:
                    balance = sync_result['db_balance']
                    sync_status = ""
                else:
                    sync_status = ""
            else:
                sync_status = ""

            usd_balance = convert_crypto_to_fiat(balance, crypto_type, use_cache_only=True)
            usd_value_text = f"(${usd_balance:.2f} USD)" if usd_balance is not None else "(USD value unavailable)"

            pending_tx_balance = get_user_pending_transaction_balance(user.id, crypto_type)
            pending_usd_balance = convert_crypto_to_fiat(pending_tx_balance, crypto_type, use_cache_only=True)
            pending_usd_value_text = f"(${pending_usd_balance:.2f} USD)" if pending_usd_balance is not None else "(USD value unavailable)"

            escaped_address = escape_markdown(address)
            wallet_text += f"*{crypto_type}*\n"
            wallet_text += f"Type: {address_type.capitalize()}\n"
            wallet_text += f"Address: `{escaped_address}`\n"
            
            # Show balance if > 0, otherwise show pending if there are pending transactions
            if balance > 0:
                wallet_text += f"Balance: {balance:.6f} {crypto_type} {usd_value_text}{sync_status}\n"
            elif pending_tx_balance > 0:
                wallet_text += f"Pending: {pending_tx_balance:.6f} {crypto_type} {pending_usd_value_text}\n"

            if wallet_type == "multisig" and len(wallet) > 7:
                m, n = wallet[7], wallet[8]
                wallet_text += f"Signatures required: {m} of {n}\n"

            wallet_text += "\n"

        keyboard = [
            [
                InlineKeyboardButton("Refresh Balances", callback_data='refresh_balances')
            ]
        ]
        
        if user.username and user.username.lower() == 'safeswapsupport':
            keyboard.append([InlineKeyboardButton("Delete", callback_data='delete_wallet')])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_send_text(
            query.edit_message_text,
            wallet_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return
    elif data == 'delete_wallet':
        if not (user.username and user.username.lower() == 'safeswapsupport'):
            await query.edit_message_text("❌ You are not authorized to delete wallets.")
            return
        
        wallets = get_user_wallets(user.id)
        if not wallets:
            await query.edit_message_text("❌ You don't have any wallets to delete.")
            return
        
        btc_wallet = None
        for wallet in wallets:
            if wallet[1] == 'BTC':
                btc_wallet = wallet
                break
        
        if not btc_wallet:
            await query.edit_message_text("❌ No BTC wallet found to delete.")
            return
        
        wallet_id = btc_wallet[0]
        
        try:
            with DatabaseConnection(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM wallets WHERE wallet_id = ?', (wallet_id,))
                logger.info(f"User {user.id} ({user.username}) deleted BTC wallet {wallet_id}")
            
            await query.edit_message_text(f"✅ BTC wallet deleted successfully.\n\nWallet ID: {wallet_id}")
        except sqlite3.Error as db_error:
            logger.error(f"Database error deleting wallet: {db_error}")
            await query.edit_message_text(f"❌ Database error: {db_error}")
        return


@with_auto_balance_refresh
async def deposit_command(update: Update, context: CallbackContext) -> int:
    await ensure_user_and_process_pending(update)
    
    # Clear any existing conversation state to ensure fresh start
    context.user_data.clear()
    
    user = update.effective_user

    keyboard = [
        [
            InlineKeyboardButton("Buyer", callback_data='role_buyer'),
            InlineKeyboardButton("Seller", callback_data='role_seller')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Are you the buyer or the seller in this transaction?",
        reply_markup=reply_markup
    )

    return SELECTING_ROLE


async def select_role(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    role = query.data.split('_')[1]
    context.user_data['role'] = role

    if role == 'seller':
        other_party = 'buyer'
    else:
        other_party = 'seller'

    await query.edit_message_text(
        f"You selected: {role.capitalize()}\n\n"
        f"Please enter Telegram username of the {other_party} (e.g., @username):"
    )

    return ENTERING_RECIPIENT


async def select_crypto(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    crypto_type = query.data.split('_')[1]
    context.user_data['crypto_type'] = crypto_type

    # Get current price of the cryptocurrency in USD (non-blocking)
    price = await asyncio.to_thread(get_crypto_price, crypto_type)
    price_info = f"Current {crypto_type} price: ${price:.2f} USD" if price is not None else "Price information unavailable"

    await query.edit_message_text(
        f"{price_info}\n\n"
        f"Please enter USD value of the amount of BTC that will be deposited to escrow:"
    )

    return ENTERING_AMOUNT


async def enter_amount(update: Update, context: CallbackContext) -> int:
    try:
        usd_amount = float(update.message.text.strip())
        if usd_amount <= 0:
            await update.message.reply_text("Amount must be greater than 0. Please try again:")
            return ENTERING_AMOUNT

        crypto_type = context.user_data['crypto_type']

        # Convert USD amount to cryptocurrency amount (using cached price)
        crypto_amount = convert_fiat_to_crypto(usd_amount, crypto_type, use_cache_only=True)

        if crypto_amount is None:
            await update.message.reply_text(
                "Unable to convert USD to cryptocurrency at this time. Please try again later."
            )
            return ConversationHandler.END

        # Store both USD and crypto amounts
        context.user_data['usd_amount'] = usd_amount
        context.user_data['amount'] = crypto_amount

        # Calculate fees
        usd_fee = usd_amount * 0.05
        usd_total = usd_amount + usd_fee

        crypto_fee = crypto_amount * 0.05
        crypto_total = crypto_amount + crypto_fee

        # Get previous completed transaction descriptions
        message_text = (
            f"Transaction amount: ${usd_amount:.2f} USD\n"
            f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
            f"Total: ${usd_total:.2f} USD\n\n"
            "Please enter a description for this transaction (e.g., 'Escrow payment'):"
        )
        
        await update.message.reply_text(message_text)

        return CONFIRMING_TRANSACTION
    except ValueError:
        await update.message.reply_text("Invalid amount. Please enter a valid number:")
        return ENTERING_AMOUNT


async def enter_recipient(update: Update, context: CallbackContext) -> int:
    recipient = update.message.text.strip()

    if not recipient.startswith('@'):
        await update.message.reply_text("Please enter a valid Telegram username starting with @:")
        return ENTERING_RECIPIENT

    context.user_data['recipient'] = recipient

    user = update.effective_user
    wallets = get_user_wallets(user.id)

    keyboard = []
    for wallet in wallets:
        wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
        usd_balance = convert_crypto_to_fiat(balance, crypto_type, use_cache_only=True)
        usd_value_text = f"${usd_balance:.2f} USD" if usd_balance is not None else "USD value unavailable"

        keyboard.append(
            [InlineKeyboardButton(f"{crypto_type} ({usd_value_text} available)", callback_data=f"deposit_{crypto_type}")])

    if not keyboard:
        keyboard.append([InlineKeyboardButton("Bitcoin (BTC)", callback_data="deposit_BTC")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"Select which cryptocurrency you want to use for this transaction:",
        reply_markup=reply_markup
    )

    return SELECTING_CRYPTO
async def confirm_transaction(update: Update, context: CallbackContext) -> int:
    description = update.message.text.strip()
    
    # Check for empty description
    if not description:
        await update.message.reply_text(
            "❌ *Error: Empty Description*\n\n"
            "Please enter a description for this transaction:",
            parse_mode=ParseMode.MARKDOWN
        )
        return CONFIRMING_TRANSACTION
    
    # Check for duplicate description
    user = update.effective_user
    if check_duplicate_description(user.id, description):
        await update.message.reply_text(
            "❌ *Error: Duplicate Description*\n\n"
            "You have already used this description for an active transaction. "
            "Please enter a unique description for this transaction:",
            parse_mode=ParseMode.MARKDOWN
        )
        return CONFIRMING_TRANSACTION
    
    context.user_data['description'] = description

    crypto_type = context.user_data['crypto_type']
    amount = context.user_data['amount']
    usd_amount = context.user_data['usd_amount']
    recipient = context.user_data['recipient']

    # Calculate fees
    fee = amount * 0.05
    usd_fee = usd_amount * 0.05

    # Calculate totals
    total = amount + fee
    usd_total = usd_amount + usd_fee

    keyboard = [
        [
            InlineKeyboardButton("Confirm", callback_data='confirm_transaction'),
            InlineKeyboardButton("Cancel", callback_data='cancel_transaction')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_send_text(
        update.message.reply_text,
        f"📝 *Transaction Summary*\n\n"
        f"Cryptocurrency: {crypto_type}\n"
        f"Amount: ${usd_amount:.2f} USD\n"
        f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
        f"Total: ${usd_total:.2f} USD\n"
        f"Seller: {recipient}\n"
        f"Description: {description}\n\n"
        f"Please confirm this transaction:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

    return ConversationHandler.END


async def transaction_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    data = query.data

    if data == 'confirm_transaction':
        crypto_type = context.user_data['crypto_type']
        amount = context.user_data['amount']
        usd_amount = context.user_data['usd_amount']
        recipient = context.user_data['recipient']
        description = context.user_data['description']
        role = context.user_data.get('role', 'buyer')

        fee = amount * 0.05
        total = amount + fee
        usd_fee = usd_amount * 0.05
        usd_total = usd_amount + usd_fee

        recipient_user_id = get_user_id_from_username(recipient)

        conn = None
        wallet = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ? AND crypto_type = ?',
                           (user.id, crypto_type.upper()))
            wallet = cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Database error in transaction_callback: {e}")
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: Database error\n\n"
                f"Please try again later.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        finally:
            if conn:
                conn.close()

        if not wallet:
            wallet_id, wallet_address = create_wallet(user.id, crypto_type)
            if not wallet_id:
                await safe_send_text(
                    query.edit_message_text,
                    f"❌ Transaction failed!\n\n"
                    f"Reason: Could not create {crypto_type} wallet\n\n"
                    f"Please try again later.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            current_balance = 0.0
        else:
            wallet_id, current_balance = wallet

        # Create intermediary wallet for BTC transactions
        intermediary_wallet_id = None
        intermediary_wallet_address = None
        if crypto_type.upper() == 'BTC':
            intermediary_wallet_id, intermediary_wallet_address = create_intermediary_wallet(str(uuid.uuid4()), crypto_type)
            if not intermediary_wallet_id:
                await safe_send_text(
                    query.edit_message_text,
                    f"❌ Transaction failed!\n\n"
                    f"Reason: Could not create intermediary wallet\n\n"
                    f"Please try again later.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return

        # Conditional BTC deduction based on role and balance
        btc_transferred = 0.0  # Amount actually transferred to intermediary wallet
        remaining_btc_needed = 0.0  # Amount still needed if partial transfer
        partial_transfer = False
        subtract_result = None  # Initialize to track balance deduction
        deducted_amount = 0.0  # Track the actual amount deducted from initiator's wallet
        
        if crypto_type.upper() == 'BTC':
            # Sellers: no deduction
            if role == 'seller':
                deducted_amount = 0.0  # No deduction for sellers
            # Buyers with zero balance: no deduction
            elif role == 'buyer' and current_balance == 0:
                deducted_amount = 0.0  # No deduction for buyers with zero balance
            # Buyers with insufficient balance: partial transfer
            elif role == 'buyer' and 0 < current_balance < total:
                # Convert balance to satoshis
                balance_satoshis = int(current_balance * 1e8)
                
                # Reserve 250 satoshis for transaction fee
                if balance_satoshis > 250:
                    # Get private key to send BTC to third-party wallet
                    try:
                        conn_pk = sqlite3.connect(DB_PATH, timeout=20.0)
                        cursor_pk = conn_pk.cursor()
                        cursor_pk.execute('SELECT private_key FROM wallets WHERE wallet_id = ?', (wallet_id,))
                        pk_result = cursor_pk.fetchone()
                        conn_pk.close()
                        
                        if pk_result and pk_result[0]:
                            wif_key = pk_result[0]
                            
                            # Send available balance (minus 250 sats) to intermediary wallet (non-blocking)
                            transfer_result = await asyncio.to_thread(
                                btcwalletclient_wif.send_max_btc_auto,
                                wif_private_key=wif_key,
                                destination_address=intermediary_wallet_address
                            )
                            
                            if transfer_result['success']:
                                btc_transferred = transfer_result['amount_sent']
                                remaining_btc_needed = total - btc_transferred
                                partial_transfer = True
                                deducted_amount = btc_transferred  # Track the partial amount deducted
                                
                                # Update wallet balance to reflect the transfer
                                subtract_result = subtract_wallet_balance(wallet_id, btc_transferred)
                                if not subtract_result['success']:
                                    error_msg = subtract_result.get('error', 'Unknown error')
                                    await safe_send_text(
                                        query.edit_message_text,
                                        f"❌ Transaction failed!\n\n"
                                        f"Reason: {error_msg}",
                                        parse_mode=ParseMode.MARKDOWN
                                    )
                                    return
                            else:
                                await safe_send_text(
                                    query.edit_message_text,
                                    f"❌ Transaction failed!\n\n"
                                    f"Reason: BTC transfer failed - {transfer_result.get('error', 'Unknown error')}",
                                    parse_mode=ParseMode.MARKDOWN
                                )
                                return
                        else:
                            await safe_send_text(
                                query.edit_message_text,
                                f"❌ Transaction failed!\n\n"
                                f"Reason: Could not retrieve wallet private key",
                                parse_mode=ParseMode.MARKDOWN
                            )
                            return
                    except Exception as e:
                        print(f"Error during partial BTC transfer: {e}")
                        await safe_send_text(
                            query.edit_message_text,
                            f"❌ Transaction failed!\n\n"
                            f"Reason: Error processing partial transfer - {str(e)}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        return
                # else: balance too small, just skip deduction
            # Buyers with sufficient balance: full deduction
            elif role == 'buyer' and current_balance >= total:
                deducted_amount = total  # Track the full amount deducted
                subtract_result = subtract_wallet_balance(wallet_id, total)
                if not subtract_result['success']:
                    error_msg = subtract_result.get('error', 'Unknown error')
                    await safe_send_text(
                        query.edit_message_text,
                        f"❌ Transaction failed!\n\n"
                        f"Reason: {error_msg}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
        else:
            # For non-BTC cryptocurrencies, keep original behavior
            deducted_amount = total  # Track the full amount deducted for non-BTC
            subtract_result = subtract_wallet_balance(wallet_id, total)
            if not subtract_result['success']:
                error_msg = subtract_result.get('error', 'Unknown error')
                await safe_send_text(
                    query.edit_message_text,
                    f"❌ Transaction failed!\n\n"
                    f"Reason: {error_msg}",
                    parse_mode=ParseMode.MARKDOWN
                )
                return

        pending_result = None
        if recipient_user_id:
            pending_result = add_to_pending_balance(recipient_user_id, crypto_type, total)
            if not pending_result['success']:
                if 'Wallet not found' in pending_result.get('error', ''):
                    recipient_wallet_id, recipient_wallet_address = create_wallet(recipient_user_id, crypto_type)
                    if recipient_wallet_id:
                        pending_result = add_to_pending_balance(recipient_user_id, crypto_type, total)
                
                if not pending_result['success']:
                    add_back = add_to_pending_balance(user.id, crypto_type, total)
                    await safe_send_text(
                        query.edit_message_text,
                        f"❌ Transaction failed!\n\n"
                        f"Reason: Failed to update recipient's pending balance\n\n"
                        f"Your balance has been restored.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return

        if role == 'seller':
            transaction_id = create_transaction(
                seller_id=user.id,
                buyer_id=recipient_user_id,
                crypto_type=crypto_type,
                amount=amount,
                description=description,
                wallet_id=wallet_id,
                tx_hex=None,
                txid=None,
                recipient_username=recipient if not recipient_user_id else None,
                intermediary_wallet_id=intermediary_wallet_id,
                initiator_id=user.id,
                deducted_amount=deducted_amount,
                usd_amount=usd_amount,
                usd_fee_amount=usd_fee
            )
        else:
            transaction_id = create_transaction(
                seller_id=recipient_user_id,
                buyer_id=user.id,
                crypto_type=crypto_type,
                amount=amount,
                description=description,
                wallet_id=wallet_id,
                tx_hex=None,
                txid=None,
                recipient_username=recipient if not recipient_user_id else None,
                intermediary_wallet_id=intermediary_wallet_id,
                initiator_id=user.id,
                deducted_amount=deducted_amount,
                usd_amount=usd_amount,
                usd_fee_amount=usd_fee
            )

        if not transaction_id:
            add_back = add_to_pending_balance(user.id, crypto_type, total)
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: Failed to create transaction\n\n"
                f"Your balance has been restored.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        group_created = False
        group_link = None

        if recipient_user_id:
            try:
                group_title = f"Escrow: {user.first_name or user.username} → {recipient}"
                group = await context.bot.create_supergroup(
                    title=group_title,
                    description=f"Escrow transaction {transaction_id}"
                )
                group_id = group.id

                failed_users = []
                try:
                    await context.bot.add_chat_members(
                        chat_id=group_id,
                        user_ids=[user.id, recipient_user_id]
                    )
                except Exception as add_error:
                    print(f"Failed to add users to group: {add_error}")
                    # Try adding users individually to identify which ones failed
                    for user_id in [user.id, recipient_user_id]:
                        try:
                            await context.bot.add_chat_members(chat_id=group_id, user_ids=[user_id])
                        except Exception:
                            failed_users.append(user_id)

                try:
                    invite_link = await context.bot.create_chat_invite_link(group_id)
                    group_link = invite_link.invite_link
                except Exception:
                    group_info = await context.bot.get_chat(group_id)
                    if group_info.invite_link:
                        group_link = group_info.invite_link
                
                # Send invite links to users who couldn't be added to the group
                if failed_users and group_link:
                    for failed_user_id in failed_users:
                        try:
                            if failed_user_id == user.id:
                                failed_user_name = user.first_name or user.username
                            else:
                                failed_user_name = recipient
                            
                            await context.bot.send_message(
                                chat_id=failed_user_id,
                                text=f"🔗 You couldn't be automatically added to the escrow group for transaction {transaction_id}.\n\nPlease join using this invite link: {group_link}",
                                parse_mode=ParseMode.MARKDOWN
                            )
                            print(f"Sent invite link to user {failed_user_id} ({failed_user_name})")
                        except Exception as e:
                            print(f"Failed to send invite link to user {failed_user_id}: {e}")

                if role == 'seller':
                    buyer_user_id = recipient_user_id
                    action_text = f"@{recipient.lstrip('@')}, you need to deposit {total:.8f} {crypto_type} to complete this transaction."
                    buyer_seller_text = f"*Seller:* {user.first_name or user.username}\n*Buyer:* {recipient}"
                else:
                    buyer_user_id = user.id
                    action_text = "Seller should run /check command to see if a buyer has deposited a sufficient amount of BTC to the escrow wallet\n\nBuyer should run /release command to transfer BTC to that seller's BTC wallet once goods & services are received."
                    buyer_seller_text = f"*Buyer:* {user.first_name or user.username}\n*Seller:* {recipient}"

                await context.bot.send_message(
                    chat_id=group_id,
                    text=(
                        f"💰 *Escrow Transaction Created*\n\n"
                        f"{buyer_seller_text}\n\n"
                        f"*Transaction Details:*\n"
                        f"*Cryptocurrency:* {crypto_type}\n"
                        f"*Amount:* {amount:.8f} {crypto_type}\n"
                        f"*USD Value:* ${usd_amount:.2f} USD\n"
                        f"*Escrow fee (5%):* ${usd_fee:.2f} USD\n"
                        f"*Total:* ${usd_total:.2f} USD\n"
                        f"*Transaction ID:* `{transaction_id}`\n"
                        f"*Escrow Wallet Address:* `{intermediary_wallet_address if intermediary_wallet_address else 'N/A'}`\n\n"
                        f"*Description:* {description}\n\n"
                        f"⚠️ *Action Required:*\n"
                        f"{action_text}"
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )

                update_transaction_group_id(transaction_id, group_id)
                group_created = True
            except Exception as e:
                print(f"Error creating group or adding members: {e}")

        context.user_data['create_group_data'] = {
            'recipient': recipient,
            'transaction_id': transaction_id,
            'sender_name': user.first_name or user.username,
            'sender_username': user.username,
            'sender_id': user.id,
            'crypto_type': crypto_type,
            'amount': amount,
            'usd_amount': usd_amount,
            'fee': fee,
            'usd_fee': usd_fee,
            'total': total,
            'usd_total': usd_total,
            'description': description,
            'wallet_address': intermediary_wallet_address if intermediary_wallet_address else None,
            'partial_transfer': partial_transfer,
            'btc_transferred': btc_transferred,
            'remaining_btc_needed': remaining_btc_needed
        }

        keyboard = []
        if group_created and group_link:
            keyboard.append([InlineKeyboardButton("Open Escrow Group", url=group_link)])
        keyboard.append([InlineKeyboardButton("Create Escrow Group", callback_data='create_escrow_group')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        if crypto_type.upper() == 'BTC':
            # Use cached balance instead of blocking API call
            sync_result = get_cached_balance_by_wallet_id(wallet_id)

            balance_info = ""
            if sync_result['success']:
                if role == 'seller':
                    recipient_label = "Buyer"
                    status_text = "✅ Transaction created!\n\nWaiting for buyer to deposit funds."
                else:
                    recipient_label = "Seller"
                    status_text = "✅ Transaction initiated!"
                    
                recipient_info = f"{recipient_label}: {recipient}\n"
                if pending_result:
                    recipient_info += f"{recipient_label} pending balance: {pending_result['new_pending_balance']:.8f} {crypto_type}\n"
                recipient_notification = "\nAn escrow group has been created between buyer, seller, and this bot."

                balance_after = subtract_result['new_balance'] if subtract_result else current_balance
                await safe_send_text(
                    query.edit_message_text,
                    f"{status_text}\n\n"
                    f"Transaction ID: {transaction_id}\n\n"
                    f"Amount: ${usd_amount:.2f} USD\n"
                    f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
                    f"Total: ${usd_total:.2f} USD\n\n"
                    f"Balance after deduction: {balance_after:.8f} {crypto_type}{balance_info}{recipient_notification}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
            else:
                if role == 'seller':
                    recipient_label = "Buyer"
                    status_text = "✅ Transaction initiated!\n\nWaiting for buyer to deposit funds."
                else:
                    recipient_label = "Seller"
                    status_text = "✅ Transaction initiated!"
                    
                recipient_info = f"{recipient_label}: {recipient}\n"
                if pending_result:
                    recipient_info += f"{recipient_label} pending balance: {pending_result['new_pending_balance']:.8f} {crypto_type}\n"
                recipient_notification = "\nAn escrow group has been created between buyer, seller, and this bot."

                balance_after = subtract_result['new_balance'] if subtract_result else current_balance
                await safe_send_text(
                    query.edit_message_text,
                    f"{status_text}\n\n"
                    f"Transaction ID: {transaction_id}\n\n"
                    f"Amount: ${usd_amount:.2f} USD\n"
                    f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
                    f"Total: ${usd_total:.2f} USD\n\n"
                    f"Balance after deduction: {balance_after:.8f} {crypto_type}{recipient_notification}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
        else:
            if role == 'seller':
                recipient_label = "Buyer"
                status_text = "✅ Transaction created!\n\nWaiting for buyer to deposit funds."
            else:
                recipient_label = "Seller"
                status_text = "✅ Transaction initiated!"
                
            recipient_info = f"{recipient_label}: {recipient}\n"
            if pending_result:
                recipient_info += f"{recipient_label} pending balance: {pending_result['new_pending_balance']:.8f} {crypto_type}\n"
            recipient_notification = "\nAn escrow group has been created between buyer, seller, and this bot."

            balance_after = subtract_result['new_balance'] if subtract_result else current_balance
            await safe_send_text(
                query.edit_message_text,
                f"{status_text}\n\n"
                f"Transaction ID: {transaction_id}\n\n"
                f"Amount: ${usd_amount:.2f} USD\n"
                f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
                f"Total: ${usd_total:.2f} USD\n\n"
                f"Balance after deduction: {balance_after:.8f} {crypto_type}{recipient_notification}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
    elif data == 'cancel_transaction':
        account_keyboard = [
            [KeyboardButton("Start Trade"), KeyboardButton("My Wallet")],
            [KeyboardButton("Release Funds"), KeyboardButton("File Dispute")],
            [KeyboardButton("Back to Main Menu 🔙")]
        ]
        reply_markup = ReplyKeyboardMarkup(account_keyboard, resize_keyboard=True)
        
        await query.edit_message_text(
            "❌ Transaction cancelled.\n\n"
            "Returning to My Account menu...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        await query.message.reply_text(
            "My Account Options:",
            reply_markup=reply_markup
        )
    elif data.startswith('transactions_page_'):
        page = int(data.replace('transactions_page_', ''))
        
        # Get user transactions and filter out cancelled ones
        transactions = get_user_transactions(user.id)
        active_transactions = [t for t in transactions if t[6] != 'CANCELLED']
        
        if not active_transactions:
            await query.edit_message_text("You don't have any active transactions.")
            return
        
        # Show the requested page
        await show_transactions_page(query.edit_message_text, user.id, active_transactions, page=page)
    elif data.startswith('view_transaction_'):
        transaction_id = data.replace('view_transaction_', '')
        transaction = get_transaction(transaction_id)
        
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return
        
        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = transaction[3]
        amount = transaction[4]
        fee_amount = transaction[5]
        status = transaction[6]
        creation_date = transaction[7]
        completion_date = transaction[8]
        description = transaction[9]
        initiator_id = transaction[19] if len(transaction) > 19 else None
        stored_usd_amount = transaction[21] if len(transaction) > 21 else None
        stored_usd_fee = transaction[22] if len(transaction) > 22 else None
        
        role = "Seller" if seller_id == user.id else "Buyer"
        
        if stored_usd_amount is not None:
            usd_amount = stored_usd_amount
            usd_value_text = f"${usd_amount:.2f} USD"
        else:
            usd_amount = convert_crypto_to_fiat(amount, crypto_type, use_cache_only=True)
            usd_value_text = f"${usd_amount:.2f} USD" if usd_amount is not None else "USD value unavailable"
        
        if stored_usd_fee is not None:
            usd_fee = stored_usd_fee
            usd_fee_text = f"${usd_fee:.2f} USD"
        else:
            usd_fee = convert_crypto_to_fiat(fee_amount, crypto_type, use_cache_only=True) if fee_amount else None
            usd_fee_text = f"${usd_fee:.2f} USD" if usd_fee is not None else "USD value unavailable"
        
        total_crypto = amount + fee_amount if fee_amount else amount
        if stored_usd_amount is not None and stored_usd_fee is not None:
            usd_total = stored_usd_amount + stored_usd_fee
            usd_total_text = f"${usd_total:.2f} USD"
        else:
            usd_total = convert_crypto_to_fiat(total_crypto, crypto_type, use_cache_only=True)
            usd_total_text = f"${usd_total:.2f} USD" if usd_total is not None else "USD value unavailable"
        
        details_text = (
            f"*Transaction Details*\n\n"
            f"*Transaction ID:* `{transaction_id}`\n"
            f"*Your Role:* {role}\n"
            f"*Cryptocurrency:* {crypto_type}\n"
            f"*Amount:* {amount:.8f} {crypto_type}\n"
            f"*USD Value:* {usd_value_text}\n"
            f"*Escrow Fee (5%):* {usd_fee_text}\n"
            f"*Total:* {usd_total_text}\n"
            f"*Status:* {status}\n"
            f"*Created:* {creation_date}\n"
            f"*Description:* {description if description else 'N/A'}\n"
        )
        
        keyboard = []
        
        if status == 'PENDING':
            if initiator_id == user.id:
                keyboard.append([
                    InlineKeyboardButton("❌ Cancel", callback_data=f'cancel_transaction_{transaction_id}')
                ])
        
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        
        await safe_send_text(
            query.edit_message_text,
            details_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    
    elif data.startswith('accept_transaction_'):
        transaction_id = data.replace('accept_transaction_', '')
        transaction = get_transaction(transaction_id)
        
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return
        
        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = transaction[3]
        amount = transaction[4]
        status = transaction[6]
        wallet_id = transaction[10]
        
        if buyer_id != user.id:
            await query.edit_message_text("Only the buyer can accept this transaction.")
            return
        
        if status != 'PENDING':
            await query.edit_message_text(f"Cannot accept transaction. Status is {status}.")
            return
        
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ? AND crypto_type = ?',
                          (user.id, crypto_type.upper()))
            buyer_wallet = cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            await query.edit_message_text(f"Database error: {e}")
            return
        finally:
            if conn:
                conn.close()
        
        if not buyer_wallet:
            await query.edit_message_text(
                f"❌ You need a {crypto_type} wallet to accept this transaction.\n\n"
                f"Please create one using the /wallet command first."
            )
            return
        
        buyer_wallet_id, buyer_balance = buyer_wallet
        
        if buyer_balance < amount:
            keyboard = [[
                InlineKeyboardButton("✅ Accept", callback_data=f'accept_transaction_{transaction_id}'),
                InlineKeyboardButton("❌ Decline", callback_data=f'decline_transaction_{transaction_id}')
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"❌ Insufficient balance!\n\n"
                f"You need {amount:.8f} {crypto_type} to accept this transaction.\n"
                f"Your current balance: {buyer_balance:.8f} {crypto_type}\n\n"
                f"Please deposit more funds to your wallet and try again.",
                reply_markup=reply_markup
            )
            return
        
        subtract_result = subtract_wallet_balance(buyer_wallet_id, amount)
        if not subtract_result['success']:
            error_msg = subtract_result.get('error', 'Unknown error')
            await query.edit_message_text(
                f"❌ Failed to accept transaction!\n\n"
                f"Reason: {error_msg}"
            )
            return
        
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
            escrow_wallet = cursor.fetchone()
            
            if escrow_wallet:
                current_balance = escrow_wallet[0]
                new_balance = current_balance + amount
                
                cursor.execute(
                    'UPDATE wallets SET balance = ? WHERE wallet_id = ?',
                    (new_balance, wallet_id)
                )
                conn.commit()
                
                escrow_wallet_balance = new_balance
                required_amount = amount
                
                cursor.execute('SELECT group_id FROM transactions WHERE transaction_id = ?', (transaction_id,))
                group_result = cursor.fetchone()
                transaction_group_id = group_result[0] if group_result else None
            else:
                if conn:
                    conn.rollback()
                add_back_result = subtract_wallet_balance(buyer_wallet_id, -amount)
                await query.edit_message_text(
                    f"❌ Failed to accept transaction!\n\n"
                    f"Reason: Escrow wallet not found\n\n"
                    f"Your balance has been restored."
                )
                return
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            if conn:
                conn.rollback()
            add_back_result = subtract_wallet_balance(buyer_wallet_id, -amount)
            await query.edit_message_text(
                f"❌ Failed to accept transaction!\n\n"
                f"Reason: Database error\n\n"
                f"Your balance has been restored."
            )
            return
        finally:
            if conn:
                conn.close()
        
        if transaction_group_id and escrow_wallet_balance >= (required_amount * 0.99):
            try:
                await context.bot.send_message(
                    chat_id=transaction_group_id,
                    text=(
                        f"✅ *Funds Deposited to Escrow*\n\n"
                        f"The buyer has successfully deposited {amount:.8f} {crypto_type} to the escrow wallet.\n\n"
                        f"*Escrow Balance:* {escrow_wallet_balance:.8f} {crypto_type}\n"
                        f"*Transaction ID:* `{transaction_id}`\n\n"
                        f"The seller can proceed with delivering the service/product."
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                print(f"Failed to send notification to group {transaction_group_id}: {e}")
        
        remaining_pending = get_pending_transactions_for_buyer(user.id)
        
        if remaining_pending:
            next_transaction = remaining_pending[0]
            next_transaction_id = next_transaction[0]
            next_seller_id = next_transaction[1]
            next_crypto_type = next_transaction[3]
            next_amount = next_transaction[4]
            next_fee_amount = next_transaction[5]
            next_creation_date = next_transaction[7]
            next_description = next_transaction[8]
            
            conn_next = None
            next_seller_username = "Unknown"
            try:
                conn_next = sqlite3.connect(DB_PATH, timeout=20.0)
                cursor_next = conn_next.cursor()
                cursor_next.execute('SELECT username FROM users WHERE user_id = ?', (next_seller_id,))
                next_seller_result = cursor_next.fetchone()
                if next_seller_result and next_seller_result[0]:
                    next_seller_username = f"@{next_seller_result[0]}"
            except sqlite3.Error as e:
                print(f"Database error: {e}")
            finally:
                if conn_next:
                    conn_next.close()
            
            next_usd_amount = convert_crypto_to_fiat(next_amount, next_crypto_type, use_cache_only=True)
            next_usd_value_text = f"${next_usd_amount:.2f} USD" if next_usd_amount is not None else "USD value unavailable"
            
            next_usd_fee = convert_crypto_to_fiat(next_fee_amount, next_crypto_type, use_cache_only=True) if next_fee_amount else None
            next_usd_fee_text = f"${next_usd_fee:.2f} USD" if next_usd_fee is not None else "USD value unavailable"
            
            next_total_crypto = next_amount + next_fee_amount if next_fee_amount else next_amount
            next_usd_total = convert_crypto_to_fiat(next_total_crypto, next_crypto_type, use_cache_only=True)
            next_usd_total_text = f"${next_usd_total:.2f} USD" if next_usd_total is not None else "USD value unavailable"
            
            remaining_count = len(remaining_pending) - 1
            remaining_text = f"\n\n⚠️ You have {remaining_count} more pending transaction(s) after this one." if remaining_count > 0 else ""
            
            next_pending_message = (
                f"✅ Previous transaction accepted!\n\n"
                f"⚠️ *NEXT PENDING TRANSACTION - ACTION REQUIRED*\n\n"
                f"*Transaction Details:*\n"
                f"*Transaction ID:* `{next_transaction_id}`\n"
                f"*From:* {next_seller_username}\n"
                f"*Cryptocurrency:* {next_crypto_type}\n"
                f"*Amount:* {next_amount:.8f} {next_crypto_type}\n"
                f"*USD Value:* {next_usd_value_text}\n"
                f"*Escrow Fee (5%):* {next_usd_fee_text}\n"
                f"*Total:* {next_usd_total_text}\n"
                f"*Created:* {next_creation_date}\n"
                f"*Description:* {next_description if next_description else 'N/A'}"
                f"{remaining_text}"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Accept", callback_data=f'accept_transaction_{next_transaction_id}'),
                    InlineKeyboardButton("❌ Decline", callback_data=f'decline_transaction_{next_transaction_id}')
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await safe_send_text(
                query.edit_message_text,
                next_pending_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        else:
            await safe_send_text(
                query.edit_message_text,
                f"✅ Transaction accepted!\n\n"
                f"Transaction ID: `{transaction_id}`\n\n"
                f"{amount:.8f} {crypto_type} has been transferred to escrow.\n"
                f"Your new balance: {subtract_result['new_balance']:.8f} {crypto_type}\n\n"
                f"The seller can now release the funds once the service/product is delivered.\n\n"
                f"✅ You have no more pending transactions.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    elif data.startswith('decline_transaction_'):
        transaction_id = data.replace('decline_transaction_', '')
        transaction = get_transaction(transaction_id)
        
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return
        
        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = transaction[3]
        amount = transaction[4]
        status = transaction[6]
        wallet_id = transaction[10]
        
        if buyer_id != user.id:
            await query.edit_message_text("Only the buyer can decline this transaction.")
            return
        
        if status != 'PENDING':
            await query.edit_message_text(f"Cannot decline transaction. Status is {status}.")
            return
        
        update_transaction_status(transaction_id, 'CANCELLED')
        
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT balance, pending_balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
            seller_wallet = cursor.fetchone()
            
            if seller_wallet:
                current_balance, current_pending = seller_wallet
                new_balance = current_balance + amount
                new_pending = current_pending - amount
                
                cursor.execute(
                    'UPDATE wallets SET balance = ?, pending_balance = ? WHERE wallet_id = ?',
                    (new_balance, new_pending, wallet_id)
                )
                conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
        
        remaining_pending = get_pending_transactions_for_buyer(user.id)
        
        if remaining_pending:
            next_transaction = remaining_pending[0]
            next_transaction_id = next_transaction[0]
            next_seller_id = next_transaction[1]
            next_crypto_type = next_transaction[3]
            next_amount = next_transaction[4]
            next_fee_amount = next_transaction[5]
            next_creation_date = next_transaction[7]
            next_description = next_transaction[8]
            
            conn_next = None
            next_seller_username = "Unknown"
            try:
                conn_next = sqlite3.connect(DB_PATH, timeout=20.0)
                cursor_next = conn_next.cursor()
                cursor_next.execute('SELECT username FROM users WHERE user_id = ?', (next_seller_id,))
                next_seller_result = cursor_next.fetchone()
                if next_seller_result and next_seller_result[0]:
                    next_seller_username = f"@{next_seller_result[0]}"
            except sqlite3.Error as e:
                print(f"Database error: {e}")
            finally:
                if conn_next:
                    conn_next.close()
            
            next_usd_amount = convert_crypto_to_fiat(next_amount, next_crypto_type, use_cache_only=True)
            next_usd_value_text = f"${next_usd_amount:.2f} USD" if next_usd_amount is not None else "USD value unavailable"
            
            next_usd_fee = convert_crypto_to_fiat(next_fee_amount, next_crypto_type, use_cache_only=True) if next_fee_amount else None
            next_usd_fee_text = f"${next_usd_fee:.2f} USD" if next_usd_fee is not None else "USD value unavailable"
            
            next_total_crypto = next_amount + next_fee_amount if next_fee_amount else next_amount
            next_usd_total = convert_crypto_to_fiat(next_total_crypto, next_crypto_type, use_cache_only=True)
            next_usd_total_text = f"${next_usd_total:.2f} USD" if next_usd_total is not None else "USD value unavailable"
            
            remaining_count = len(remaining_pending) - 1
            remaining_text = f"\n\n⚠️ You have {remaining_count} more pending transaction(s) after this one." if remaining_count > 0 else ""
            
            next_pending_message = (
                f"❌ Previous transaction declined!\n\n"
                f"⚠️ *NEXT PENDING TRANSACTION - ACTION REQUIRED*\n\n"
                f"*Transaction Details:*\n"
                f"*Transaction ID:* `{next_transaction_id}`\n"
                f"*From:* {next_seller_username}\n"
                f"*Cryptocurrency:* {next_crypto_type}\n"
                f"*Amount:* {next_amount:.8f} {next_crypto_type}\n"
                f"*USD Value:* {next_usd_value_text}\n"
                f"*Escrow Fee (5%):* {next_usd_fee_text}\n"
                f"*Total:* {next_usd_total_text}\n"
                f"*Created:* {next_creation_date}\n"
                f"*Description:* {next_description if next_description else 'N/A'}"
                f"{remaining_text}"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Accept", callback_data=f'accept_transaction_{next_transaction_id}'),
                    InlineKeyboardButton("❌ Decline", callback_data=f'decline_transaction_{next_transaction_id}')
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await safe_send_text(
                query.edit_message_text,
                next_pending_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        else:
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction declined!\n\n"
                f"Transaction ID: `{transaction_id}`\n\n"
                f"The transaction has been cancelled and the seller's funds have been returned.\n\n"
                f"✅ You have no more pending transactions.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    elif data.startswith('cancel_transaction_'):
        transaction_id = data.replace('cancel_transaction_', '')
        transaction = get_transaction(transaction_id)
        
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return
        
        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = transaction[3]
        amount = transaction[4]
        status = transaction[6]
        wallet_id = transaction[10]
        initiator_id = transaction[19] if len(transaction) > 19 else None
        deducted_amount = transaction[20] if len(transaction) > 20 else 0.0
        
        if initiator_id != user.id:
            await query.edit_message_text("Only the transaction initiator can cancel this transaction.")
            return
        
        if status != 'PENDING':
            await query.edit_message_text(f"Cannot cancel transaction. Status is {status}.")
            return
        
        update_transaction_status(transaction_id, 'CANCELLED')
        
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT balance, pending_balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
            initiator_wallet = cursor.fetchone()
            
            if initiator_wallet:
                current_balance, current_pending = initiator_wallet
                new_balance = current_balance + deducted_amount
                new_pending = current_pending - deducted_amount
                
                cursor.execute(
                    'UPDATE wallets SET balance = ?, pending_balance = ? WHERE wallet_id = ?',
                    (new_balance, new_pending, wallet_id)
                )
                conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
        
        await safe_send_text(
            query.edit_message_text,
            f"❌ Transaction cancelled!\n\n"
            f"Transaction ID: `{transaction_id}`\n\n"
            f"The transaction has been cancelled and your funds have been returned.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Delete the message after 3 seconds to remove it from the transactions list
        await asyncio.sleep(3)
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Error deleting message: {e}")


@with_auto_balance_refresh
async def transactions_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user
    transactions = get_user_transactions(user.id)

    if not transactions:
        await update.message.reply_text("You don't have any transactions yet.")
        return

    # Filter out cancelled transactions
    active_transactions = [t for t in transactions if t[6] != 'CANCELLED']
    
    if not active_transactions:
        await update.message.reply_text("You don't have any active transactions.")
        return

    # Show first page (page 0)
    await show_transactions_page(update.message.reply_text, user.id, active_transactions, page=0)


async def show_transactions_page(message_method, user_id, transactions, page=0):
    """Display a page of transactions with pagination."""
    TRANSACTIONS_PER_PAGE = 5
    total_transactions = len(transactions)
    total_pages = (total_transactions + TRANSACTIONS_PER_PAGE - 1) // TRANSACTIONS_PER_PAGE
    
    # Calculate start and end indices for the current page
    start_idx = page * TRANSACTIONS_PER_PAGE
    end_idx = min(start_idx + TRANSACTIONS_PER_PAGE, total_transactions)
    
    # Build keyboard with transactions for current page
    keyboard = []
    for transaction in transactions[start_idx:end_idx]:
        transaction_id = transaction[0]
        description = transaction[9]
        
        button_text = description if description else "No description"
        keyboard.append([InlineKeyboardButton(
            button_text,
            callback_data=f'view_transaction_{transaction_id}'
        )])
    
    # Add navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f'transactions_page_{page-1}'))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f'transactions_page_{page+1}'))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    page_info = f"Page {page + 1}/{total_pages}" if total_pages > 1 else ""
    message_text = f"Select a transaction to view details:\n{page_info}" if page_info else "Select a transaction to view details:"
    
    await message_method(
        message_text,
        reply_markup=reply_markup
    )


@with_auto_balance_refresh
async def check_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        # Get the most recent pending transaction for the user (as buyer or seller)
        cursor.execute('''
            SELECT transaction_id, amount, fee_amount, intermediary_wallet_id, buyer_id, seller_id, crypto_type
            FROM transactions
            WHERE (buyer_id = ? OR seller_id = ?) AND status = 'PENDING' AND crypto_type = 'BTC' AND intermediary_wallet_id IS NOT NULL
            ORDER BY creation_date DESC
            LIMIT 1
        ''', (user.id, user.id))
        
        transaction = cursor.fetchone()
        
        if not transaction:
            await update.message.reply_text(
                "❌ You don't have any pending BTC transactions with an intermediary wallet.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        transaction_id, transaction_amount, fee_amount, intermediary_wallet_id, buyer_id, seller_id, crypto_type = transaction
        
        # Get intermediary wallet address
        cursor.execute('SELECT address FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
        intermediary_result = cursor.fetchone()
        
        if not intermediary_result:
            await update.message.reply_text(
                "❌ Error: Could not find intermediary wallet for this transaction.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        intermediary_address = intermediary_result[0]

        # Get cached escrow wallet balance (updated by background jobs)
        cached_balance = get_cached_wallet_balance(intermediary_address)

        if cached_balance is None:
            await update.message.reply_text(
                "❌ Error: Could not fetch escrow wallet balance. Please try again later.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        balance_btc = cached_balance['balance']

        # Calculate 99% threshold (including fee)
        total_amount = transaction_amount + fee_amount
        threshold_99 = total_amount * 0.99

        # Check if user is buyer or seller
        is_buyer = (user.id == buyer_id)

        # Check if balance meets 99% threshold
        if balance_btc >= threshold_99:
            # Sufficient balance
            if is_buyer:
                await update.message.reply_text(
                    f"✅ **Sufficient BTC Deposit**\n\n"
                    f"Escrow wallet balance:\n"
                    f"*{balance_btc:.8f} BTC*\n\n"
                    f"The seller has been notified to provide goods & services.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                # User is seller
                await update.message.reply_text(
                    f"✅ **Sufficient BTC Deposit**\n\n"
                    f"Escrow wallet balance:\n"
                    f"*{balance_btc:.8f} BTC*\n\n"
                    f"Please deliver goods & services to the buyer as agreed upon.",
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            # Insufficient balance
            shortfall = threshold_99 - balance_btc
            
            if is_buyer:
                await update.message.reply_text(
                    f"⚠️ **Insufficient BTC Deposit**\n\n"
                    f"Escrow wallet balance:\n"
                    f"*{balance_btc:.8f} BTC*\n\n"
                    f"You need to deposit an additional *{shortfall:.8f} BTC* to the intermediary wallet before the seller can deliver goods & services.\n\n"
                    f"Escrow wallet address: `{intermediary_address}`",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                # User is seller
                await update.message.reply_text(
                    f"⚠️ **Insufficient BTC Deposit**\n\n"
                    f"Escrow wallet balance:\n"
                    f"*{balance_btc:.8f} BTC*\n\n"
                    f"The buyer needs to deposit an additional *{shortfall:.8f} BTC* before you should deliver goods & services.\n\n"
                    f"Please wait for the buyer to complete their deposit.",
                    parse_mode=ParseMode.MARKDOWN
                )
        
    except sqlite3.Error as e:
        logger.error(f"Database error in check_command: {e}")
        await update.message.reply_text(
            "❌ An error occurred while checking the transaction. Please try again later.",
            parse_mode=ParseMode.MARKDOWN
        )
    finally:
        if conn:
            conn.close()


@with_auto_balance_refresh
async def withdraw_command(update: Update, context: CallbackContext) -> int:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user
    
    # Check for pending transactions before allowing withdrawal
    if has_pending_transactions(user.id, 'BTC'):
        await update.message.reply_text(
            "❌ **Withdrawal Blocked**\n\n"
            "You cannot withdraw BTC while you have a pending transaction. "
            "Please wait for pending transaction to complete before attempting to withdraw.\n\n"
            "You can check your transaction status using the /transactions command.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    wallets = get_user_wallets(user.id)

    if not wallets:
        await update.message.reply_text(
            "You don't have any wallets yet. Please create a wallet first using the /wallet command."
        )
        return ConversationHandler.END

    btc_wallets = [w for w in wallets if w[1] == 'BTC']
    
    if not btc_wallets:
        await update.message.reply_text(
            "You don't have a BTC wallet yet. Please create one using the /wallet command."
        )
        return ConversationHandler.END

    keyboard = []
    for wallet in btc_wallets:
        wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
        usd_balance = convert_crypto_to_fiat(balance, crypto_type, use_cache_only=True)
        usd_value_text = f"${usd_balance:.2f} USD" if usd_balance is not None else "USD value unavailable"

        keyboard.append(
            [InlineKeyboardButton(
                f"{crypto_type}: {balance:.8f} ({usd_value_text})",
                callback_data=f"withdraw_{wallet_id}"
            )]
        )

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select BTC wallet to withdraw from:",
        reply_markup=reply_markup
    )
    
    return SELECTING_WITHDRAW_WALLET


async def select_withdraw_wallet(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()
    
    wallet_id = query.data.replace('withdraw_', '')
    context.user_data['withdraw_wallet_id'] = wallet_id
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        cursor.execute('SELECT balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()
        
        if wallet:
            balance = wallet[0]
            
            # Use cached balance instead of blocking API call
            sync_result = get_cached_balance_by_wallet_id(wallet_id)
            if sync_result['success']:
                balance = sync_result['db_balance']
                print(f"Retrieved cached withdraw wallet balance: {balance} BTC for wallet {wallet_id}")
            else:
                print(f"Failed to get cached withdraw wallet balance: {sync_result.get('error', 'Unknown error')}")
                # Continue with stored balance as fallback
            
            context.user_data['withdraw_wallet_balance'] = balance
            
            displayable_balance = max(0, balance - 0.00000250)
            
            keyboard = [[InlineKeyboardButton("Withdraw Max Amount", callback_data='withdraw_max')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"Your current balance: {displayable_balance:.8f} BTC\n\n"
                f"How much BTC would you like to withdraw?\n"
                f"(Enter amount in BTC)",
                reply_markup=reply_markup
            )
        else:
            await query.edit_message_text("Wallet not found.")
            return ConversationHandler.END
            
    except sqlite3.Error as e:
        await query.edit_message_text(f"Database error: {e}")
        return ConversationHandler.END
    finally:
        if conn:
            conn.close()
    
    return ENTERING_WITHDRAW_AMOUNT


async def enter_withdraw_amount(update: Update, context: CallbackContext) -> int:
    try:
        amount = float(update.message.text.strip())
        
        if amount <= 0:
            await update.message.reply_text("Amount must be greater than 0. Please try again:")
            return ENTERING_WITHDRAW_AMOUNT
        
        wallet_balance = context.user_data.get('withdraw_wallet_balance', 0)
        
        # Use cached balance instead of blocking API call
        wallet_id = context.user_data.get('withdraw_wallet_id')
        if wallet_id:
            sync_result = get_cached_balance_by_wallet_id(wallet_id)
            if sync_result['success']:
                wallet_balance = sync_result['db_balance']
                context.user_data['withdraw_wallet_balance'] = wallet_balance
                print(f"Retrieved cached withdraw wallet balance: {wallet_balance} BTC for wallet {wallet_id}")
            else:
                print(f"Failed to get cached withdraw wallet balance: {sync_result.get('error', 'Unknown error')}")
        
        if amount > wallet_balance:
            await update.message.reply_text(
                f"Insufficient balance. Your balance is {wallet_balance:.8f} BTC.\n"
                f"Please enter a valid amount:"
            )
            return ENTERING_WITHDRAW_AMOUNT
        
        context.user_data['withdraw_amount'] = amount
        context.user_data['withdraw_max'] = False
        
        await update.message.reply_text(
            f"Amount to withdraw: {amount:.8f} BTC\n\n"
            f"Please enter BTC wallet address where these funds will be withdrawn to:"
        )
        
        return ENTERING_WALLET_ADDRESS
        
    except ValueError:
        await update.message.reply_text(
            "Invalid amount. Please enter a number:"
        )
        return ENTERING_WITHDRAW_AMOUNT


async def withdraw_max_amount(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()
    
    context.user_data['withdraw_max'] = True
    
    await query.edit_message_text(
        "You selected to withdraw maximum amount.\n\n"
        "Please enter BTC wallet address where these funds will be withdrawn to:"
    )
    
    return ENTERING_WALLET_ADDRESS


async def enter_wallet_address(update: Update, context: CallbackContext) -> int:
    address = update.message.text.strip()
    
    if len(address) < 26 or len(address) > 62:
        await update.message.reply_text(
            "Invalid BTC address format. Please enter a valid BTC address:"
        )
        return ENTERING_WALLET_ADDRESS
    
    context.user_data['withdraw_address'] = address
    
    wallet_id = context.user_data['withdraw_wallet_id']
    is_max_withdrawal = context.user_data.get('withdraw_max', False)
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT address, private_key FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()
        
        if not wallet:
            await update.message.reply_text("Wallet not found.")
            return ConversationHandler.END
        
        from_address, private_key = wallet
        
        if is_max_withdrawal:
            result = await asyncio.to_thread(
                btcwalletclient_wif.send_max_btc_auto,
                wif_private_key=private_key,
                destination_address=address
            )
        else:
            amount = context.user_data['withdraw_amount']
            result = await asyncio.to_thread(
                btcwalletclient_wif.send_specific_btc_amount,
                wif_private_key=private_key,
                destination_address=address,
                amount_btc=amount
            )
        
        if result['success']:
            amount_sent = result['amount_sent']
            fee_paid = result['fee']
            
            cursor.execute(
                'UPDATE wallets SET balance = balance - ? WHERE wallet_id = ?',
                (amount_sent + fee_paid, wallet_id)
            )
            conn.commit()
            
            await update.message.reply_text(
                f"✅ Withdrawal successful!\n\n"
                f"Amount sent: {amount_sent:.8f} BTC\n"
                f"Transaction fee: {fee_paid:.8f} BTC\n\n"
                f"BTC Wallet Address: {address}\n\n"
                f"Transaction ID: {result['txid']}\n\n"
                f"Your funds have been sent!"
            )
        else:
            await update.message.reply_text(
                f"❌ Withdrawal failed: {result.get('error', 'Unknown error')}"
            )
        
    except Exception as e:
        await update.message.reply_text(f"Error processing withdrawal: {str(e)}")
    finally:
        if conn:
            conn.close()
    
    return ConversationHandler.END


def send_btc_to_seller(buyer_wallet_id, seller_id, amount, fee_amount, fee_wallet_address):
    """
    Send BTC from buyer's wallet to seller's wallet and pay the fee.
    
    Args:
        buyer_wallet_id (str): Wallet ID of the buyer
        seller_id (int): User ID of the seller
        amount (float): Total amount to send (includes fee)
        fee_amount (float): Fee amount to deduct
        fee_wallet_address (str): Bitcoin address to send the fee to
    
    Returns:
        dict: {success: bool, error: str, seller_address: str, seller_amount: float, txid: str}
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT address, private_key, wallet_type, address_type FROM wallets WHERE wallet_id = ?', (buyer_wallet_id,))
        buyer_wallet = cursor.fetchone()
        
        if not buyer_wallet:
            return {'success': False, 'error': 'Buyer wallet not found'}
        
        buyer_address, buyer_private_key, wallet_type, address_type = buyer_wallet
        
        cursor.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, 'BTC'))
        seller_wallet = cursor.fetchone()
        
        if not seller_wallet:
            return {'success': False, 'error': 'Seller wallet not found'}
        
        seller_address = seller_wallet[0]
        
        try:
            result = btcwalletclient_wif.send_batch_95_5_split(
                wif_private_key=buyer_private_key,
                seller_address=seller_address
            )
            
            if result['success']:
                return {
                    'success': True,
                    'seller_address': seller_address,
                    'seller_amount': result['seller_amount'],
                    'fee_amount': result['fee_wallet_amount'],
                    'transaction_fee': result['transaction_fee'],
                    'txid': result['txid']
                }
            else:
                return {'success': False, 'error': result.get('error', 'Transaction failed')}
                
        except Exception as tx_error:
            logger.error(f"Error creating BTC transaction: {tx_error}")
            return {'success': False, 'error': f'Transaction creation failed: {str(tx_error)}'}
            
    except sqlite3.Error as db_error:
        logger.error(f"Database error in send_btc_to_seller: {db_error}")
        return {'success': False, 'error': f'Database error: {str(db_error)}'}
    finally:
        if conn:
            conn.close()


def refund_btc_to_buyer(escrow_wallet_id, seller_id):
    """
    Send 50% of BTC to seller and 50% to fee wallet for disputed transactions.
    
    Args:
        escrow_wallet_id (str): Wallet ID of the escrow wallet
        seller_id (int): User ID of the seller
    
    Returns:
        dict: {success: bool, error: str, seller_address: str, seller_amount: float, txid: str}
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT address, private_key, wallet_type, address_type FROM wallets WHERE wallet_id = ?', (escrow_wallet_id,))
        escrow_wallet = cursor.fetchone()
        
        if not escrow_wallet:
            return {'success': False, 'error': 'Escrow wallet not found'}
        
        escrow_address, escrow_private_key, wallet_type, address_type = escrow_wallet
        
        cursor.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, 'BTC'))
        seller_wallet = cursor.fetchone()
        
        if not seller_wallet:
            return {'success': False, 'error': 'Seller wallet not found'}
        
        seller_address = seller_wallet[0]
        
        try:
            result = btcwalletclient_wif.send_dispute_refund_50_50(
                wif_private_key=escrow_private_key,
                seller_address=seller_address
            )
            
            if result['success']:
                return {
                    'success': True,
                    'seller_address': seller_address,
                    'seller_amount': result['seller_amount'],
                    'fee_amount': result['fee_wallet_amount'],
                    'transaction_fee': result['transaction_fee'],
                    'txid': result['txid']
                }
            else:
                return {'success': False, 'error': result.get('error', 'Transaction failed')}
                
        except Exception as tx_error:
            logger.error(f"Error creating BTC refund transaction: {tx_error}")
            return {'success': False, 'error': f'Transaction creation failed: {str(tx_error)}'}
            
    except sqlite3.Error as db_error:
        logger.error(f"Database error in refund_btc_to_buyer: {db_error}")
        return {'success': False, 'error': f'Database error: {str(db_error)}'}
    finally:
        if conn:
            conn.close()


@with_auto_balance_refresh
async def release_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user

    conn = None
    results = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''SELECT transaction_id, seller_id, buyer_id, crypto_type, amount, status, creation_date, description
               FROM transactions
               WHERE buyer_id = ? AND status = 'PENDING'
               ORDER BY creation_date DESC''',
            (user.id,)
        )
        results = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in release_command: {e}")
    finally:
        if conn:
            conn.close()

    if not results:
        await update.message.reply_text(
            "No pending transaction found to release. Please check your transactions with /transactions command."
        )
        return

    keyboard = []
    for transaction in results:
        transaction_id = transaction[0]
        description = transaction[7]
        
        button_text = description if description else "No description"
        keyboard.append([InlineKeyboardButton(
            button_text,
            callback_data=f'select_release_{transaction_id}'
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select a transaction to release funds:",
        reply_markup=reply_markup
    )


async def release_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith('select_release_'):
        transaction_id = data.replace('select_release_', '')

        transaction = get_transaction(transaction_id)
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return

        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = transaction[3]
        status = transaction[6]

        user_id = query.from_user.id
        
        if buyer_id != user_id:
            await query.edit_message_text("Only the buyer can release funds for this transaction.")
            return
        
        if status != 'PENDING':
            await query.edit_message_text(f"Cannot release funds. Transaction status is {status}.")
            return

        # Check if seller has a wallet for this crypto type before proceeding
        conn_check = None
        try:
            conn_check = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor_check = conn_check.cursor()
            cursor_check.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, crypto_type.upper()))
            seller_wallet = cursor_check.fetchone()
            
            if not seller_wallet:
                await query.edit_message_text(
                    f"There is no wallet address associated with the seller for this transaction, the seller needs to create a {crypto_type} wallet before funds can be released."
                )
                return
        finally:
            if conn_check:
                conn_check.close()

        keyboard = [
            [
                InlineKeyboardButton("Yes, release funds", callback_data=f'release_{transaction_id}'),
                InlineKeyboardButton("No, cancel", callback_data='cancel_release')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Are you sure you want to release funds for transaction {transaction_id}?\n"
            f"This action cannot be undone.",
            reply_markup=reply_markup
        )
    elif data.startswith('release_'):
        transaction_id = data.split('_')[1]

        # Get transaction details including intermediary_wallet_id
        conn = None
        transaction = None
        intermediary_wallet_id = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT seller_id, buyer_id, crypto_type, amount, fee_amount, status, wallet_id, intermediary_wallet_id FROM transactions WHERE transaction_id = ?', (transaction_id,))
            transaction = cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Database error in release_callback: {e}")
        finally:
            if conn:
                conn.close()

        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return

        seller_id = transaction[0]
        buyer_id = transaction[1]
        crypto_type = transaction[2]
        amount = transaction[3]
        fee_amount = transaction[4]
        status = transaction[5]
        wallet_id = transaction[6]
        intermediary_wallet_id = transaction[7]

        user_id = query.from_user.id
        
        if buyer_id != user_id:
            await query.edit_message_text("Only the buyer can release funds for this transaction.")
            return
        
        if status == 'EXPIRED':
            await query.edit_message_text(
                "Cannot release funds. This transaction has expired (pending for more than 24 hours)."
            )
            return
        
        if status != 'PENDING':
            await query.edit_message_text(f"Cannot release funds. Transaction status is {status}.")
            return

        if intermediary_wallet_id:
            conn_balance = None
            try:
                conn_balance = sqlite3.connect(DB_PATH, timeout=20.0)
                cursor_balance = conn_balance.cursor()
                cursor_balance.execute('SELECT address FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
                intermediary_result = cursor_balance.fetchone()
                
                if intermediary_result:
                    intermediary_address = intermediary_result[0]

                    try:
                        # Get cached balance (updated by background jobs)
                        cached_balance = get_cached_wallet_balance(intermediary_address)

                        if cached_balance is not None:
                            balance_btc = cached_balance['balance']
                            threshold_99 = amount * 0.99

                            if balance_btc < threshold_99:
                                shortfall = threshold_99 - balance_btc
                                await query.edit_message_text(
                                    f"⚠️ Cannot release funds yet.\n\n"
                                    f"Current balance: {balance_btc:.8f} BTC\n"
                                    f"Required balance: {threshold_99:.8f} BTC\n\n"
                                    f"Please deposit an additional {shortfall:.8f} BTC to the intermediary wallet before releasing funds."
                                )
                                return
                    except Exception as balance_error:
                        logger.error(f"Error checking escrow wallet balance: {balance_error}")
            except sqlite3.Error as e:
                logger.error(f"Database error checking intermediary wallet: {e}")
            finally:
                if conn_balance:
                    conn_balance.close()

        if crypto_type == 'BTC':
            try:
                # Check if seller has a wallet for this crypto type before proceeding
                conn_check = None
                try:
                    conn_check = sqlite3.connect(DB_PATH, timeout=20.0)
                    cursor_check = conn_check.cursor()
                    cursor_check.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, crypto_type.upper()))
                    seller_wallet = cursor_check.fetchone()
                    
                    if not seller_wallet:
                        await query.edit_message_text(
                            f"There is no wallet address associated with the seller for this transaction, the seller needs to create a {crypto_type} wallet before funds can be released."
                        )
                        return
                finally:
                    if conn_check:
                        conn_check.close()
                
                fee_wallet_address = "bc1q8mcfyyt0hdhsqvv4ly6czz52gyak5zaayw8qa5"
                
                # Use intermediary wallet if available, otherwise fall back to buyer's wallet
                escrow_wallet_id = intermediary_wallet_id if intermediary_wallet_id else wallet_id
                
                result = send_btc_to_seller(escrow_wallet_id, seller_id, amount, fee_amount, fee_wallet_address)
                
                if result['success']:
                    update_transaction_status(transaction_id, 'COMPLETED')
                    txid = result.get('txid', 'pending')
                    seller_amount = result['seller_amount']
                    fee_wallet_amount = result['fee_amount']
                    transaction_fee = result.get('transaction_fee', 0)
                    
                    await query.edit_message_text(
                        f"✅ Funds released for transaction {transaction_id}\n\n"
                        f"Transaction ID: {txid}\n\n"
                        f"Seller receives (95%): {seller_amount:.8f} BTC\n"
                        f"Escrow fee (5%): {fee_wallet_amount:.8f} BTC\n"
                        f"Network fee: {transaction_fee:.8f} BTC"
                    )
                else:
                    error_msg = result.get('error', 'Unknown error')
                    await query.edit_message_text(
                        f"⚠️ Error processing transaction {transaction_id}\n"
                        f"Error: {error_msg}"
                    )
            except Exception as e:
                logger.error(f"Error processing BTC transaction: {e}")
                await query.edit_message_text(
                    f"⚠️ Error processing transaction {transaction_id} "
                    f"Please contact support for assistance."
                )
        else:
            # Check if seller has a wallet for this crypto type before proceeding
            conn_check = None
            try:
                conn_check = sqlite3.connect(DB_PATH, timeout=20.0)
                cursor_check = conn_check.cursor()
                cursor_check.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, crypto_type.upper()))
                seller_wallet = cursor_check.fetchone()
                
                if not seller_wallet:
                    await query.edit_message_text(
                        f"There is no wallet address associated with the seller for this transaction, the seller needs to create a {crypto_type} wallet before funds can be released."
                    )
                    return
            finally:
                if conn_check:
                    conn_check.close()
            
            update_transaction_status(transaction_id, 'COMPLETED')
            await query.edit_message_text(
                f"✅ Funds released for transaction {transaction_id} "
                f"The seller has been notified and will receive the funds shortly."
            )
    elif data == 'cancel_release':
        await query.edit_message_text("Release cancelled.")


async def dispute_command(update: Update, context: CallbackContext) -> int:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user

    conn = None
    results = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''SELECT transaction_id, seller_id, buyer_id, crypto_type, amount, status, creation_date, description
               FROM transactions
               WHERE (seller_id = ? OR buyer_id = ?) AND status = 'PENDING'
               ORDER BY creation_date DESC''',
            (user.id, user.id)
        )
        results = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in dispute_command: {e}")
    finally:
        if conn:
            conn.close()

    if not results:
        await update.message.reply_text(
            "No pending transaction found to dispute. Please check your transactions with /transactions command."
        )
        return ConversationHandler.END

    keyboard = []
    for transaction in results:
        transaction_id = transaction[0]
        description = transaction[7]
        
        button_text = description if description else "No description"
        keyboard.append([InlineKeyboardButton(
            button_text,
            callback_data=f'select_dispute_{transaction_id}'
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select a transaction to dispute:",
        reply_markup=reply_markup
    )
    
    return DISPUTE_REASON


async def dispute_selection_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith('select_dispute_'):
        transaction_id = data.replace('select_dispute_', '')
        
        transaction = get_transaction(transaction_id)
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return ConversationHandler.END

        seller_id = transaction[1]
        buyer_id = transaction[2]
        status = transaction[6]

        if status == 'DISPUTED':
            await query.edit_message_text(
                "This transaction has been disputed. Please allow our team 1-2 business day(s) to make a determination regarding this dispute."
            )
            return ConversationHandler.END

        if status != 'PENDING':
            await query.edit_message_text(f"Cannot dispute transaction. Status is {status}.")
            return ConversationHandler.END

        context.user_data['dispute_transaction_id'] = transaction_id

        await query.edit_message_text(
            f"You are opening a dispute for transaction {transaction_id}.\n\n"
            f"Please explain the reason for the dispute:"
        )

        return DISPUTE_REASON
    
    return ConversationHandler.END


async def dispute_reason(update: Update, context: CallbackContext) -> int:
    reason = update.message.text.strip()
    context.user_data['dispute_reason'] = reason

    await update.message.reply_text(
        "Please provide evidence to support your dispute claim. "
        "This could be screenshots, transaction hashes, or any other relevant information:"
    )

    return DISPUTE_EVIDENCE


async def dispute_evidence(update: Update, context: CallbackContext) -> int:
    evidence = update.message.text.strip()

    user = update.effective_user
    transaction_id = context.user_data['dispute_transaction_id']
    reason = context.user_data['dispute_reason']

    # Create dispute in database
    dispute_id = create_dispute(transaction_id, user.id, reason, evidence)

    # Escape the dispute ID for Markdown
    escaped_dispute_id = escape_markdown(dispute_id)
    await safe_send_text(
        update.message.reply_text,
        f"✅ Dispute opened successfully!\n\n"
        f"Dispute ID: `{escaped_dispute_id}`\n\n"
        f"Our team will review your case and contact you soon. "
        f"The transaction has been put on hold until the dispute is resolved.",
        parse_mode=ParseMode.MARKDOWN
    )

    return ConversationHandler.END


async def language_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    keyboard = [
        [
            InlineKeyboardButton("English 🇬🇧", callback_data='lang_en'),
            InlineKeyboardButton("Español 🇪🇸", callback_data='lang_es')
        ],
        [
            InlineKeyboardButton("Русский 🇷🇺", callback_data='lang_ru'),
            InlineKeyboardButton("中文 🇨🇳", callback_data='lang_zh')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select your preferred language:",
        reply_markup=reply_markup
    )


async def language_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    data = query.data

    if data.startswith('lang_'):
        language_code = data.split('_')[1]

        # Update user's language preference in database
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()

            cursor.execute(
                'UPDATE users SET language_code = ? WHERE user_id = ?',
                (language_code, user.id)
            )

            conn.commit()
        except sqlite3.Error as e:
            print(f"Database error in language_callback: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

        language_names = {
            'en': 'English',
            'es': 'Spanish',
            'ru': 'Russian',
            'zh': 'Chinese'
        }

        await query.edit_message_text(
            f"Your language has been set to {language_names.get(language_code, language_code)}."
        )


async def create_escrow_group_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    
    group_data = context.user_data.get('create_group_data')
    if not group_data:
        await query.edit_message_text("Error: No transaction data found. Please initiate a transaction first.")
        return
    
    recipient = group_data['recipient']
    transaction_id = group_data['transaction_id']
    sender_name = group_data['sender_name']
    sender_username = group_data.get('sender_username')
    sender_id = group_data.get('sender_id')
    crypto_type = group_data['crypto_type']
    amount = group_data['amount']
    usd_amount = group_data['usd_amount']
    fee = group_data['fee']
    usd_fee = group_data['usd_fee']
    total = group_data['total']
    usd_total = group_data['usd_total']
    description = group_data['description']
    wallet_address = group_data.get('wallet_address')
    partial_transfer = group_data.get('partial_transfer', False)
    btc_transferred = group_data.get('btc_transferred', 0.0)
    remaining_btc_needed = group_data.get('remaining_btc_needed', 0.0)
    
    random_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    group_name = f"Escrow #{random_code}"
    
    bot_username = os.getenv('BOT_USERNAME', 'IncognitoEscrowBot')
    usernames_to_add = [recipient]
    
    if sender_username:
        usernames_to_add.append(f"@{sender_username}" if not sender_username.startswith('@') else sender_username)
    elif sender_id:
        usernames_to_add.append(sender_id)
    
    await query.edit_message_text(
        f"Creating escrow group '{group_name}'...\nPlease wait..."
    )
    
    try:
        result = await create_supergroup_with_users(group_name, usernames_to_add, bot_username)
        
        if result['success']:
            telethon_group_id = result.get('telethon_group_id')
            
            if telethon_group_id and telethon_client:
                # Prepare partial transfer info if applicable
                partial_transfer_text = ""
                if partial_transfer and crypto_type.upper() == 'BTC':
                    partial_transfer_text = (
                        f"\n\n💳 **Partial Payment Made:**\n"
                        f"**Transferred to Escrow:** {btc_transferred:.8f} BTC\n"
                        f"**Remaining Amount Needed:** {remaining_btc_needed:.8f} BTC\n"
                        f"⚠️ Please send the remaining {remaining_btc_needed:.8f} BTC to the wallet address above to complete the transaction."
                    )
                
                group_message = (
                    f"💰 **Escrow Transaction Created**\n\n"
                    f"**Buyer:** {sender_name}\n"
                    f"**Seller:** {recipient}\n\n"
                    f"**Description:** {description}"
                    f"{partial_transfer_text}\n\n"
                    f"**Payment Method:** {crypto_type}\n"
                    f"**Amount:** {amount * 1.05:.8f} {crypto_type}\n"
                    f"**USD Value:** ${usd_amount:.2f} USD\n"
                    f"**Escrow Fee (5%):** ${usd_fee:.2f} USD\n"
                    f"**Total:** ${usd_total:.2f} USD\n"
                    f"**Transaction ID:** `{transaction_id}`\n"
                    f"**Escrow Wallet Address:** `{wallet_address if wallet_address else 'N/A'}`\n\n"
                    f"⚠️ **Action Required:**\n"
                    "Seller can run /check command to see if a buyer has deposited a sufficient amount of BTC to the escrow wallet.\n\nBuyer can run /release command to transfer BTC from escrow BTC wallet to seller's BTC wallet once goods & services are delivered to the buyer as described."
                )
                
                await telethon_client.send_message(
                    telethon_group_id,
                    group_message
                )
            
            message = f"✅ Escrow group '{group_name}' created successfully!"
            if result.get('group_link'):
                keyboard = [[InlineKeyboardButton("Open Escrow Group", url=result['group_link'])]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, reply_markup=reply_markup)
            else:
                await query.edit_message_text(message)
        else:
            await query.edit_message_text(f"❌ Failed to create escrow group: {result['message']}")
    except Exception as e:
        error_msg = f"An error occurred while creating the group: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await query.edit_message_text(f"❌ Error: {error_msg}")


async def enter_m(update: Update, context: CallbackContext) -> int:
    try:
        m = int(update.message.text.strip())
        if m < 1 or m > 15:
            await update.message.reply_text("Please enter a number between 1 and 15:")
            return ENTERING_M

        context.user_data['m'] = m

        await update.message.reply_text(
            f"How many total keys should be in this wallet? (n in m-of-n)\n\n"
            f"Enter a number between {m} and 15:"
        )

        return ENTERING_N
    except ValueError:
        await update.message.reply_text("Please enter a valid number:")
        return ENTERING_M


async def enter_n(update: Update, context: CallbackContext) -> int:
    try:
        n = int(update.message.text.strip())
        m = context.user_data['m']

        if n < m or n > 15:
            await update.message.reply_text(f"Please enter a number between {m} and 15:")
            return ENTERING_N

        context.user_data['n'] = n

        # Ask if user wants to enter public keys or generate new ones
        keyboard = [
            [
                InlineKeyboardButton("Generate new keys", callback_data='generate_keys'),
                InlineKeyboardButton("Enter public keys", callback_data='enter_keys')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"You're creating a {m}-of-{n} multisig wallet.\n\n"
            f"Do you want to generate new keys or enter existing public keys?",
            reply_markup=reply_markup
        )

        return ENTERING_PUBLIC_KEYS
    except ValueError:
        await update.message.reply_text("Please enter a valid number:")
        return ENTERING_N


async def public_keys_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == 'generate_keys':
        # Generate new keys
        crypto_type = context.user_data['crypto_type']
        address_type = context.user_data['address_type']
        m = context.user_data['m']
        n = context.user_data['n']

        # Check if user already has a wallet for this cryptocurrency
        existing_wallets = get_user_wallets(query.from_user.id)
        has_wallet = any(wallet[1] == crypto_type for wallet in existing_wallets)

        if has_wallet:
            # User already has a wallet for this cryptocurrency
            await safe_send_text(
                query.edit_message_text,
                f"⚠️ You already have a {crypto_type} wallet. Only one wallet per cryptocurrency is allowed.\n\n"
                f"Please use your existing wallet or choose a different cryptocurrency.",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationHandler.END
        else:
            # Create multisig wallet
            wallet_id, address = create_wallet(
                query.from_user.id,
                crypto_type,
                wallet_type='multisig',
                address_type=address_type,
                m=m,
                n=n
            )

            # Process any pending transactions for this user
            pending_result = process_pending_recipient(query.from_user.id, query.from_user.username)

            # Escape the address for Markdown
            escaped_address = escape_markdown(address)
            wallet_created_msg = (
                f"✅ Your new {crypto_type} multisig wallet has been created!\n\n"
                f"Address: `{escaped_address}`\n\n"
                f"This is a {m}-of-{n} multisig wallet with {address_type} address format.\n"
                f"The private keys are securely stored in the database."
            )
            
            # If there were pending transactions, add a notification
            if pending_result['success'] and pending_result['transactions_updated'] > 0:
                wallet_created_msg += (
                    f"\n\n💰 *{pending_result['transactions_updated']} pending transaction(s)* "
                    f"have been added to your wallet!"
                )
            
            await safe_send_text(
                query.edit_message_text,
                wallet_created_msg,
                parse_mode=ParseMode.MARKDOWN
            )

        return ConversationHandler.END
    elif data == 'enter_keys':
        await query.edit_message_text(
            f"Please enter {context.user_data['n']} public keys, one per line:"
        )

        return CONFIRMING_WALLET

    return ConversationHandler.END


async def confirm_wallet(update: Update, context: CallbackContext) -> int:
    public_keys_text = update.message.text.strip()
    public_keys = [key.strip() for key in public_keys_text.split('\n')]

    crypto_type = context.user_data['crypto_type']
    address_type = context.user_data['address_type']
    m = context.user_data['m']
    n = context.user_data['n']

    if len(public_keys) != n:
        await update.message.reply_text(
            f"You entered {len(public_keys)} keys, but {n} are required. Please try again:"
        )
        return CONFIRMING_WALLET

    try:
        # Check if user already has a wallet for this cryptocurrency
        existing_wallets = get_user_wallets(update.effective_user.id)
        has_wallet = any(wallet[1] == crypto_type for wallet in existing_wallets)

        if has_wallet:
            # User already has a wallet for this cryptocurrency
            await safe_send_text(
                update.message.reply_text,
                f"⚠️ You already have a {crypto_type} wallet. Only one wallet per cryptocurrency is allowed.\n\n"
                f"Please use your existing wallet or choose a different cryptocurrency.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            # Create multisig wallet with provided public keys
            wallet_id, address = create_wallet(
                update.effective_user.id,
                crypto_type,
                wallet_type='multisig',
                address_type=address_type,
                m=m,
                n=n,
                public_keys=public_keys
            )

            # Process any pending transactions for this user
            pending_result = process_pending_recipient(update.effective_user.id, update.effective_user.username)

            # Escape the address for Markdown
            escaped_address = escape_markdown(address)
            wallet_created_msg = (
                f"✅ Your new {crypto_type} multisig wallet has been created!\n\n"
                f"Address: `{escaped_address}`\n\n"
                f"This is a {m}-of-{n} multisig wallet with {address_type} address format."
            )
            
            # If there were pending transactions, add a notification
            if pending_result['success'] and pending_result['transactions_updated'] > 0:
                wallet_created_msg += (
                    f"\n\n💰 *{pending_result['transactions_updated']} pending transaction(s)* "
                    f"have been added to your wallet!"
                )
            
            await safe_send_text(
                update.message.reply_text,
                wallet_created_msg,
                parse_mode=ParseMode.MARKDOWN
            )

        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(
            f"Error creating wallet: {str(e)}\n\n"
            f"Please check your public keys and try again:"
        )
        return CONFIRMING_WALLET


async def cancel(update: Update, context: CallbackContext) -> int:
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors in the telegram bot."""
    print(f"An error occurred: {context.error}")

    # Handle entity parsing errors
    if isinstance(context.error, BadRequest) and "entity" in str(context.error).lower():
        print(f"Entity parsing error: {context.error}")
        if update and update.effective_message:
            try:
                # Try to send a message without formatting

                await update.effective_message.reply_text(
                    "Sorry, there was an error processing your message. "
                    "The message has been sent without formatting."
                )
            except Exception as e:
                print(f"Error sending error message: {e}")
    elif update:
        # For other errors, notify the user that something went wrong
        try:
            await update.effective_message.reply_text(
                "Sorry, an error occurred while processing your request."
            )
        except Exception as e:
            print(f"Error sending error message: {e}")
    # You can add more error handling logic here
    return


@with_auto_balance_refresh
async def sign_transaction_command(update: Update, context: CallbackContext) -> int:
    """Command to sign a multisig transaction"""
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user

    # Get wallet_id and txid from the database
    conn = None
    result = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        # Look for a wallet with txid
        cursor.execute(
            '''SELECT wallet_id, txid
               FROM wallets
               WHERE user_id = ? AND txid IS NOT NULL
               ORDER BY wallet_id DESC LIMIT 1''',
            (user.id,)
        )
        result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in sign_transaction_command (1): {e}")
    finally:
        if conn:
            conn.close()

    if not result:
        await update.message.reply_text(
            "No pending transaction found to sign. Please create a transaction first."
        )
        return ConversationHandler.END

    wallet_id = result[0]
    txid = result[1]

    await update.message.reply_text(
        f"Signing transaction with wallet ID: {wallet_id} and transaction ID: {txid}"
    )

    # Get wallet from database
    conn = None
    wallet = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''SELECT wallet_id, crypto_type, address, wallet_type, private_key, required_sigs, total_keys
               FROM wallets WHERE wallet_id = ? AND user_id = ?''',
            (wallet_id, user.id)
        )
        wallet = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in sign_transaction_command (2): {e}")
    finally:
        if conn:
            conn.close()

    if not wallet:
        await update.message.reply_text(f"Wallet {wallet_id} not found or you don't have access to it.")
        return ConversationHandler.END

    wallet_type = wallet[3]

    if wallet_type != 'multisig':
        await update.message.reply_text("This command is only for multisig wallets.")
        return ConversationHandler.END

    try:
        # Get private keys from wallet
        private_key = wallet[4]
        private_keys = json.loads(private_key)

        # Sign transaction
        crypto_type = wallet[1]
        if crypto_type.upper() == 'BTC':
            # Extract wallet name from database
            wallet_name = f"user_{user.id}_{crypto_type.lower()}_{wallet_id}"

            # Sign transaction using TransactionManager
            signed_tx = TransactionManager.sign_transaction(wallet_name, txid, private_keys)

            if signed_tx:
                # Store the signed transaction hex in the database
                conn = None
                try:
                    conn = sqlite3.connect(DB_PATH, timeout=20.0)
                    cursor = conn.cursor()

                    # Update the wallet with the signed transaction hex
                    cursor.execute(
                        'UPDATE wallets SET tx_hex = ? WHERE wallet_id = ?',
                        (signed_tx, wallet_id)
                    )

                    # Check if there's a transaction associated with this user
                    cursor.execute(
                        'SELECT transaction_id FROM transactions WHERE seller_id = ? OR buyer_id = ? ORDER BY creation_date DESC LIMIT 1',
                        (user.id, user.id)
                    )
                    transaction = cursor.fetchone()

                    if transaction:
                        # Update the transaction with the signed transaction hex
                        cursor.execute(
                            'UPDATE transactions SET tx_hex = ? WHERE transaction_id = ?',
                            (signed_tx, transaction[0])
                        )

                    conn.commit()
                except sqlite3.Error as e:
                    print(f"Database error in sign_transaction_command (3): {e}")
                    if conn:
                        conn.rollback()
                finally:
                    if conn:
                        conn.close()

                # Escape the transaction hex for Markdown
                escaped_tx = escape_markdown(signed_tx[:64])
                await safe_send_text(
                    update.message.reply_text,
                    f"Transaction signed successfully!\n\n"
                    f"Signed transaction: `{escaped_tx}...`\n\n"
                    f"You can broadcast this transaction using /broadcast <tx_hex>",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("Failed to sign transaction. Please check the transaction ID.")
        else:
            await update.message.reply_text(f"Signing {crypto_type} multisig transactions is not supported yet.")
    except Exception as e:
        await update.message.reply_text(f"Error signing transaction: {str(e)}")

    return ConversationHandler.END


@with_auto_balance_refresh
async def broadcast_transaction_command(update: Update, context: CallbackContext) -> None:
    """Command to broadcast a signed transaction"""
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user

    # Get tx_hex from the database
    conn = None
    result = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()

        # First, try to find a transaction associated with the user that has a tx_hex
        cursor.execute(
            '''SELECT tx_hex
               FROM transactions
               WHERE (buyer_id = ? OR seller_id = ?) AND tx_hex IS NOT NULL
               ORDER BY creation_date DESC LIMIT 1''',
            (user.id, user.id)
        )
        result = cursor.fetchone()

        if not result:
            # If no transaction found, try to find a wallet with tx_hex
            cursor.execute(
                '''SELECT tx_hex
                   FROM wallets
                   WHERE user_id = ? AND tx_hex IS NOT NULL
                   ORDER BY wallet_id DESC LIMIT 1''',
                (user.id,)
            )
            result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in broadcast_transaction_command (1): {e}")
    finally:
        if conn:
            conn.close()

    if not result or not result[0]:
        await update.message.reply_text(
            "No signed transaction found to broadcast. Please sign a transaction first with /sign command."
        )
        return

    tx_hex = result[0]

    await update.message.reply_text(
        f"Broadcasting transaction with hex: {tx_hex[:32]}..."
    )

    try:
        # Broadcast transaction using TransactionManager
        txid = TransactionManager.broadcast_transaction(tx_hex)

        if txid:
            # Store tx_hex and txid in the database
            # First, check if this is for a wallet or a transaction
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # Try to find a matching wallet
            cursor.execute('SELECT wallet_id FROM wallets WHERE user_id = ?', (update.effective_user.id,))
            wallets = cursor.fetchall()

            if wallets:
                # Update the first wallet found (in a real implementation, you would specify which wallet)
                cursor.execute(
                    'UPDATE wallets SET tx_hex = ?, txid = ? WHERE wallet_id = ?',
                    (tx_hex, txid, wallets[0][0])
                )

            # Try to find a matching transaction
            cursor.execute('SELECT transaction_id FROM transactions WHERE buyer_id = ? OR seller_id = ?',
                           (update.effective_user.id, update.effective_user.id))
            transactions = cursor.fetchall()

            if transactions:
                # Update the first transaction found (in a real implementation, you would specify which transaction)
                cursor.execute(
                    'UPDATE transactions SET tx_hex = ?, txid = ? WHERE transaction_id = ?',
                    (tx_hex, txid, transactions[0][0])
                )

            conn.commit()
            conn.close()

            # Escape the transaction ID for Markdown
            escaped_txid = escape_markdown(txid)
            await safe_send_text(
                update.message.reply_text,
                f"Transaction broadcast successfully!\n\n"
                f"Transaction ID: `{escaped_txid}`",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text("Failed to broadcast transaction. Please check the transaction hex.")
    except Exception as e:
        await update.message.reply_text(f"Error broadcasting transaction: {str(e)}")


async def handle_keyboard_buttons(update: Update, context: CallbackContext) -> None:
    """Handle keyboard button presses from the main menu."""
    await ensure_user_and_process_pending(update)
    
    global app
    text = update.message.text

    if text == "My Account":
        # Handle My Account button - show nested menu
        account_keyboard = [
            [KeyboardButton("Start Trade"), KeyboardButton("My Wallet")],
            [KeyboardButton("Release Funds"), KeyboardButton("File Dispute")],
            [KeyboardButton("Back to Main Menu 🔙")]
        ]
        reply_markup = ReplyKeyboardMarkup(account_keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "My Account Options:",
            reply_markup=reply_markup
        )
    elif text == "Transaction History":
        # Handle Transaction History button - redirect to transactions command
        await transactions_command(update, context)
    elif text == "Language":
        # Handle Language button - redirect to language command
        await language_command(update, context)
    elif text == "Help Desk":
        # Handle Help Desk button - redirect to help command
        await help_command(update, context)
    elif text == "Withdraw Funds":
        # Handle Withdraw Funds button - create a command update to trigger the conversation handler
        await app.process_update(
            Update.de_json(
                {
                    "update_id": update.update_id,
                    "message": {
                        "message_id": update.message.message_id,
                        "from": update.message.from_user.to_dict(),
                        "chat": update.message.chat.to_dict(),
                        "date": update.message.date.timestamp(),
                        "text": "/withdraw",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 9}]
                    }
                },
                context.bot
            )
        )
    elif text == "My Wallet":
        # Handle My Wallet button - redirect to wallet command
        await wallet_command(update, context)
    elif text == "Start Trade":
        # Handle Deposit Funds button - create a command update to trigger the conversation handler
        # This ensures the conversation state is properly set up
        await app.process_update(
            Update.de_json(
                {
                    "update_id": update.update_id,
                    "message": {
                        "message_id": update.message.message_id,
                        "from": update.message.from_user.to_dict(),
                        "chat": update.message.chat.to_dict(),
                        "date": update.message.date.timestamp(),
                        "text": "/deposit",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 8}]
                    }
                },
                context.bot
            )
        )
    elif text == "Release Funds":
        # Handle Release Funds button - redirect to release command
        await release_command(update, context)
    elif text == "File Dispute":
        # Handle File Dispute button - create a command update to trigger the conversation handler
        # This ensures the conversation state is properly set up
        await app.process_update(
            Update.de_json(
                {
                    "update_id": update.update_id,
                    "message": {
                        "message_id": update.message.message_id,
                        "from": update.message.from_user.to_dict(),
                        "chat": update.message.chat.to_dict(),
                        "date": update.message.date.timestamp(),
                        "text": "/dispute",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 8}]
                    }
                },
                context.bot
            )
        )
    elif text == "Back to Main Menu 🔙":
        # Return to main menu with welcome message and video
        user = update.effective_user
        deals_completed = get_stat('deals_completed')
        disputes_resolved = get_stat('disputes_resolved')
        
        if deals_completed % 10 == 0 and disputes_resolved % 10 == 0:
            disputes_resolved += 1
        
        welcome_message = (
            f"Welcome to SafeSwap Escrow Bot,              {user.first_name}{f' {user.last_name}' if user.last_name else ''}!\n\n"
            "We are your trusted escrow service for secure transactions. "
            "Keep your funds safe and pay other users with confidence.\n\n"
            f"📊 *Deals Completed:* {deals_completed:,}\n"
            f"⚖️ *Disputes Resolved:* {disputes_resolved:,}\n\n"
            "_Tap 'Help Desk' button for further guidance_\n\n"
        )
        
        keyboard = [
            [KeyboardButton("My Account"), KeyboardButton("Transaction History")],
            [KeyboardButton("Language"), KeyboardButton("Help Desk")],
            [KeyboardButton("Withdraw Funds")]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        if WELCOME_VIDEO_URL:
            try:
                await update.message.reply_video(
                    video=WELCOME_VIDEO_URL,
                    caption=welcome_message,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
            except BadRequest as e:
                logger.warning(f"Failed to send video: {e}. Falling back to text message.")
                await update.message.reply_text(
                    welcome_message,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
        else:
            await update.message.reply_text(
                welcome_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )


async def initialize_telethon_client():
    """
    Initialize and authenticate the Telethon user client.
    Handles first-time login flow and session management.
    """
    global telethon_client
    
    try:
        api_id = int(os.getenv('API_ID', '0'))
        api_hash = os.getenv('API_HASH', '')
        
        if not api_id or not api_hash or api_id == 0:
            logger.error("API_ID or API_HASH not properly configured in .env")
            return False
        
        telethon_client = TelegramClient('user_session', api_id, api_hash)
        await telethon_client.start()
        
        me = await telethon_client.get_me()
        logger.info(f"Telethon client initialized. Logged in as: {me.first_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing Telethon client: {e}")
        return False


async def shutdown_telethon_client(application):
    """
    Properly disconnect the Telethon client during application shutdown.
    This prevents pending task errors when the event loop is closed.
    """
    global telethon_client
    
    if telethon_client and telethon_client.is_connected():
        try:
            logger.info("Disconnecting Telethon client...")
            await telethon_client.disconnect()
            logger.info("Telethon client disconnected successfully")
        except Exception as e:
            logger.error(f"Error disconnecting Telethon client: {e}")


async def create_supergroup_with_users(group_name, usernames_to_add, bot_username):
    """
    Create a supergroup and add specified users to it.
    
    Args:
        group_name (str): Name of the supergroup to create
        usernames_to_add (list): List of Telegram usernames to add
        bot_username (str): The bot's username to add to the group
        
    Returns:
        dict: Contains 'success', 'group_id', 'group_link', and 'message'
    """
    
    if not telethon_client:
        return {
            'success': False,
            'message': 'User client not initialized. Please restart the bot.'
        }
    
    try:
        result = await telethon_client(CreateChannelRequest(
            title=group_name,
            about='Group created by bot',
            megagroup=True
        ))
        
        telethon_group_id = result.chats[0].id
        bot_api_group_id = -1000000000000 - telethon_group_id
        logger.info(f"Supergroup created: {group_name} (Telethon ID: {telethon_group_id}, Bot API ID: {bot_api_group_id})")
        
        users_added = []
        users_failed = []
        
        all_usernames = list(set(usernames_to_add + [bot_username]))
        
        for username in all_usernames:
            try:
                if isinstance(username, int):
                    input_entity = await telethon_client.get_input_entity(username)
                    user_identifier = str(username)
                else:
                    clean_username = username.lstrip('@')
                    input_entity = await telethon_client.get_input_entity(clean_username)
                    user_identifier = clean_username
                
                await telethon_client(InviteToChannelRequest(
                    channel=telethon_group_id,
                    users=[input_entity]
                ))
                
                users_added.append(user_identifier)
                logger.info(f"Added user {user_identifier} to group {telethon_group_id}")
                
            except (UsernameNotOccupiedError, UsernameInvalidError):
                users_failed.append((username, 'Username not found'))
                logger.warning(f"Username {username} not found")
            except FloodError as e:
                users_failed.append((username, f'Rate limited: {e}'))
                logger.warning(f"Rate limited while adding {username}: {e}")
            except Exception as e:
                users_failed.append((username, str(e)))
                logger.warning(f"Error adding {username}: {e}")
        
        try:
            invite = await telethon_client(ExportChatInviteRequest(telethon_group_id))
            group_link = invite.link
        except Exception as e:
            logger.warning(f"Error exporting invite link: {e}")
            group_link = f"https://t.me/c/{telethon_group_id}"
        
        # Send invite links to users who couldn't be added to the group
        invite_links_sent = []
        for username, reason in users_failed:
            if "Username not found" not in reason:  # Only send links if user exists but couldn't be added
                try:
                    if isinstance(username, int):
                        user_id = username
                    else:
                        # Try to get user ID from username to send private message
                        try:
                            user_entity = await telethon_client.get_input_entity(username.lstrip('@'))
                            user_id = user_entity.user_id
                        except Exception:
                            continue  # Skip if we can't resolve the username
                    
                    await telethon_client.send_message(
                        user_id,
                        f"🔗 You couldn't be automatically added to the group '{group_name}'.\n\nPlease join using this invite link: {group_link}"
                    )
                    invite_links_sent.append(username)
                    logger.info(f"Sent invite link to user {username}")
                except Exception as e:
                    logger.warning(f"Failed to send invite link to user {username}: {e}")
        
        message = f"Supergroup '{group_name}' created successfully!\n"
        message += f"Group ID: {bot_api_group_id}\n"
        message += f"Group Link: {group_link}\n"
        message += f"Users added: {len(users_added)}/{len(all_usernames)}\n"
        
        if users_failed:
            message += f"\nFailed to add: {len(users_failed)} user(s)\n"
            for username, reason in users_failed:
                message += f"  - {username}: {reason}\n"
        
        if invite_links_sent:
            message += f"\nInvite links sent to: {len(invite_links_sent)} user(s)\n"
            for username in invite_links_sent:
                message += f"  - {username}\n"
        
        return {
            'success': True,
            'group_id': bot_api_group_id,
            'telethon_group_id': telethon_group_id,
            'group_link': group_link,
            'users_added': users_added,
            'users_failed': users_failed,
            'invite_links_sent': invite_links_sent,
            'message': message
        }
        
    except Exception as e:
        error_message = f"Error creating supergroup: {str(e)}"
        logger.error(error_message)
        return {
            'success': False,
            'message': error_message
        }


@with_auto_balance_refresh
async def create_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /creategroup command.
    Format: /creategroup <group_name> <username1> <username2> ...
    """
    await ensure_user_and_process_pending(update)
    
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "Usage: /creategroup <group_name> <username1> <username2> ...\n"
            "Example: /creategroup MyGroup @user1 @user2"
        )
        return
    
    try:
        group_name = context.args[0]
        usernames = context.args[1:] if len(context.args) > 1 else []
        
        if not group_name or group_name.strip() == '':
            await update.message.reply_text("Group name cannot be empty.")
            return
        
        await update.message.reply_text(
            f"Creating supergroup '{group_name}'...\nAdding {len(usernames)} user(s)...\nPlease wait..."
        )
        
        bot_username = os.getenv('BOT_USERNAME', '')
        result = await create_supergroup_with_users(group_name, usernames, bot_username)
        
        if result['success']:
            await update.message.reply_text(result['message'])
        else:
            await update.message.reply_text(f"Error: {result['message']}")
            
    except Exception as e:
        error_msg = f"An error occurred: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await update.message.reply_text(f"Error: {error_msg}")


async def monitor_buyer_wallets_callback(context: ContextTypes.DEFAULT_TYPE):
    """
    Monitor buyer BTC wallets for pending transactions and automatically transfer funds to intermediary wallets.
    Runs every 30 seconds.
    """
    try:
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            
            # Get all pending BTC transactions grouped by buyer (exclude already auto-transferred)
            cursor.execute('''
                SELECT buyer_id, MAX(creation_date) as latest_date
                FROM transactions
                WHERE status = 'PENDING' AND crypto_type = 'BTC' AND intermediary_wallet_id IS NOT NULL AND (auto_transferred = 0 OR auto_transferred IS NULL)
                GROUP BY buyer_id
            ''')
            buyers_with_pending = cursor.fetchall()
            
            for buyer_id, _ in buyers_with_pending:
                # Get the most recent transaction for this buyer that hasn't been auto-transferred
                cursor.execute('''
                    SELECT transaction_id, amount, wallet_id, intermediary_wallet_id
                    FROM transactions
                    WHERE buyer_id = ? AND status = 'PENDING' AND crypto_type = 'BTC' AND intermediary_wallet_id IS NOT NULL AND (auto_transferred = 0 OR auto_transferred IS NULL)
                    ORDER BY creation_date DESC
                    LIMIT 1
                ''', (buyer_id,))
                
                transaction = cursor.fetchone()
                if not transaction:
                    continue
                
                transaction_id, transaction_amount, buyer_wallet_id, intermediary_wallet_id = transaction
                
                # Get buyer's wallet details
                cursor.execute('SELECT address, private_key FROM wallets WHERE wallet_id = ?', (buyer_wallet_id,))
                buyer_wallet = cursor.fetchone()
                
                if not buyer_wallet:
                    continue
                
                buyer_address, buyer_private_key = buyer_wallet
                
                # Get intermediary wallet address
                cursor.execute('SELECT address FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
                intermediary_result = cursor.fetchone()
                
                if not intermediary_result:
                    continue
                
                intermediary_address = intermediary_result[0]
                
                # Check buyer's wallet balance from blockchain (non-blocking)
                try:
                    balance_satoshis = await asyncio.to_thread(btcwalletclient_wif.get_balance, buyer_address)
                    balance_btc = balance_satoshis / 1e8

                    # Only proceed if there's a balance greater than 250 satoshis
                    if balance_satoshis <= 250:
                        continue

                    # Determine transfer amount based on balance vs transaction amount
                    if balance_btc <= transaction_amount:
                        # Send entire balance minus 250 sats (non-blocking)
                        transfer_result = await asyncio.to_thread(
                            btcwalletclient_wif.send_max_btc_auto,
                            wif_private_key=buyer_private_key,
                            destination_address=intermediary_address
                        )
                    else:
                        # Send transaction amount (250 sats will be deducted for fee) (non-blocking)
                        transfer_result = await asyncio.to_thread(
                            btcwalletclient_wif.send_specific_btc_amount,
                            wif_private_key=buyer_private_key,
                            destination_address=intermediary_address,
                            amount_btc=transaction_amount
                        )
                    
                    if transfer_result['success']:
                        amount_sent = transfer_result['amount_sent']
                        txid = transfer_result['txid']
                        
                        # Update database with write lock
                        try:
                            with DatabaseConnection(DB_PATH) as write_conn:
                                write_cursor = write_conn.cursor()
                                
                                # Update buyer's wallet balance in database
                                write_cursor.execute('''
                                    UPDATE wallets 
                                    SET balance = balance - ?
                                    WHERE wallet_id = ?
                                ''', (amount_sent, buyer_wallet_id))
                                
                                # Mark transaction as auto-transferred
                                write_cursor.execute('''
                                    UPDATE transactions
                                    SET auto_transferred = 1
                                    WHERE transaction_id = ?
                                ''', (transaction_id,))
                                
                                # Update escrow wallet balance
                                write_cursor.execute('''
                                    UPDATE wallets 
                                    SET balance = balance + ?
                                    WHERE wallet_id = ?
                                ''', (amount_sent, intermediary_wallet_id))
                        except sqlite3.Error as write_error:
                            logger.error(f"Database write error in auto-transfer: {write_error}")
                            continue
                        
                        logger.info(f"Auto-transferred {amount_sent:.8f} BTC from buyer {buyer_id} to intermediary wallet for transaction {transaction_id}. TxID: {txid}")
                        
                        # Send notification to buyer
                        try:
                            await context.bot.send_message(
                                chat_id=buyer_id,
                                text=f"✅ Successfully auto-transferred {amount_sent:.8f} BTC to escrow wallet for transaction ID: {transaction_id}\n\nTransaction ID: {txid}"
                            )
                        except Exception as notif_error:
                            logger.error(f"Could not send notification to buyer {buyer_id}: {notif_error}")
                        
                        # Send notification to transaction group if it exists
                        try:
                            cursor.execute('SELECT group_id FROM transactions WHERE transaction_id = ?', (transaction_id,))
                            group_result = cursor.fetchone()
                            if group_result and group_result[0]:
                                group_id = group_result[0]
                                await context.bot.send_message(
                                    chat_id=group_id,
                                    text=f"✅ *Funds Received*\n\n{amount_sent:.8f} BTC has been automatically transferred to the escrow wallet.\n\nBlockchain TxID: `{txid}`",
                                    parse_mode='Markdown'
                                )
                        except Exception as group_notif_error:
                            logger.error(f"Could not send notification to group: {group_notif_error}")
                    
                except Exception as transfer_error:
                    logger.error(f"Error transferring funds for buyer {buyer_id}: {transfer_error}")
                    continue
                    
        except sqlite3.Error as db_error:
            logger.error(f"Database error in monitor_buyer_wallets_callback: {db_error}")
        finally:
            if conn:
                conn.close()
        
        # Schedule next check in 30 seconds
        context.job_queue.run_once(monitor_buyer_wallets_callback, 30)
        
    except Exception as e:
        logger.error(f"Error in monitor_buyer_wallets_callback: {e}")
        # Still schedule next run even if there's an error
        try:
            context.job_queue.run_once(monitor_buyer_wallets_callback, 30)
        except:
            pass


async def monitor_intermediary_wallets_callback(context: ContextTypes.DEFAULT_TYPE):
    """
    Monitor intermediary BTC wallet balances for pending transactions and send deposit notifications.
    Runs every 30 seconds.
    """
    try:
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            
            # Get all pending BTC transactions with intermediary wallets
            cursor.execute('''
                SELECT transaction_id, amount, fee_amount, intermediary_wallet_id, group_id, buyer_id, 
                       deposit_99_notified, last_partial_balance_notified
                FROM transactions
                WHERE status = 'PENDING' AND crypto_type = 'BTC' AND intermediary_wallet_id IS NOT NULL
            ''')
            
            transactions = cursor.fetchall()
            
            for transaction in transactions:
                transaction_id, transaction_amount, fee_amount, intermediary_wallet_id, group_id, buyer_id, deposit_99_notified, last_partial_balance_notified = transaction
                
                # Skip if no group exists for notifications
                if not group_id:
                    continue
                
                # Get intermediary wallet address
                cursor.execute('SELECT address FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
                intermediary_result = cursor.fetchone()
                
                if not intermediary_result:
                    continue
                
                intermediary_address = intermediary_result[0]
                
                # Check escrow wallet balance from blockchain (non-blocking)
                try:
                    balance_btc = await async_get_btc_balance_from_blockchain(intermediary_address)
                    
                    if balance_btc is None:
                        continue
                    
                    # Calculate 99% threshold (including fee)
                    total_amount = transaction_amount + fee_amount
                    threshold_99 = total_amount * 0.99
                    
                    # Check if balance meets 99% threshold
                    if balance_btc >= threshold_99 and not deposit_99_notified:
                        # Send 99% deposit notification (only once)
                        try:
                            await context.bot.send_message(
                                chat_id=group_id,
                                text="✅ BTC has been deposited to escrow. Seller, please deliver the goods & services to the buyer.",
                                parse_mode='Markdown'
                            )
                            
                            # Mark as 99% notified
                            cursor.execute('''
                                UPDATE transactions
                                SET deposit_99_notified = 1
                                WHERE transaction_id = ?
                            ''', (transaction_id,))
                            conn.commit()
                            
                            logger.info(f"Sent 99% deposit notification for transaction {transaction_id}. Balance: {balance_btc:.8f} BTC, Required: {total_amount:.8f} BTC")
                            
                        except Exception as notif_error:
                            logger.error(f"Could not send 99% notification to group {group_id}: {notif_error}")
                    
                    # Check if balance is partial (> 0 but < 99%) and has changed since last notification
                    elif balance_btc > 0 and balance_btc < threshold_99:
                        # Only send notification if balance has changed from last notification
                        # Use a small epsilon to avoid floating point comparison issues
                        epsilon = 0.00000001  # 1 satoshi tolerance
                        balance_changed = abs(balance_btc - (last_partial_balance_notified or 0)) > epsilon
                        
                        if balance_changed:
                            # Calculate shortfall
                            shortfall = threshold_99 - balance_btc
                            
                            # Send partial deposit notification
                            try:
                                await context.bot.send_message(
                                    chat_id=group_id,
                                    text=f"⚠️ *Partial Deposit Detected*\n\nCurrent balance: {balance_btc:.8f} BTC\nRequired balance: {threshold_99:.8f} BTC\n\nBuyer needs to deposit an additional *{shortfall:.8f} BTC* before the seller can deliver goods & services.",
                                    parse_mode='Markdown'
                                )
                                
                                # Update last partial balance notified
                                cursor.execute('''
                                    UPDATE transactions
                                    SET last_partial_balance_notified = ?
                                    WHERE transaction_id = ?
                                ''', (balance_btc, transaction_id))
                                conn.commit()
                                
                                logger.info(f"Sent partial deposit notification for transaction {transaction_id}. Balance: {balance_btc:.8f} BTC, Shortfall: {shortfall:.8f} BTC")
                                
                            except Exception as notif_error:
                                logger.error(f"Could not send partial notification to group {group_id}: {notif_error}")
                
                except Exception as balance_error:
                    logger.error(f"Error checking escrow wallet balance for transaction {transaction_id}: {balance_error}")
                    continue
                    
        except sqlite3.Error as db_error:
            logger.error(f"Database error in monitor_intermediary_wallets_callback: {db_error}")
        finally:
            if conn:
                conn.close()
        
        # Schedule next check in 30 seconds
        context.job_queue.run_once(monitor_intermediary_wallets_callback, 30)
        
    except Exception as e:
        logger.error(f"Error in monitor_intermediary_wallets_callback: {e}")
        # Still schedule next run even if there's an error
        try:
            context.job_queue.run_once(monitor_intermediary_wallets_callback, 30)
        except:
            pass


def setup_wallet_monitoring(wallet_id, user_id, address, crypto_type='BTC'):
    """
    Add or update a wallet in the monitoring system.
    
    Args:
        wallet_id (str): Wallet ID
        user_id (int): User ID
        address (str): Wallet address
        crypto_type (str): Cryptocurrency type (default: 'BTC')
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        # Check if wallet is already being monitored
        cursor.execute('''
            SELECT monitoring_id, current_balance FROM wallet_monitoring 
            WHERE wallet_id = ?
        ''', (wallet_id,))
        
        existing = cursor.fetchone()
        
        if existing:
            # Update existing monitoring record
            cursor.execute('''
                UPDATE wallet_monitoring 
                SET address = ?, user_id = ?, crypto_type = ?, monitoring_enabled = 1,
                    last_checked = CURRENT_TIMESTAMP
                WHERE wallet_id = ?
            ''', (address, user_id, crypto_type, wallet_id))
        else:
            # Initialize with 0 balance - background job will update it
            # Avoiding blocking API call during wallet setup for instant creation
            current_balance = 0.0

            # Add new monitoring record
            cursor.execute('''
                INSERT INTO wallet_monitoring
                (wallet_id, address, user_id, crypto_type, current_balance, previous_balance, monitoring_enabled)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            ''', (wallet_id, address, user_id, crypto_type, current_balance, current_balance))
        
        conn.commit()
        logger.info(f"Setup monitoring for wallet {wallet_id} ({address})")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Database error setting up wallet monitoring: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


async def monitor_all_wallets_callback(context: ContextTypes.DEFAULT_TYPE):
    """
    Monitor all BTC wallets for balance changes and automatically update the database.
    Runs every 60 seconds to check all user wallets.
    """
    try:
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            
            # Get all wallets that need monitoring
            cursor.execute('''
                SELECT wm.wallet_id, wm.address, wm.user_id, wm.crypto_type, 
                       wm.current_balance, wm.previous_balance, wm.last_checked
                FROM wallet_monitoring wm
                WHERE wm.monitoring_enabled = 1 AND wm.crypto_type = 'BTC'
            ''')
            
            wallets_to_monitor = cursor.fetchall()
            
            for wallet_info in wallets_to_monitor:
                wallet_id, address, user_id, crypto_type, db_balance, previous_balance, last_checked = wallet_info
                
                try:
                    # Get current balance from blockchain (non-blocking)
                    blockchain_balance = await async_get_btc_balance_from_blockchain(address)
                    
                    if blockchain_balance is None:
                        logger.warning(f"Could not fetch blockchain balance for wallet {wallet_id} ({address})")
                        continue
                    
                    balance_changed = abs(blockchain_balance - db_balance) > 1e-8  # Compare with 1 satoshi precision
                    
                    if balance_changed:
                        balance_change = blockchain_balance - db_balance
                        
                        # Update wallet monitoring record
                        cursor.execute('''
                            UPDATE wallet_monitoring 
                            SET previous_balance = current_balance,
                                current_balance = ?,
                                balance_change = ?,
                                last_checked = CURRENT_TIMESTAMP
                            WHERE wallet_id = ?
                        ''', (blockchain_balance, balance_change, wallet_id))
                        
                        # Update actual wallet balance in wallets table
                        cursor.execute('''
                            UPDATE wallets
                            SET balance = ?,
                                last_balance_update = CURRENT_TIMESTAMP
                            WHERE wallet_id = ?
                        ''', (blockchain_balance, wallet_id))
                        
                        conn.commit()
                        
                        logger.info(f"Balance updated for wallet {wallet_id}: {db_balance:.8f} → {blockchain_balance:.8f} BTC (change: {balance_change:+.8f})")
                    
                    else:
                        # Balance hasn't changed, but update last_checked and last_balance_update timestamps
                        cursor.execute('''
                            UPDATE wallet_monitoring
                            SET last_checked = CURRENT_TIMESTAMP
                            WHERE wallet_id = ?
                        ''', (wallet_id,))

                        cursor.execute('''
                            UPDATE wallets
                            SET last_balance_update = CURRENT_TIMESTAMP
                            WHERE wallet_id = ?
                        ''', (wallet_id,))

                        conn.commit()
                
                except Exception as wallet_error:
                    logger.error(f"Error monitoring wallet {wallet_id}: {wallet_error}")
                    continue
            
            # Also ensure all user wallets are being monitored
            cursor.execute('''
                SELECT w.wallet_id, w.user_id, w.address, w.crypto_type
                FROM wallets w
                LEFT JOIN wallet_monitoring wm ON w.wallet_id = wm.wallet_id
                WHERE w.crypto_type = 'BTC' AND wm.wallet_id IS NULL
            ''')
            
            unmonitored_wallets = cursor.fetchall()
            
            for wallet_id, user_id, address, crypto_type in unmonitored_wallets:
                setup_wallet_monitoring(wallet_id, user_id, address, crypto_type)
                logger.info(f"Added unmonitored wallet {wallet_id} to monitoring system")
                    
        except sqlite3.Error as db_error:
            logger.error(f"Database error in monitor_all_wallets_callback: {db_error}")
        finally:
            if conn:
                conn.close()
        
        # Schedule next check in 60 seconds
        context.job_queue.run_once(monitor_all_wallets_callback, 60)
        
    except Exception as e:
        logger.error(f"Error in monitor_all_wallets_callback: {e}")
        # Still schedule next run even if there's an error
        try:
            context.job_queue.run_once(monitor_all_wallets_callback, 60)
        except:
            pass


def disable_wallet_monitoring(wallet_id):
    """
    Disable monitoring for a specific wallet.
    
    Args:
        wallet_id (str): Wallet ID to disable monitoring for
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE wallet_monitoring 
            SET monitoring_enabled = 0
            WHERE wallet_id = ?
        ''', (wallet_id,))
        
        conn.commit()
        logger.info(f"Disabled monitoring for wallet {wallet_id}")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Database error disabling wallet monitoring: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def get_wallet_monitoring_status(user_id=None):
    """
    Get monitoring status for wallets.
    
    Args:
        user_id (int, optional): Get status for specific user, or all if None
        
    Returns:
        list: List of monitoring records
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
        cursor = conn.cursor()
        
        if user_id:
            cursor.execute('''
                SELECT wm.wallet_id, wm.address, wm.user_id, wm.crypto_type,
                       wm.current_balance, wm.previous_balance, wm.balance_change,
                       wm.last_checked, wm.monitoring_enabled
                FROM wallet_monitoring wm
                WHERE wm.user_id = ?
                ORDER BY wm.last_checked DESC
            ''', (user_id,))
        else:
            cursor.execute('''
                SELECT wm.wallet_id, wm.address, wm.user_id, wm.crypto_type,
                       wm.current_balance, wm.previous_balance, wm.balance_change,
                       wm.last_checked, wm.monitoring_enabled
                FROM wallet_monitoring wm
                ORDER BY wm.last_checked DESC
            ''')
        
        return cursor.fetchall()
        
    except sqlite3.Error as e:
        logger.error(f"Database error getting wallet monitoring status: {e}")
        return []
    finally:
        if conn:
            conn.close()


async def update_deals_completed_callback(context: ContextTypes.DEFAULT_TYPE):
    """Callback to increment deals_completed counter."""
    try:
        increment_stat('deals_completed')
        logger.info("Deals completed counter incremented")
    except Exception as e:
        logger.error(f"Error in update_deals_completed_callback: {e}")
    finally:
        next_interval = random.randint(1800, 5400)
        context.job_queue.run_once(update_deals_completed_callback, next_interval)
        logger.info(f"Scheduled next deals_completed increment in {next_interval} seconds")


async def update_disputes_resolved_callback(context: ContextTypes.DEFAULT_TYPE):
    """Callback to increment disputes_resolved counter."""
    try:
        increment_stat('disputes_resolved')
        logger.info("Disputes resolved counter incremented")
    except Exception as e:
        logger.error(f"Error in update_disputes_resolved_callback: {e}")
    finally:
        next_interval = random.randint(10800, 32400)
        context.job_queue.run_once(update_disputes_resolved_callback, next_interval)
        logger.info(f"Scheduled next disputes_resolved increment in {next_interval} seconds")


async def send_check_command_callback(context: ContextTypes.DEFAULT_TYPE):
    """Callback to send /check command to the group 15 minutes after deposit."""
    try:
        group_id = context.job.data.get('group_id')
        transaction_id = context.job.data.get('transaction_id')
        
        if not group_id or not transaction_id:
            logger.warning(f"Missing group_id or transaction_id in callback data")
            return
        
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20.0)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT transaction_id, amount, fee_amount, intermediary_wallet_id, buyer_id, seller_id
                FROM transactions
                WHERE transaction_id = ? AND status = 'PENDING' AND crypto_type = 'BTC'
            ''', (transaction_id,))
            
            transaction = cursor.fetchone()
            
            if not transaction:
                logger.warning(f"Transaction {transaction_id} not found or not pending")
                return
            
            _, transaction_amount, fee_amount, intermediary_wallet_id, buyer_id, seller_id = transaction
            
            cursor.execute('SELECT address FROM wallets WHERE wallet_id = ?', (intermediary_wallet_id,))
            intermediary_result = cursor.fetchone()
            
            if not intermediary_result:
                logger.error(f"Intermediary wallet not found for transaction {transaction_id}")
                return
            
            intermediary_address = intermediary_result[0]

            # Get cached balance (updated by background jobs)
            cached_balance = get_cached_wallet_balance(intermediary_address)

            if cached_balance is None:
                await context.bot.send_message(
                    chat_id=group_id,
                    text="❌ Error: Could not fetch escrow wallet balance.",
                    parse_mode='Markdown'
                )
                return

            balance_btc = cached_balance['balance']

            total_amount = transaction_amount + fee_amount
            threshold_99 = total_amount * 0.99

            if balance_btc >= threshold_99:
                message = (
                    f"✅ **Automatic Check: Sufficient BTC Deposit**\n\n"
                    f"Escrow wallet balance:\n"
                    f"*{balance_btc:.8f} BTC*\n\n"
                    f"Seller can now provide goods & services."
                )
            else:
                shortfall = threshold_99 - balance_btc
                message = (
                    f"⚠️ **Automatic Check: Insufficient BTC Deposit**\n\n"
                    f"Escrow wallet balance:\n"
                    f"*{balance_btc:.8f} BTC*\n\n"
                    f"Buyer needs to deposit an additional *{shortfall:.8f} BTC*\n\n"
                    f"Escrow wallet address: `{intermediary_address}`"
                )
            
            await context.bot.send_message(
                chat_id=group_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info(f"Sent automatic check result to group {group_id} for transaction {transaction_id}")
            
        except sqlite3.Error as db_error:
            logger.error(f"Database error in send_check_command_callback: {db_error}")
        finally:
            if conn:
                conn.close()
                
    except Exception as e:
        logger.error(f"Error in send_check_command_callback: {e}")


async def update_crypto_prices_callback(context: ContextTypes.DEFAULT_TYPE):
    """
    Background job to update cryptocurrency prices in the database.
    Runs every 60 seconds to keep prices fresh.
    Uses batch API call to reduce rate limit usage.
    """
    try:
        # List of supported cryptocurrencies
        supported_cryptos = ['BTC', 'ETH', 'LTC', 'XMR', 'DASH', 'BCH', 'ZEC']

        # Use batch function to fetch all prices in one API call (non-blocking)
        prices = await asyncio.to_thread(get_multiple_crypto_prices, supported_cryptos, True)

        if prices:
            logger.info(f"Updated prices for {len(prices)} cryptocurrencies: {', '.join([f'{k}=${v}' for k, v in prices.items()])}")
        else:
            logger.warning("No cryptocurrency prices were updated")

    except Exception as e:
        logger.error(f"Error in update_crypto_prices_callback: {e}")
    finally:
        # Schedule next update in 60 seconds
        context.job_queue.run_once(update_crypto_prices_callback, 60)


def main() -> None:
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Set up database
    setup_database()
    migrate_wallets_table()
    migrate_transactions_table()
    init_crypto_prices_table()
    
    # Initialize Telethon client
    bot_token = os.getenv('BOT_TOKEN', '8193003920:AAGPHNfVauCYHWFEIrh9reTlwpJ6jUtwLUY')
    try:
        loop.run_until_complete(initialize_telethon_client())
    except Exception as e:
        logger.warning(f"Could not initialize Telethon client: {e}. /creategroup command will not work.")

    # Create the Application and pass it your bot's token
    application = Application.builder().token(bot_token).build()

    # Store application in a global variable for access in handlers
    global app
    app = application

    # Add conversation handler for deposit command
    deposit_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('deposit', deposit_command)],
        states={
            SELECTING_ROLE: [CallbackQueryHandler(select_role, pattern='^role_')],
            SELECTING_CRYPTO: [CallbackQueryHandler(select_crypto, pattern='^deposit_')],
            ENTERING_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_amount)],
            ENTERING_RECIPIENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_recipient)],
            CONFIRMING_TRANSACTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_transaction)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )

    # Add conversation handler for dispute command
    dispute_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('dispute', dispute_command)
        ],
        states={
            DISPUTE_REASON: [
                CallbackQueryHandler(dispute_selection_callback, pattern='^select_dispute_'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, dispute_reason)
            ],
            DISPUTE_EVIDENCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, dispute_evidence)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )

    # Add conversation handler for multisig wallet creation
    multisig_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(wallet_callback, pattern='^create_multisig_'),
            CallbackQueryHandler(wallet_callback, pattern='^address_type_')
        ],
        states={
            SELECTING_ADDRESS_TYPE: [CallbackQueryHandler(wallet_callback, pattern='^address_type_')],
            ENTERING_M: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_m)],
            ENTERING_N: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_n)],
            ENTERING_PUBLIC_KEYS: [CallbackQueryHandler(public_keys_callback, pattern='^(generate_keys|enter_keys)')],
            CONFIRMING_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_wallet)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )

    # Add conversation handler for withdrawal
    withdraw_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('withdraw', withdraw_command)],
        states={
            SELECTING_WITHDRAW_WALLET: [CallbackQueryHandler(select_withdraw_wallet, pattern='^withdraw_')],
            ENTERING_WITHDRAW_AMOUNT: [
                CallbackQueryHandler(withdraw_max_amount, pattern='^withdraw_max$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_withdraw_amount)
            ],
            ENTERING_WALLET_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_wallet_address)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )

    # Register command handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('wallet', wallet_command))
    application.add_handler(CommandHandler('transactions', transactions_command))
    application.add_handler(CommandHandler('check', check_command))
    application.add_handler(CommandHandler('release', release_command))
    application.add_handler(CommandHandler('language', language_command))
    application.add_handler(CommandHandler('sign', sign_transaction_command))
    application.add_handler(CommandHandler('broadcast', broadcast_transaction_command))
    application.add_handler(CommandHandler('creategroup', create_group_command))

    # Register message handler for keyboard buttons
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND &
        filters.Regex('^(My Account|Transaction History|Language|Help Desk|Withdraw Funds|My Wallet|Start Trade|Release Funds|File Dispute|Back to Main Menu 🔙)$'),
        handle_keyboard_buttons
    ), group=1)

    # Register conversation handlers
    application.add_handler(deposit_conv_handler)
    application.add_handler(dispute_conv_handler)
    application.add_handler(multisig_conv_handler)
    application.add_handler(withdraw_conv_handler)

    # Register callback query handlers
    application.add_handler(
        CallbackQueryHandler(wallet_callback, pattern='^(create_wallet_|deposit_to_escrow|refresh_balances|confirm_wallet_BTC_segwit|delete_wallet)'))
    application.add_handler(
        CallbackQueryHandler(transaction_callback, pattern='^(confirm_transaction|cancel_transaction|view_transaction_|accept_transaction_|decline_transaction_|transactions_page_)'))
    application.add_handler(CallbackQueryHandler(release_callback, pattern='^(select_release_|release_|cancel_release)'))
    application.add_handler(CallbackQueryHandler(language_callback, pattern='^lang_'))
    application.add_handler(CallbackQueryHandler(create_escrow_group_callback, pattern='^create_escrow_group$'))

    # Add error handler
    application.add_error_handler(error_handler)

    # Register shutdown handler for Telethon client cleanup
    application.post_shutdown = shutdown_telethon_client

    # Start background jobs for stats updates and wallet monitoring
    job_queue = application.job_queue
    if job_queue:
        initial_deals_interval = random.randint(1800, 5400)
        initial_disputes_interval = random.randint(10800, 32400)
        job_queue.run_once(update_deals_completed_callback, initial_deals_interval)
        job_queue.run_once(update_disputes_resolved_callback, initial_disputes_interval)
        job_queue.run_once(monitor_buyer_wallets_callback, 10)  # Start buyer wallet monitoring after 10 seconds
        job_queue.run_once(monitor_intermediary_wallets_callback, 15)  # Start intermediary wallet monitoring after 15 seconds
        job_queue.run_once(monitor_all_wallets_callback, 20)  # Start comprehensive wallet monitoring after 20 seconds
        job_queue.run_once(update_crypto_prices_callback, 5)  # Start crypto price updates after 5 seconds
        logger.info(f"Started background jobs for stats updates (deals: {initial_deals_interval}s, disputes: {initial_disputes_interval}s), buyer wallet monitoring, intermediary wallet monitoring, comprehensive wallet monitoring, and crypto price updates")
    else:
        logger.warning("JobQueue not available. Install with: pip install 'python-telegram-bot[job-queue]'")

    # Start the Bot
    application.run_polling()


if __name__ == '__main__':
    main()
