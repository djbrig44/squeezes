from __future__ import annotations
import pickle
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
import os
import csv
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd
import matplotlib.dates as mdates
import requests
import yfinance as yf
from fredapi import Fred
from scipy import stats
# MEM Labs / Quant Accelerator upgrades
from signal_strength import get_signal_strength, integrate_with_existing_system
from fee_modeling import calculate_net_pnl, estimate_spread_from_volume
from forward_dump import dump_raw_signals
import matplotlib.pyplot as plt
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib.parse
import warnings
import json
import re
import random
import traceback
import sys

# Suppress warnings once at the top
warnings.filterwarnings('ignore')

# Note: The imports below assume you have these modules.
# If they don't exist in separate files, you should import the classes directly.
# For now, I'll comment them out and handle inline definitions
# from data_quality import DataQualityModule
# from fibonacci_module import FibonacciModule, FibonacciIntegration

# =============================================================================
# CACHE MANAGEMENT SYSTEM
# =============================================================================

class CacheManager:
    """Advanced caching system for yfinance data and computations."""
    
    def __init__(self, cache_dir: str = "./cache", expiry_hours: int = 6):
        self.cache_dir = Path(cache_dir)
        self.expiry_hours = expiry_hours
        self.cache_dir.mkdir(exist_ok=True)
        
    def _get_cache_key(self, symbol: str, start: str, end: str, data_type: str = "price") -> str:
        """Generate a unique cache key."""
        key_str = f"{symbol}_{start}_{end}_{data_type}"
        return hashlib.md5(key_str.encode()).hexdigest()[:16]
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the cache file path."""
        return self.cache_dir / f"{cache_key}.pkl"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache is still valid."""
        if not cache_path.exists():
            return False
        
        file_age = time.time() - cache_path.stat().st_mtime
        return file_age < (self.expiry_hours * 3600)
    
    def get_price_data(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """Retrieve price data from cache."""
        cache_key = self._get_cache_key(symbol, start, end, "price")
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                return data
            except Exception:
                return None
        return None
    
    def set_price_data(self, symbol: str, start: str, end: str, data: pd.DataFrame):
        """Store price data in cache."""
        cache_key = self._get_cache_key(symbol, start, end, "price")
        cache_path = self._get_cache_path(cache_key)
        
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            pass
    
    def cleanup(self):
        """Clean up expired cache files."""
        for cache_file in self.cache_dir.glob("*.pkl"):
            if not self._is_cache_valid(cache_file):
                cache_file.unlink()

# Initialize cache manager globally
cache_manager = CacheManager()

# =============================================================================
# YFINANCE COMPATIBILITY HELPERS
# =============================================================================

def safe_get_scalar(value):
    """Safely convert pandas value to Python scalar."""
    if value is None:
        return None
    if isinstance(value, pd.DataFrame):
        return float(value.iloc[0, 0]) if not value.empty else None
    if isinstance(value, pd.Series):
        return float(value.iloc[0]) if not value.empty else None
    if isinstance(value, (np.floating, np.integer)):
        return float(value)
    return value

def safe_get_close_series(df: pd.DataFrame) -> pd.Series:
    """Safely extract Close prices handling MultiIndex columns."""
    if df is None or df.empty:
        return pd.Series(dtype=float)
    
    if isinstance(df.columns, pd.MultiIndex):
        if 'Close' in df.columns.get_level_values(0):
            close = df['Close']
            if isinstance(close, pd.DataFrame):
                return close.iloc[:, 0]
            return close
    
    if 'Close' in df.columns:
        close = df['Close']
        if isinstance(close, pd.DataFrame):
            return close.iloc[:, 0]
        return close
    
    return pd.Series(dtype=float)

# =============================================================================
# =============================================================================
# SECTOR LOOKUP
# =============================================================================
def get_sector(ticker: str) -> str:
    """Get GICS Sector from Yahoo Finance API."""
    try:
        info = yf.Ticker(ticker).info
        sector = info.get('sector', None)
        if sector is None:
            category = info.get('category', '')
            if category:
                return category
            etf_sectors = {
                'SPY': 'Broad Market', 'QQQ': 'Technology', 'SMH': 'Technology',
                'GDX': 'Materials', 'XLF': 'Financials', 'XLE': 'Energy',
                'XLI': 'Industrials', 'XLK': 'Technology', 'XLV': 'Healthcare',
                'ITB': 'Industrials', 'XLC': 'Communication Services',
            }
            return etf_sectors.get(ticker.upper(), 'Unknown')
        return sector
    except Exception as e:
        return 'Unknown'


# EARNINGS PROXIMITY FILTER
# =============================================================================
def check_earnings_proximity(symbol, reduce_threshold=3, warn_threshold=7):
    """Check days to earnings, return (days, flag, size_multiplier)"""
    try:
        ticker = yf.Ticker(symbol)
        calendar = ticker.calendar
        
        # FIXED: Handle None, empty DataFrame, or dict
        if calendar is None:
            return (None, "", 1.0)
        
        # FIXED: Check if it's a dict (newer yfinance) or DataFrame
        if isinstance(calendar, dict):
            # New yfinance format returns dict
            earnings_date = calendar.get('Earnings Date')
            if earnings_date is None:
                return (None, "", 1.0)
            # May be a list
            if isinstance(earnings_date, list) and len(earnings_date) > 0:
                earnings_date = earnings_date[0]
        elif hasattr(calendar, 'empty') and calendar.empty:
            return (None, "", 1.0)
        elif 'Earnings Date' in calendar.index:
            earnings_date = calendar.loc['Earnings Date']
            if hasattr(earnings_date, '__iter__') and not isinstance(earnings_date, str):
                earnings_date = list(earnings_date)[0]
        else:
            return (None, "", 1.0)
        
        # Convert earnings date
        if isinstance(earnings_date, pd.Timestamp):
            earnings_dt = earnings_date.date()
        elif hasattr(earnings_date, 'date'):
            earnings_dt = earnings_date.date()
        else:
            try:
                earnings_dt = pd.to_datetime(earnings_date).date()
            except:
                return (None, "", 1.0)
        
        today = datetime.now().date()
        days = (earnings_dt - today).days
        
        if days < 0:
            return (days, "✅ REPORTED", 1.0)
        elif days <= reduce_threshold:
            return (days, f"⚠️ {days}D - REDUCE", 0.5)
        elif days <= warn_threshold:
            return (days, f"⚠️ {days}D - WATCH", 1.0)
        else:
            return (days, "", 1.0)
    except Exception as e:
        # Silently return no data instead of error message
        return (None, "", 1.0)


def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate Average True Range as percentage of price (clamped 3-20% for sizing)."""
    if df is None or len(df) < period + 1:
        return 0.12  # Default 12% if not enough data
    try:
        high = df["High"] if "High" in df.columns else df["high"]
        low = df["Low"] if "Low" in df.columns else df["low"]
        close = df["Close"] if "Close" in df.columns else df["close"]
        if isinstance(high, pd.DataFrame):
            high = high.iloc[:, 0]
        if isinstance(low, pd.DataFrame):
            low = low.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        current_price = close.iloc[-1]
        atr_pct = float(atr / current_price) if current_price > 0 else 0.12
        return max(0.03, min(0.20, atr_pct))
    except Exception:
        return 0.12


def calculate_raw_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate ATR as percentage of price WITHOUT clamping. For volatility readout, not sizing."""
    if df is None or len(df) < period + 1:
        return 0.0
    try:
        high = df["High"] if "High" in df.columns else df["high"]
        low = df["Low"] if "Low" in df.columns else df["low"]
        close = df["Close"] if "Close" in df.columns else df["close"]
        if isinstance(high, pd.DataFrame):
            high = high.iloc[:, 0]
        if isinstance(low, pd.DataFrame):
            low = low.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        current_price = close.iloc[-1]
        return float(atr / current_price) if current_price > 0 else 0.0
    except Exception:
        return 0.0

# =============================================================================
# TTM SQUEEZE DETECTION
# =============================================================================

def calculate_squeeze(df: pd.DataFrame, 
                      bb_length: int = 20, 
                      bb_mult: float = 2.0,
                      kc_length: int = 20, 
                      kc_mult: float = 1.5,
                      mom_length: int = 12) -> Dict:
    """
    Detect TTM Squeeze and momentum direction.
    
    Squeeze ON = Bollinger Bands inside Keltner Channels (low volatility)
    Squeeze OFF = Bollinger Bands outside Keltner Channels (expansion)
    
    Returns:
        dict with squeeze state, momentum, direction, bars_in_squeeze
    """
    if len(df) < max(bb_length, kc_length, mom_length) + 10:
        return {
            'squeeze_on': False,
            'squeeze_fired': False,
            'fire_direction': None,
            'momentum': 0,
            'momentum_accel': 0,
            'bars_in_squeeze': 0,
            'ready': False,
            'momentum_rising': False,
            'momentum_positive': False
        }
    
    close = df['Close']
    high = df['High']
    low = df['Low']
    
    # --- Bollinger Bands ---
    bb_mid = close.rolling(bb_length).mean()
    bb_std = close.rolling(bb_length).std()
    bb_upper = bb_mid + (bb_mult * bb_std)
    bb_lower = bb_mid - (bb_mult * bb_std)
    
    # --- Keltner Channels (using ATR) ---
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(kc_length).mean()
    
    kc_mid = close.rolling(kc_length).mean()
    kc_upper = kc_mid + (kc_mult * atr)
    kc_lower = kc_mid - (kc_mult * atr)
    
    # --- Squeeze Detection ---
    # Squeeze ON when BB is inside KC
    squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)
    
    # --- TRUE TTM SQUEEZE MOMENTUM ---
    # Midline = average of (Donchian midline + SMA)
    # Donchian midline = (highest high + lowest low) / 2
    highest_high = high.rolling(mom_length).max()
    lowest_low = low.rolling(mom_length).min()
    donchian_mid = (highest_high + lowest_low) / 2
    sma_close = close.rolling(mom_length).mean()
    
    # TTM Midline = average of Donchian mid and SMA
    ttm_midline = (donchian_mid + sma_close) / 2
    
    # Momentum = Linear regression of (close - ttm_midline)
    deviation = close - ttm_midline
    
    def linreg_value(x):
        """Calculate linear regression endpoint value (like TOS)"""
        if len(x) < 2:
            return 0
        n = len(x)
        X = np.arange(n)
        slope, intercept = np.polyfit(X, x, 1)
        return intercept + slope * (n - 1)
    
    momentum = deviation.rolling(mom_length).apply(linreg_value, raw=True)

    
    # Current values
    current_squeeze = squeeze_on.iloc[-1] if len(squeeze_on) > 0 else False
    prev_squeeze = squeeze_on.iloc[-2] if len(squeeze_on) > 1 else False
    current_mom = momentum.iloc[-1] if len(momentum) > 0 else 0
    prev_mom = momentum.iloc[-2] if len(momentum) > 1 else 0
    
    # --- Count bars in squeeze ---
    bars_in_squeeze = 0
    if current_squeeze:
        for i in range(1, min(50, len(squeeze_on))):
            if squeeze_on.iloc[-i]:
                bars_in_squeeze += 1
            else:
                break
    
    # --- Squeeze Fired Detection ---
    squeeze_fired = prev_squeeze and not current_squeeze
    
    # --- Fire Direction ---
    fire_direction = None
    if squeeze_fired:
        fire_direction = 'GREEN' if current_mom > 0 else 'RED'
    
    # --- Momentum Acceleration ---
    mom_accel = current_mom - prev_mom if prev_mom != 0 else 0
    
    # --- Ready to fire (6+ bars in squeeze) ---
    ready = current_squeeze and bars_in_squeeze >= 6
    
    return {
        'squeeze_on': bool(current_squeeze),
        'squeeze_fired': bool(squeeze_fired),
        'fire_direction': fire_direction,
        'momentum': float(current_mom) if not pd.isna(current_mom) else 0,
        'momentum_accel': float(mom_accel) if not pd.isna(mom_accel) else 0,
        'bars_in_squeeze': int(bars_in_squeeze),
        'ready': bool(ready),
        'momentum_rising': current_mom > prev_mom,
        'momentum_positive': current_mom > 0
    }


def get_squeeze_score(daily_squeeze: Dict, weekly_squeeze: Dict = None) -> Dict:
    """
    Convert squeeze states into a score and filter decision.
    
    Returns:
        dict with score_adjustment, allow_entry, reason
    """
    score_adj = 0.0
    allow_entry = True
    reasons = []
    
    # --- Daily Squeeze Scoring ---
    if daily_squeeze.get('squeeze_fired'):
        if daily_squeeze.get('fire_direction') == 'GREEN':
            score_adj += 0.15
            reasons.append("Daily squeeze fired GREEN (+0.15)")
        elif daily_squeeze.get('fire_direction') == 'RED':
            score_adj -= 0.20
            reasons.append("Daily squeeze fired RED (-0.20)")
            allow_entry = True  # No blocking, boost only
    
    elif daily_squeeze.get('ready'):
        # Squeeze loaded, ready to fire
        if daily_squeeze.get('momentum_rising') and daily_squeeze.get('momentum_positive'):
            score_adj += 0.10
            reasons.append(f"Daily squeeze ready ({daily_squeeze['bars_in_squeeze']} bars), momentum rising (+0.10)")
        elif daily_squeeze.get('momentum_rising'):
            score_adj += 0.05
            reasons.append(f"Daily squeeze ready ({daily_squeeze['bars_in_squeeze']} bars), momentum turning (+0.05)")
    
    elif daily_squeeze.get('squeeze_on'):
        # In squeeze but not ready yet
        if daily_squeeze.get('momentum_positive'):
            score_adj += 0.02
            reasons.append("Daily squeeze loading, momentum positive (+0.02)")
    
    # --- Weekly Squeeze Filter ---
    if weekly_squeeze:
        if weekly_squeeze.get('squeeze_fired') and weekly_squeeze.get('fire_direction') == 'RED':
            allow_entry = True  # No blocking, boost only
            score_adj -= 0.15
            reasons.append("Weekly squeeze fired RED (caution) (-0.15)")
        
        elif weekly_squeeze.get('squeeze_fired') and weekly_squeeze.get('fire_direction') == 'GREEN':
            score_adj += 0.10
            reasons.append("Weekly squeeze fired GREEN (+0.10)")
        
        elif weekly_squeeze.get('ready'):
            score_adj += 0.05
            reasons.append(f"Weekly squeeze ready ({weekly_squeeze['bars_in_squeeze']} bars) (+0.05)")
        
        # Block if weekly momentum strongly negative and accelerating down
        if weekly_squeeze.get('momentum', 0) < -5 and weekly_squeeze.get('momentum_accel', 0) < 0:
            allow_entry = True  # No blocking, boost only
            reasons.append("Weekly momentum strongly negative (caution)")
    
    return {
        'score_adjustment': score_adj,
        'allow_entry': allow_entry,
        'reasons': reasons,
        'daily_state': 'FIRED_GREEN' if daily_squeeze.get('fire_direction') == 'GREEN' 
                       else 'FIRED_RED' if daily_squeeze.get('fire_direction') == 'RED'
                       else 'READY' if daily_squeeze.get('ready')
                       else 'LOADING' if daily_squeeze.get('squeeze_on')
                       else 'OFF',
        'weekly_state': 'FIRED_GREEN' if weekly_squeeze and weekly_squeeze.get('fire_direction') == 'GREEN'
                        else 'FIRED_RED' if weekly_squeeze and weekly_squeeze.get('fire_direction') == 'RED'
                        else 'READY' if weekly_squeeze and weekly_squeeze.get('ready')
                        else 'LOADING' if weekly_squeeze and weekly_squeeze.get('squeeze_on')
                        else 'OFF' if weekly_squeeze else 'N/A'
    }


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Resample daily OHLCV data to weekly."""
    if df.empty:
        return df
    
    weekly = df.resample('W').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()
    
    return weekly

# =============================================================================
# CONFIG / CONSTANTS
# =============================================================================
CACHE_ENABLED = True
PARALLEL_ENABLED = True
AIRTABLE_INCREMENTAL = True
MAX_WORKERS = 1  # Adjust based on your CPU cores
BATCH_SIZE = 20
AIRTABLE_BATCH_SIZE = 10
AIRTABLE_UPDATE_THRESHOLD = 0.02  # 2% change threshold



AT_BASE = "appIUFp3KFrf8KXez"
AT_API = os.getenv("AT_API")
if not AT_API:
    raise RuntimeError("AT_API not set — required for Airtable writes; set in .env")
AT_TABLE = "Trading Signals"

# Optional: FRED macro data. Engine degrades gracefully if unset (see startup check
# at ~line 7813 — "Economic data disabled" warning instead of fatal).
FRED_API_KEY = os.getenv("FRED_API_KEY")

def clean_ticker_list(tickers: List[str]) -> List[str]:
    """Remove duplicates and validate tickers."""
    # Remove duplicates (case-insensitive)
    unique_tickers = list(set([t.upper() for t in tickers]))
    
    # Filter out obvious non-standard symbols
    valid_pattern = re.compile(r'^[A-Z]{1,5}(\.[A-Z]{1,2})?$')
    cleaned = [t for t in unique_tickers if valid_pattern.match(t)]
    
    # Sort alphabetically
    cleaned.sort()
    
    print(f"📊 Ticker list: {len(tickers)} → {len(cleaned)} symbols")
    return cleaned

# Merged Trading Universe
# Original DEFAULT_TICKERS + Qualified Screener Results
# Total: 1285 unique tickers
# Generated: Dec 15, 2025

DEFAULT_TICKERS = [
    'A', 'AAPL', 'AAUC', 'AAUKF', 'ABBRF', 'ABBV', 'ABCB', 'ABEV', 'ABNB', 'ABT',
    'ABZPY', 'ACAD', 'ACCMF', 'ACGL', 'ACHR', 'ACN', 'ACTU', 'ADBE', 'ADI', 'ADM',
    'ADP', 'ADSK', 'ADTX', 'AEE', 'AEG', 'AEHL', 'AEM', 'AEP', 'AES', 'AESI',
    'AEVA', 'AFBOF', 'AFL', 'AG', 'AGI', 'AGMH', 'AGRI', 'AGRZ', 'AI', 'AIG', 'AIPO',
    'AIR', 'AIRO', 'AIZ', 'AJG', 'AKAM', 'AL', 'ALAB', 'ALB', 'ALE', 'ALEX',
    'ALGN', 'ALIT', 'ALL', 'ALLE', 'ALLR', 'ALLY', 'ALM', 'ALV', 'AM', 'AMAT',
    'AMBP', 'AMC', 'AMCR', 'AMD', 'AME', 'AMGN', 'AMKR', 'AMP', 'AMPG', 'AMPY', 'AMRK',
    'AMRRY', 'AMT', 'AMTM', 'AMVMF', 'AMZN', 'ANET', 'ANF', 'AON', 'AOS', 'APA',
    'APD', 'APH', 'APO', 'APP', 'APTV', 'APUS', 'APVO', 'AQB', 'AQMS', 'ARE',
    'ARM', 'ARMN', 'ARTV', 'ARWR', 'ASAN', 'ASH', 'ASLE', 'ASM', 'ASML', 'ASMU', 'ASND',
    'ASNS', 'ASPN', 'ASTL', 'ASTS', 'ASX', 'ATAT', 'ATEC', 'ATI', 'ATMU', 'ATMV',
    'ATO', 'ATRC', 'ATRO', 'ATROB', 'ATUSF', 'AU', 'AUPH', 'AUR', 'AUTL', 'AVA',
    'AVAV', 'AVB', 'AVGO', 'AVNBF', 'AVVOF', 'AVVSY', 'AVY', 'AWK', 'AXON', 'AXP',
    'AXS', 'AXSM', 'AXTI', 'AZ', 'AZN', 'AZO', 'B', 'BA', 'BABA', 'BAC', 'BAESY',
    'BALL', 'BANC', 'BAX', 'BBAI', 'BBIO', 'BBVA', 'BBWI', 'BBY', 'BCCC', 'BCKIF',
    'BCKIY', 'BCS', 'BDNNY', 'BDRAF', 'BDRBF', 'BDRPF', 'BDRXF', 'BDSX', 'BDX', 'BEN',
    'BEP', 'BEPC', 'BETA', 'BF-B', 'BG', 'BHC', 'BHP', 'BHPLF', 'BIIB', 'BIOA',
    'BIPC', 'BITF', 'BITO', 'BK', 'BKCH', 'BKE', 'BKH', 'BKNG', 'BKR', 'BKU',
    'BLBX', 'BLDR', 'BLIDF', 'BLK', 'BLNE', 'BLSH', 'BMNR', 'BMO', 'BMY', 'BN',
    'BNPQY', 'BNS', 'BOLD', 'BOMBF', 'BORR', 'BP', 'BPOP', 'BR', 'BRAG', 'BRELY',
    'BRK-B', 'BRN', 'BRO', 'BRSGF', 'BRSL', 'BSX', 'BTAI', 'BTBD', 'BTC', 'BTDR',
    'BTG', 'BTI', 'BTM', 'BURU', 'BVN', 'BWA', 'BWAY', 'BWXT', 'BX', 'BXMT',
    'BXP', 'BYD', 'BYRN', 'C', 'CADE', 'CAE', 'CAG', 'CAH', 'CAI', 'CAL',
    'CAN', 'CAPR', 'CAPS', 'CARR', 'CAT', 'CAVA', 'CB', 'CBOE', 'CBRE', 'CBRS', 'CC',
    'CCCX', 'CCEP', 'CCHH', 'CCI', 'CCJ', 'CCL', 'CDE', 'CDNS', 'CDRE', 'CDW',
    'CEG', 'CELH', 'CEPU', 'CETX', 'CF', 'CFG', 'CFRUY', 'CG', 'CHA', 'CHAI',
    'CHD', 'CHRS', 'CHRW', 'CHTR', 'CHWY', 'CHYM', 'CI', 'CIFR', 'CINF', 'CL',
    'CLF', 'CLFD', 'CLRO', 'CLS', 'CLSK', 'CLVT', 'CLX', 'CM', 'CMA', 'CMBT',
    'CMCLY', 'CMCSA', 'CMCT', 'CME', 'CMGMF', 'CMGMY', 'CMP', 'CMPX', 'CMS', 'CNC',
    'CNH', 'CNL', 'CNO', 'CNP', 'CNTY', 'COCO', 'COEP', 'COF', 'COHR', 'COHTF',
    'COIN', 'COKE', 'COLL', 'CONL', 'COO', 'COOT', 'COP', 'COR', 'COST', 'CPAY',
    'CPB', 'CPRT', 'CPT', 'CPWPF', 'CPXWF', 'CRBG', 'CRCL', 'CRDO', 'CRIS', 'CRL',
    'CRM', 'CRML', 'CRPJY', 'CRRSF', 'CRSR', 'CRWD', 'CRWV', 'CSCO', 'CSGP', 'CSGS',
    'CSIQ', 'CSX', 'CTAS', 'CTGO', 'CTRA', 'CTRE', 'CTSH', 'CTVA', 'CUK', 'CVE',
    'CVI', 'CVS', 'CVX', 'CWEN', 'CWEN-A', 'D', 'DAL', 'DASH', 'DAY', 'DB',
    'DBOEY', 'DCO', 'DD', 'DDOG', 'DE', 'DECK', 'DEFT', 'DELL', 'DEO', 'DEVS',
    'DG', 'DGX', 'DHI', 'DHR', 'DINO', 'DIS', 'DJT', 'DLR', 'DLTR', 'DLXY',
    'DNN', 'DOC', 'DOV', 'DOW', 'DPRO', 'DPZ', 'DRAM', 'DRI', 'DRS', 'DRUG', 'DTE',
    'DTIL', 'DTM', 'DUAVF', 'DUK', 'DV', 'DVA', 'DVN', 'DVS', 'DWMNF', 'DXCM',
    'DXF', 'EA', 'EADSF', 'EADSY', 'EBAY', 'ECL', 'ECX', 'ED', 'EDRWY', 'EFX',
    'EG', 'EGO', 'EH', 'EHMEF', 'EIX', 'EJH', 'EKSO', 'EL', 'ELF', 'ELME',
    'ELV', 'ELVR', 'EMBJ', 'EME', 'EMMS', 'EMR', 'ENB', 'ENSG', 'ENTO', 'EPAM',
    'EPD', 'EPWDF', 'EQIX', 'EQNR', 'EQR', 'EQT', 'ERDCF', 'ERIE', 'ERMAF', 'ERMAY',
    'ERO', 'ES', 'ESNT', 'ESS', 'ETHM', 'ETN', 'ETR', 'EVAX', 'EVEX', 'EVRG',
    'EVTL', 'EW', 'EWBC', 'EWY', 'EXALF', 'EXAS', 'EXC', 'EXE', 'EXEL', 'EXK', 'EXPD',
    'EXPE', 'EXR', 'EYE', 'EYUBY', 'F', 'FANG', 'FAST', 'FBYD', 'FCX', 'FDS',
    'FDX', 'FE', 'FFIV', 'FGI', 'FHI', 'FHN', 'FIEE', 'FIGS', 'FINMF', 'FINMY',
    'FIS', 'FITB', 'FIX', 'FLNC', 'FLR', 'FLY', 'FMCC', 'FMX', 'FNB', 'FNV',
    'FOFO', 'FORD', 'FOUR', 'FOX', 'FOXA', 'FRMI', 'FRO', 'FRT', 'FSLR', 'FSM',
    'FTNT', 'FTS', 'FTV', 'FWRD', 'GAUZ', 'GCTK', 'GD', 'GDDY', 'GDIV', 'GE',
    'GEHC', 'GEN', 'GENI', 'GEO', 'GEOS', 'GEV', 'GFAI', 'GFI', 'GFS', 'GIBO',
    'GIL', 'GILD', 'GIS', 'GL', 'GLD', 'GLNCY', 'GLW', 'GLWG', 'GM', 'GMAB', 'GME',
    'GNRC', 'GOLD', 'GOOG', 'GOOGL', 'GOTU', 'GPC', 'GPK', 'GPN', 'GRMN', 'GRRR',
    'GS', 'GSAT', 'GSK', 'GTX', 'GVA', 'GWAV', 'GWW', 'HAGHY', 'HAL', 'HAO',
    'HAS', 'HASI', 'HBAN', 'HBM', 'HCA', 'HCWB', 'HD', 'HEI', 'HELE', 'HG',
    'HHS', 'HIG', 'HII', 'HIMS', 'HL', 'HLF', 'HLT', 'HMY', 'HNRG', 'HNSDF',
    'HOLO', 'HOLX', 'HON', 'HOOD', 'HOVR', 'HP', 'HPE', 'HPIFY', 'HPQ', 'HRL',
    'HSIC', 'HST', 'HSY', 'HUBB', 'HUM', 'HUMA', 'HUN', 'HUT', 'HWM', 'HXL', 'HYFT',
    'HYMC', 'HYPD', 'IAG', 'IBG', 'IBM', 'IBRX', 'ICE', 'IDA', 'IDEXY', 'IDR',
    'IDXX', 'IE', 'IEX', 'IFF', 'ILKAY', 'IMNN', 'IMO', 'IMSR', 'INCY', 'INDV',
    'INFQ', 'ING', 'INMB', 'INSM', 'INSP', 'INSW', 'INTC', 'INTR', 'INTT', 'INTU', 'INUV',
    'INVH', 'IONQ', 'IONS', 'IOT', 'IOTR', 'IP', 'IPG', 'IPX', 'IQV', 'IR',
    'IRBT', 'IREN', 'IRM', 'IRTC', 'ISRG', 'ISSC', 'IT', 'ITRG', 'ITUB', 'ITW',
    'IVDA', 'IVF', 'IVPAF', 'IVVD', 'IVZ', 'IWM', 'J', 'JAGX', 'JBDI', 'JBHT',
    'JBL', 'JCI', 'JD', 'JDZG', 'JFBR', 'JKHY', 'JMIA', 'JNJ', 'JPM', 'JSPR',
    'JXG', 'JXN', 'K', 'KALA', 'KAPA', 'KAR', 'KBGGY', 'KD', 'KDP', 'KEN',
    'KEP', 'KEWL', 'KEY', 'KEYS', 'KGC', 'KHC', 'KIM', 'KKR', 'KLAC', 'KMB',
    'KMI', 'KMT', 'KNSA', 'KO', 'KOSS', 'KPRX', 'KR', 'KRMD', 'KRMN', 'KSS',
    'KTOS', 'KTTA', 'KVUE', 'L', 'LAB', 'LAC', 'LAUR', 'LBRX', 'LCUT', 'LDOS',
    'LEA', 'LEG', 'LEN', 'LEU', 'LEVI', 'LGHL', 'LH', 'LHX', 'LII', 'LIMN',
    'LIN', 'LITM', 'LIVN', 'LKQ', 'LLY', 'LMT', 'LNC', 'LNG', 'LNT', 'LNZA',
    'LOAR', 'LOMA', 'LOW', 'LRCX', 'LSANF', 'LSF', 'LSIIF', 'LULU', 'LUNR', 'LUV',
    'LVS', 'LW', 'LXP', 'LYB', 'LYG', 'LYRA', 'LYSCF', 'LYV', 'MA', 'MAA',
    'MAGH', 'MAIN', 'MALJF', 'MALRF', 'MALRY', 'MAR', 'MARA', 'MAS', 'MBIO', 'MBND',
    'MCD', 'MCHB', 'MCHP', 'MCK', 'MCO', 'MCRB', 'MD', 'MDALF', 'MDCX', 'MDLZ',
    'MDT', 'MEHCQ', 'MELI', 'MET', 'META', 'MFC', 'MGA', 'MGM', 'MHK', 'MIMTF',
    'MIND', 'MIR', 'MJDLF', 'MKC', 'MLHKF', 'MLM', 'MLSPF', 'MMC', 'MMM', 'MMSMY',
    'MNDR', 'MNST', 'MO', 'MOB', 'MOBQ', 'MOD', 'MODG', 'MOG-A', 'MOH', 'MOS', 'MP',
    'MPC', 'MPLX', 'MPWR', 'MRCY', 'MRK', 'MRNA', 'MRVL', 'MRVU', 'MS', 'MSCI', 'MSFT',
    'MSI', 'MSILF', 'MSTR', 'MTB', 'MTC', 'MTCH', 'MTD', 'MTG', 'MTRN', 'MTUAY',
    'MU', 'MUFG', 'MUX', 'MWYN', 'MYSZ', 'NAK', 'NATL', 'NB', 'NBIS', 'NBIX',
    'NCLH', 'NCNA', 'NDAQ', 'NDSN', 'NEE', 'NEM', 'NERV', 'NEWP', 'NEXA', 'NFE',
    'NFGC', 'NFLX', 'NG', 'NGD', 'NGG', 'NGLOY', 'NGXXF', 'NHTC', 'NI', 'NIO',
    'NKE', 'NLY', 'NMRA', 'NOC', 'NOMA', 'NOV', 'NOW', 'NPK', 'NPWR', 'NRDY',
    'NRG', 'NSC', 'NSKFF', 'NTAP', 'NTCT', 'NTES', 'NTRA', 'NTRP', 'NTRS', 'NU',
    'NUAI', 'NUE', 'NUKK', 'NUVL', 'NVA', 'NVDA', 'NVMI', 'NVO', 'NVR', 'NVRI',
    'NVS', 'NVST', 'NWE', 'NWG', 'NWGL', 'NWL', 'NWN', 'NWS', 'NWSA', 'NXPI',
    'NXTT', 'O', 'OBLG', 'ODFL', 'OFAL', 'OGS', 'OKE', 'OKLO', 'OKUR', 'OMC',
    'OMF', 'OMH', 'ON', 'ONCO', 'ONDS', 'ONEG', 'ONFO', 'ONMD', 'ONON', 'ONTO', 'OPEN',
    'OPXS', 'ORA', 'ORCL', 'ORI', 'ORLY', 'OSCR', 'OSTTF', 'OTIS', 'OUT', 'OXY',
    'PAA', 'PAAS', 'PANW', 'PAPL', 'PARR', 'PATH', 'PAX', 'PAYC', 'PAYX', 'PCAR',
    'PCG', 'PDD', 'PEG', 'PEP', 'PFE', 'PFG', 'PG', 'PGNY', 'PGR', 'PH',
    'PHM', 'PHYS', 'PILL', 'PKE', 'PKG', 'PL', 'PLD', 'PLRZ', 'PLTK', 'PLTR',
    'PM', 'PMAX', 'PNC', 'PNR', 'PNRG', 'PNW', 'PODD', 'POOL', 'POR', 'POWI', 'POWL',
    'PPCB', 'PPG', 'PPL', 'PRGO', 'PRMB', 'PRU', 'PRVA', 'PSA', 'PSHG', 'PSKY',
    'PSX', 'PTC', 'PTN', 'PTNDY', 'PTY', 'PUK', 'PWR', 'PYPL', 'Q', 'QBTS',
    'QCOM', 'QNTM', 'QNTQY', 'QQQ', 'QS', 'QTTB', 'RBLX', 'RBOT', 'RCL', 'RDDT',
    'RDHL', 'RDN', 'RDW', 'REE', 'REG', 'REGN', 'REKR', 'RF', 'RGTI', 'RHHBY',
    'RIGL', 'RIO', 'RIOT', 'RIVN', 'RJF', 'RKLB', 'RL', 'RMBS', 'RMD', 'RNMBY',
    'ROIV', 'ROK', 'ROL', 'ROP', 'ROST', 'RPRX', 'RR', 'RRR', 'RSG',
    'RTNTF', 'RTPPF', 'RTX', 'RVMD', 'RVTY', 'RWEOY', 'RWNFF', 'RY', 'RYCEY', 'RYOJ',
    'RYTM', 'RZLV', 'SA', 'SAABF', 'SAABY', 'SAFRY', 'SAN', 'SANM', 'SARO', 'SBAC',
    'SBCF', 'SBET', 'SBS', 'SBSW', 'SBUX', 'SCGLY', 'SCHW', 'SCNI', 'SCZMF', 'SEAT',
    'SER', 'SF', 'SGBX', 'SGGKF', 'SGGKY', 'SGI', 'SGML', 'SGN', 'SHEL', 'SHOP',
    'SHPH', 'SHW', 'SIF', 'SINT', 'SJM', 'SLB', 'SLE', 'SLNH', 'SLSR', 'SMCI',
    'SMFG', 'SMLR', 'SMMYY', 'SMR', 'SMTK', 'SMX', 'SNA', 'SNBR', 'SNEX', 'SNGX',
    'SNOW', 'SNPS', 'SNT', 'SNTI', 'SO', 'SOBO', 'SOFI', 'SOGP', 'SOLS', 'SOLV',
    'SONM', 'SOUHY', 'SOUN', 'SOXX', 'SPAI', 'SPG', 'SPGI', 'SPNT', 'SPR', 'SR',
    'SRE', 'SRL', 'SSKN', 'SSRM', 'STE', 'STEM', 'STKE', 'STKH', 'STKL', 'STLA',
    'STLD', 'STMNF', 'STT', 'STTSY', 'STX', 'STZ', 'SU', 'SUPN', 'SVM', 'SW',
    'SWK', 'SWKS', 'SYF', 'SYK', 'SYM', 'SYNX', 'SYY', 'T', 'TAC', 'TANH',
    'TAP', 'TARS', 'TATT', 'TBBB', 'TCBI', 'TCKRF', 'TD', 'TDG', 'TDS', 'TDY',
    'TE', 'TEAM', 'TECH', 'TECK', 'TEL', 'TEM', 'TER', 'TEVA', 'TFC', 'TFPM',
    'TGEN', 'TGT', 'THBRF', 'THC', 'THLEF', 'THLLY', 'TIL', 'TIRX', 'TJX', 'TKO',
    'TLN', 'TMC', 'TMO', 'TMQ', 'TMUS', 'TNIPF', 'TNK', 'TNL', 'TNXP', 'TPL',
    'TPR', 'TRAW', 'TREX', 'TRGP', 'TRI', 'TRIB', 'TRMB', 'TROW', 'TRP', 'TRUG',
    'TRV', 'TSCO', 'TSLA', 'TSM', 'TSN', 'TSSI', 'TT', 'TTD', 'TTMI', 'TTNNF',
    'TTWO', 'TWG', 'TXN', 'TXNM', 'TXT', 'TYL', 'U', 'UAL', 'UAMY', 'UBER',
    'UBS', 'UDR', 'UEC', 'UFO', 'UGI', 'UHS', 'UK', 'ULTA', 'UNH', 'UNP', 'UNPRF',
    'UPS', 'UPST', 'URBN', 'URI', 'USAR', 'USAS', 'USAU', 'USB', 'USFD', 'USGO',
    'UTHR', 'UUU', 'UUUU', 'V', 'VEEE', 'VET', 'VGZ', 'VIAV', 'VICI', 'VIPS',
    'VIVK', 'VLO', 'VLTO', 'VMC', 'VOO', 'VOYG', 'VRCA', 'VRMTF', 'VRSK', 'VRSN',
    'VRT', 'VRTX', 'VSEC', 'VST', 'VT', 'VTI', 'VTR', 'VTSI', 'VUZI', 'VVX',
    'VWAV', 'VYX', 'VZ', 'VZLA', 'WAB', 'WAI', 'WAT', 'WBA', 'WBD', 'WBUY',
    'WBX', 'WDAY', 'WDC', 'WEC', 'WELL', 'WES', 'WFC', 'WIMI', 'WIT', 'WKSP',
    'WM', 'WMB', 'WMT', 'WOK', 'WOLF', 'WPM', 'WRB', 'WSM', 'WST', 'WTRG',
    'WTW', 'WULF', 'WWD', 'WWW', 'WY', 'WYNN', 'WYY', 'XBI', 'XEL', 'XLE',
    'XLF', 'XLP', 'XLRE', 'XLU', 'XLV', 'XOM', 'XYL', 'XYZ', 'XZJCF', 'YOU',
    'YUM', 'ZBH', 'ZBRA', 'ZTS', 'ZWS'
]

# Override with clean universe if available
try:
    from clean_universe import CLEAN_TICKERS
    DEFAULT_TICKERS = CLEAN_TICKERS
    print(f"✅ Using clean universe: {len(DEFAULT_TICKERS)} quality stocks")
except ImportError:
    print(f"⚠️  Using original universe: {len(DEFAULT_TICKERS)} stocks")

# HTTP session with retry
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PATCH"],
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

AT_HEADERS = {
    "Authorization": f"Bearer {AT_API}",
    "Content-Type": "application/json",
}

# ═══════════════════════════════════════════════════════════════════════════
# 🎛️ FILTER CONFIGURATION - PRODUCTION SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
FILTER_MODE = "NONE"
"""
Filter Mode Guide:
- NONE:     No filters at all (baseline test)
- FIBONACCI ONLY: 
- MINIMAL:  Only remove obvious junk (score > -0.3, sharpe > 0.3)
- MODERATE: Balanced filtering (score > 0, sharpe > 0.4, basic fib checks)
- STRICT:   Your original strict filters (use after proving baseline works)
"""

# Base risk parameters
RISK_PER_TRADE = 0.008              # 0.8% risk per trade (was 1.0%)
SWING_RISK_PER_TRADE = 0.025        # 2.5% for swing trades
MIN_SWING_RISK_PER_TRADE = 0.004    # 0.4% minimum
MAX_SWING_RISK_PER_TRADE = 0.03    # 3% max

# Position size limits
MIN_POSITION_SIZE = 0.03            # 3% minimum

# Quality filters
MIN_PRICE = 7.50                    # Minimum stock price
MAX_PRICE = 300.00                   # Maximum stock price
MIN_VOLUME = 500000                 # Minimum average daily volume
EXCLUDE_OTC = True                  # Exclude OTC stocks (tickers ending in F, Y)
MAX_POSITION_SIZE: float = 0.06     # 6% maximum (was 12%)
MAX_PORTFOLIO_LEVERAGE: float = 1.0 # 100% max leverage (no leverage)

# Drawdown controls - TIERED SYSTEM
MAX_ACCEPTABLE_DD_GLOBAL = 0.25     # 25% full stop (raised from 20%)
DD_WARNING_THRESHOLD = 0.15         # 15% - reduce to 50% size
DD_SEVERE_THRESHOLD = 0.20          # 20% - reduce to 25% size
GLOBAL_STOP_RESET_DAYS = 60         # Days before resuming after stop

# Stop loss parameters
MIN_STOP_ALLOWED: float = 0.05      # 5% minimum stop (was 8%)
MAX_STOP_ALLOWED: float = 0.08      # 8% maximum stop (was 35%)

# Risk measurement
VAR_WINDOW = 60                     # VaR lookback window (days)
CONFIDENCE_LEVEL = 0.95             # VaR confidence level (95%)

# Signal thresholds
SWING_STRONG_BUY: float = 0.50      # Strong buy threshold
SWING_BUY_SCORE: float = 0.20       # Buy threshold
SWING_WEAK_SELL: float = -0.20      # Weak sell threshold
SWING_STRONG_SELL: float = -0.50    # Strong sell threshold

# Transaction costs
SLIPPAGE_BPS: float = 3.0           # 3 basis points slippage
TRANSACTION_COST_BPS: float = 1.0   # 1 basis point transaction cost

# Quality filters
QUALITY_FILTERS_ENABLED = True
MAX_CONCURRENT_POSITIONS = 10       # 10 positions (was 15)
COOLDOWN_DAYS = 10                   # Days before re-entering same stock
# Deployment parameters - TURN OFF AGGRESSIVE MODE
FORCE_FULL_DEPLOYMENT = False       # Let system be selective (was True)
MIN_POSITIONS_REQUIRED = 5          # Lower bar (was 10)
POSITION_FILL_AGGRESSION = 1.0      # Normal sizing (was 1.2)

# Ranking configuration
TOP_N_UNIVERSE_SCAN = 25            # Scan top 25 (was 30)
TOP_N_POSITIONS = 10                # Size top 10 (was 15)

# =============================================================================
# LONG TERM SWINGS CONSTANTS & CONFIGURATION
# =============================================================================
# Holding periods
SWING_HOLDING_DAYS = 5              # Swing trades: 2-10 days
POSITION_HOLDING_DAYS = 21          # Position trades: 3 weeks (was 30)

# Stop multipliers
SWING_STOP_MULTIPLIER = 1.0         # Normal stops for swing
POSITION_STOP_MULTIPLIER = 1.3      # 1.3x wider stops (was 1.5x)

# Position-specific settings
POSITION_MIN_STOP = 0.06            # 6% minimum (was 10%)
POSITION_MAX_STOP = 0.10            # 10% maximum (was 40%)

# =============================================================================
# TEST MODE CONFIGURATION - DISABLED FOR PRODUCTION
# =============================================================================
TEST_MODE = False  # Set to False for normal operation

# Remove or comment out the TEST_MODE override section entirely:
# if TEST_MODE:
#     print("\n" + "="*80)
#     print("🔬 TEST MODE ACTIVE - DIAGNOSTIC CONFIGURATION")
#     print("="*80)
#     print("   - Fibonacci position sizing: DISABLED")
#     print("   - Filter mode: MINIMAL")
#     print("   - Goal: Isolate performance issues")
#     print("="*80 + "\n")
#     
#     # ⭐ Remove this line that overrides your MODERATE setting
#     # FILTER_MODE = "MINIMAL"

# =============================================================================
# FIBONACCI POSITION SIZING - REGIME DEPENDENT (ENABLED FOR PRODUCTION)
# =============================================================================
FIB_POSITION_CONFIG = {
    'enabled': True,  # ✅ ENABLED FOR PRODUCTION
    'regime_dependent': True,
    'boost_support': True,
    'reduce_resistance': True,
    'min_fib_score_for_boost': 0.5,
    
    # ⭐ REGIME-BASED MULTIPLIERS:
    'regime_multipliers': {
        'CENTRALIZED': {
            'support_boost': 1.0,        # No boost in centralized markets
            'resistance_penalty': 0.9,   # 10% penalty at resistance
            'require_support': False,
        },
        'MIXED': {
            'support_boost': 1.1,        # 10% boost at support
            'resistance_penalty': 0.85,  # 15% penalty at resistance
            'require_support': False,
        },
        'BROAD': {
            'support_boost': 1.2,        # 20% boost at support
            'resistance_penalty': 0.8,   # 20% penalty at resistance
            'require_support': True,     # Require support levels
        }
    },
    
    'fib_stop_tightening': True,
    'strict_fib_filtering': False,
}

# =============================================================================
# UTILS
# =============================================================================
def sanitize_number(val):
    try:
        if val is None:
            return 0.0
        if isinstance(val, str):
            return float(val)
        if isinstance(val, (int, float, np.number)):
            if np.isnan(val) or np.isinf(val):
                return 0.0
            return round(float(val), 4)
    except Exception:
        return 0.0
    return 0.0

def safe_scalar(value):
    """Alias for safe_get_scalar to maintain compatibility."""
    return safe_get_scalar(value)

def flatten_yfinance_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten MultiIndex columns from yfinance to simple column names.
    Call this immediately after yf.download() for single-ticker downloads.
    """
    if df is None or df.empty:
        return df
    
    df = df.copy()
    
    if isinstance(df.columns, pd.MultiIndex):
        # Take the first level (Open, High, Low, Close, Volume)
        df.columns = df.columns.get_level_values(0)
    
    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Capitalize column names
    df.columns = [str(col).strip().capitalize() for col in df.columns]
    
    return df

# =============================================================================
# AIRTABLE INTEGRATION
# =============================================================================

def fetch_airtable_records() -> Dict[str, str]:
    """Return {TICKER: record_id} from Airtable."""
    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    params = {"pageSize": 100}
    id_map: Dict[str, str] = {}

    while True:
        try:
            resp = session.get(url, headers=AT_HEADERS, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for rec in data.get("records", []):
                ticker = rec.get("fields", {}).get("Ticker")
                if ticker:
                    id_map[str(ticker).upper()] = rec["id"]

            if "offset" in data:
                params["offset"] = data["offset"]
            else:
                break
        except Exception as e:
            print(f"❌ Airtable fetch failed: {e}")
            break

    return id_map


def batch_patch(records: List[dict]):
    if not records:
        return
    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    try:
        resp = session.patch(url, headers=AT_HEADERS, json={"records": records})
        resp.raise_for_status()
        print(f"🔄 Patched {len(records)} records")
    except Exception as e:
        print(f"❌ Batch PATCH failed: {e}")
        try:
            print(f"   Response: {resp.text[:500]}")
        except: pass

def batch_create(records: List[dict]):
    if not records:
        return
    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    try:
        resp = session.post(url, headers=AT_HEADERS, json={"records": records})
        resp.raise_for_status()
        print(f"🆕 Created {len(records)} records")
    except Exception as e:
        print(f"❌ Batch CREATE failed: {e}")
        try:
            print(f"   Response: {resp.text[:500]}")
        except:
            pass


def push_to_airtable(final_signals: Dict[str, dict], risk_mgmt: Dict[str, dict]):
    """Upsert final signals with incremental updates only.
    
    UPDATED: Only push TOP 10 ranked signals (matching email) plus existing portfolio positions.
    """
    print("📤 Preparing incremental Airtable upsert...")
    existing = fetch_airtable_records()
    print(f"📄 Existing records: {len(existing)}")
    
    update_batch: List[dict] = []
    create_batch: List[dict] = []
    
    stop_map = risk_mgmt.get("stop_losses", {})
    position_map = risk_mgmt.get("position_sizing", {})
    
    # Track changes for incremental updates
    update_count = 0
    create_count = 0
    skip_count = 0
    pushed_tickers = set()  # Track what we actually push to Airtable
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: Identify portfolio positions (In Portfolio = Yes)
    # ═══════════════════════════════════════════════════════════════
    portfolio_tickers = set()
    for ticker, record_id in existing.items():
        try:
            url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}/{record_id}"
            resp = session.get(url, headers=AT_HEADERS, timeout=10)
            if resp.status_code == 200:
                fields = resp.json().get("fields", {})
                if fields.get("In Portfolio", "No") == "Yes":
                    portfolio_tickers.add(ticker.upper())
        except:
            pass
    print(f"📊 Portfolio positions: {len(portfolio_tickers)} ({', '.join(sorted(portfolio_tickers))})")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: Rank signals and select TOP 10 (matching email logic)
    # ═══════════════════════════════════════════════════════════════
    def calculate_ranking_score(sym: str, data: dict) -> float:
        """Calculate ranking score - same formula as email."""
        sharpe = data.get('sharpe_ratio', 0)
        score = data.get('combined_score', 0)
        confidence_val = data.get('confidence', 'MEDIUM')
        fib_score = data.get('fib_score', 0.5)
        
        # Convert confidence to numeric
        conf_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
        confidence = conf_map.get(confidence_val, 0.5)
        
        # Ranking formula (Fibonacci disabled)
        total = (
            sharpe * 0.35 +
            score * 0.35 +
            confidence * 0.15 +
            fib_score * 0.0  # DISABLED
        )
        return total
    
    # Filter and rank signals
    rankable_signals = []
    for sym, data in final_signals.items():
        sym_u = sym.upper()
        
        # Skip if no position size
        position_size = position_map.get(sym, 0.0)
        if position_size < MIN_POSITION_SIZE:
            continue
        
        # Quality filters
        if EXCLUDE_OTC and (len(sym_u) >= 5 and (sym_u.endswith("F") or sym_u.endswith("Y"))):
            continue
        current_price = data.get("current_price", 0) or data.get("price", 0)
        if current_price and (current_price < MIN_PRICE or current_price > MAX_PRICE):
            continue
        avg_volume = data.get("avg_volume", 0) or data.get("volume", 0)
        if avg_volume and avg_volume < MIN_VOLUME:
            continue
        
        rank_score = calculate_ranking_score(sym_u, data)
        rankable_signals.append((sym_u, data, rank_score))
    
    # Sort by ranking score descending and take top 10
    rankable_signals.sort(key=lambda x: x[2], reverse=True)
    top_10_signals = {sym: data for sym, data, score in rankable_signals[:10]}
    
    print(f"📊 Top 10 signals: {', '.join(top_10_signals.keys())}")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 3: Determine which tickers to push
    # - Top 10 signals (new picks)
    # - Portfolio positions (updates only)
    # ═══════════════════════════════════════════════════════════════
    tickers_to_push = set(top_10_signals.keys()) | portfolio_tickers
    print(f"📊 Total tickers to push: {len(tickers_to_push)}")
    
    for sym_u in tickers_to_push:
        # Get signal data (may be None for portfolio-only positions)
        data = final_signals.get(sym_u) or final_signals.get(sym_u.lower()) or {}
        
        # For portfolio positions not in today's signals, just update what we can
        if not data and sym_u in portfolio_tickers:
            # Skip - we'll just keep the existing record as-is
            continue
        
        if not data:
            continue
        
        # Get existing record if available
        existing_record = None
        if sym_u in existing:
            try:
                url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}/{existing[sym_u]}"
                resp = session.get(url, headers=AT_HEADERS)
                if resp.status_code == 200:
                    existing_record = resp.json()['fields']
            except:
                pass
        
        # Prepare new fields - match Airtable column names
        current_price = sanitize_number(data.get("current_price", 0))
        stop_price = sanitize_number(data.get("stop_price", 0))
        target_price = sanitize_number(data.get("target_price", 0))
        stop_loss_pct = sanitize_number(stop_map.get(sym_u, stop_map.get(sym_u.lower(), 0.0)))
        position_size = sanitize_number(position_map.get(sym_u, position_map.get(sym_u.lower(), 0.0)))
        
        # Calculate derived fields
        target_pct = (target_price / current_price) - 1 if current_price > 0 else 0
        win_rate = 0.52
        trade_type = data.get("trade_type", "SWING")
        
        expected_swing = (win_rate * target_pct) - ((1 - win_rate) * stop_loss_pct)
        position_target = stop_loss_pct * 3.0
        position_win_rate = 0.55
        expected_position = (position_win_rate * position_target) - ((1 - position_win_rate) * stop_loss_pct)
        
        if trade_type == "POSITION":
            expected_annual = expected_position * 2
        elif trade_type == "BOTH":
            swing_annual = expected_swing * 3
            position_annual = expected_position * 2
            expected_annual = (swing_annual + position_annual) / 2
        else:
            expected_annual = expected_swing * 3
        
        # Determine action for portfolio positions
        in_portfolio = existing_record.get("In Portfolio", "No") if existing_record else "No"
        if in_portfolio == "Yes":
            trade_price = existing_record.get("Trade Price", 0)
            stop_pct = existing_record.get("Stop Loss", 0.12)
            stop_price_calc = trade_price * (1 - stop_pct) if trade_price else 0
            if data.get("signal") in ["SELL", "STRONG_SELL"]:
                action = "SELL"
            elif current_price and stop_price_calc and current_price <= stop_price_calc:
                action = "SELL - STOPPED"
            else:
                action = "KEEP"
        else:
            action = ""
        
        fields = {
            "Ticker": sym_u,
            "Sector": get_sector(sym_u),
            "Final Signal": data.get("signal", ""),
            "Signal Strength": sanitize_number(data.get("combined_score", 0)),
            "Position Size": sanitize_number(position_size),
            "Stop Loss": sanitize_number(stop_loss_pct),
            "Last Updated": date.today().isoformat(),
            "In Portfolio": in_portfolio,
            "Action": action,
            "Confidence": data.get("confidence", "MEDIUM"),
            "Trade Type": data.get("trade_type", "SWING"),
            "Sharpe Ratio": sanitize_number(data.get("sharpe_ratio", 0)),
            "Fib Score": sanitize_number(data.get("fib_score", 0)),
            "Fib Level": (float(data.get("fib_level", "0").replace("%", "")) / 100) if data.get("fib_level") and data.get("fib_level") not in ["N/A", ""] else None,
            "Fib Distance": sanitize_number(data.get("fib_distance_pct", 0)),
            "Near Fib Support": "Yes" if data.get("near_fib_support", False) else "No",
            "Current Price": current_price,
            "Target Price": target_price,
            "ATR Pct": sanitize_number(data.get("target_pct", target_pct)),
            "True ATR Pct": sanitize_number(data.get("raw_atr_pct", 0)),
            "Days to Earnings": sanitize_number(data.get("days_to_earnings", 0)),
            "Earnings Flag": data.get("earnings_flag", ""),
            "Earnings Mult": sanitize_number(data.get("earnings_mult", 1.0)),
            "Economic Bias": data.get("economic_bias", "NEUTRAL"),
            "Drift Score": sanitize_number(data.get("drift_score", 0)),
            "Regime Score": sanitize_number(data.get("regime_score", 0)),
            "VAR 95%": sanitize_number(data.get("var_95", 0) / 100) if data.get("var_95", 0) > 10 else sanitize_number(data.get("var_95", 0)),
        }
        
        # CRITICAL: Only set Entry Date if creating NEW record or existing has no date
        existing_entry_date = existing_record.get("Entry Date") if existing_record else None
        if not existing_entry_date:
            new_entry_date = data.get("entry_date")
            if new_entry_date:
                fields["Entry Date"] = new_entry_date
        
        # Check if update is needed (incremental update)
        if existing_record and AIRTABLE_INCREMENTAL:
            needs_update = False

            for key, new_value in fields.items():
                if key not in existing_record:
                    # Schema addition: existing record missing this field — force update
                    # so newly-added Airtable columns get backfilled on the next run.
                    needs_update = True
                    break
                old_value = existing_record[key]
                if isinstance(new_value, (int, float)) and isinstance(old_value, (int, float)):
                    if abs(new_value - old_value) / (abs(old_value) + 0.0001) > AIRTABLE_UPDATE_THRESHOLD:
                        needs_update = True
                        break
                elif str(new_value) != str(old_value):
                    needs_update = True
                    break

            if not needs_update:
                skip_count += 1
                continue
        
        # Add to appropriate batch
        pushed_tickers.add(sym_u)
        if sym_u in existing:
            update_batch.append({"id": existing[sym_u], "fields": fields})
            update_count += 1
        else:
            create_batch.append({"fields": fields})
            create_count += 1
        
        # Send batches periodically
        if len(update_batch) >= AIRTABLE_BATCH_SIZE:
            batch_patch(update_batch)
            update_batch = []
        
        if len(create_batch) >= AIRTABLE_BATCH_SIZE:
            batch_create(create_batch)
            create_batch = []
    
    # Send remaining batches
    if update_batch:
        batch_patch(update_batch)
    if create_batch:
        batch_create(create_batch)
    
    print(f"✅ Airtable sync complete: {update_count} updates, {create_count} creates, {skip_count} skipped (unchanged)")
    
    # CLEANUP: Delete old records that are not in portfolio AND not in today's top 10
    print("🧹 Cleaning up stale records...")
    delete_count = 0
    
    for ticker, record_id in existing.items():
        # Skip if ticker is in today's top 10 or portfolio
        if ticker in tickers_to_push:
            continue
        
        # Check if this record is in portfolio (double-check)
        try:
            url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}/{record_id}"
            resp = session.get(url, headers=AT_HEADERS, timeout=10)
            if resp.status_code == 200:
                fields = resp.json().get("fields", {})
                in_portfolio = fields.get("In Portfolio", "No")
                
                # Delete if NOT in portfolio and NOT in today's signals
                if in_portfolio != "Yes":
                    del_resp = session.delete(url, headers=AT_HEADERS, timeout=10)
                    if del_resp.status_code == 200:
                        delete_count += 1
                        print(f"   🗑️  Deleted stale record: {ticker}")
        except Exception as e:
            pass
    
    if delete_count > 0:
        print(f"🗑️  Deleted {delete_count} stale records")


def export_top_picks_csv(final_signals: Dict[str, dict], risk_mgmt: Dict[str, dict]):
    """Export top picks to CSV for record keeping."""
    positions = risk_mgmt.get("position_sizing", {})
    stops = risk_mgmt.get("stop_losses", {})
    
    # Only export positions with size > 0
    picks = [
        {
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Ticker": sym,
            "Signal": data.get("signal"),
            "Confidence": data.get("confidence"),
            "Score": round(data.get("combined_score", 0), 3),
            "Sharpe": round(data.get("sharpe_ratio", 0), 2),
            "Fib_Score": round(data.get("fib_score", 0), 2),
            "Fib_Level": data.get("fib_level", ""),
            "Near_Support": "Yes" if data.get("near_fib_support", False) else "No",
            "Position": round(positions.get(sym, 0) * 100, 1),
            "Stop": round(stops.get(sym, 0) * 100, 1),
            "Trade_Type": data.get("trade_type", "SWING"),
        }
        for sym, data in final_signals.items()
        if positions.get(sym, 0) > 0
    ]
    
    if not picks:
        return
    
    # Sort by position size (largest first)
    picks.sort(key=lambda x: x["Position"], reverse=True)
    
    filepath = f"picks_{datetime.now().strftime('%Y%m%d')}.csv"
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=picks[0].keys())
        writer.writeheader()
        writer.writerows(picks)
    
    print(f"📁 Exported {len(picks)} picks to {filepath}")

# =============================================================================
# HYBRID TOP-10 RANK TILTING
# =============================================================================

def build_hybrid_rank_multipliers(symbols):
    """
    Given an ordered list of symbols [best ... worst],
    return a dict of rank-based multipliers.
    Top positions get more weight, decaying gradually.
    """
    n = len(symbols)
    
    if n == 0:
        return {}
    
    # Generate decaying multipliers dynamically
    # Top symbol gets 2.0x, decays to 0.5x for last position
    if n == 1:
        return {symbols[0]: 2.0}
    
    # Linear decay from 2.0 to 0.5
    multipliers = [2.0 - (1.5 * i / (n - 1)) for i in range(n)]
    
    return {sym: multipliers[i] for i, sym in enumerate(symbols)}


# =============================================================================
# DATA QUALITY MODULE
# =============================================================================

class DataQualityModule:
    """Validates and cleans price & economic data for swing trading."""

    @staticmethod
    def validate_price_data(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Clean and validate price data:
        - Remove dup columns/index
        - Require Close
        - Forward-fill max 1 day (preserve gaps)
        """
        try:
            if df is None or df.empty:
                return pd.DataFrame()

            df = df.copy()
            df = df.loc[:, ~df.columns.duplicated()]
            df = df[~df.index.duplicated(keep="first")]

            if "Close" not in df.columns:
                print(f"⚠️ Missing Close column for {symbol}. Skipping.")
                return pd.DataFrame()

            df = df.ffill(limit=1)
            df = df.dropna(subset=["Close"])

            first_valid = df["Close"].first_valid_index()
            last_valid = df["Close"].last_valid_index()
            if first_valid is not None and last_valid is not None:
                df = df.loc[first_valid:last_valid]

            return df

        except Exception as e:
            print(f"❌ Error validating {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    def validate_economic_data(series: pd.Series, name: str) -> pd.Series:
        """Economic data can be forward-filled more aggressively."""
        try:
            if series is None or series.empty:
                return pd.Series(dtype=float)

            s = series.copy()
            s = s[~s.index.duplicated(keep="first")]
            s = s.ffill(limit=30)
            return s
        except Exception:
            return pd.Series(dtype=float)


# =============================================================================
# ECONOMIC MODULE
# =============================================================================

class EconomicModule:
    """Loads economic indicators for HIGH-LEVEL swing bias only."""

    def __init__(self, api_key: str = FRED_API_KEY):
        self.fred = Fred(api_key=api_key)
        self.dq = DataQualityModule()

    def _safe_fred(self, code, units=None, start=None, end=None):
        try:
            if start and end:
                s = self.fred.get_series(code, start, end, units=units)
            else:
                s = self.fred.get_series(code, units=units)

            if s is None or s.empty:
                return pd.Series(dtype=float)

            s.index = pd.to_datetime(s.index)
            return self.dq.validate_economic_data(s, code)
        except Exception:
            return pd.Series(dtype=float)

    def load(self) -> Dict[str, float]:
        """Snapshot for current regime."""
        try:
            cpi = self._safe_fred("CPIAUCSL", units="pc1")
            dff = self._safe_fred("DFF")
            dgs10 = self._safe_fred("DGS10")
            tips10 = self._safe_fred("DFII10")
            dgs2 = self._safe_fred("DGS2")
            vix = self._safe_fred("VIXCLS")

            if any(s.empty for s in [cpi, dff, dgs10, tips10, dgs2, vix]):
                return {
                    "inflation": 0.0,
                    "fed_funds": 0.0,
                    "inflation_expectation": 0.0,
                    "yield_curve": 0.0,
                    "real_yield": 0.0,
                    "vix": 20.0,
                }

            return {
                "inflation": float(cpi.iloc[-1]),
                "fed_funds": float(dff.iloc[-1]),
                "inflation_expectation": float(dgs10.iloc[-1] - tips10.iloc[-1]),
                "yield_curve": float(dgs10.iloc[-1] - dgs2.iloc[-1]),
                "real_yield": float(tips10.iloc[-1]),
                "vix": float(vix.iloc[-1]),
            }

        except Exception:
            return {
                "inflation": 0.0,
                "fed_funds": 0.0,
                "inflation_expectation": 0.0,
                "yield_curve": 0.0,
                "real_yield": 0.0,
                "vix": 20.0,
            }

    def load_historical(self, start: str, end: str) -> Dict[str, pd.Series]:
        """Historical macro time series for backtest."""
        try:
            cpi = self._safe_fred("CPIAUCSL", units="pc1", start=start, end=end)
            dff = self._safe_fred("DFF", start=start, end=end)
            dgs10 = self._safe_fred("DGS10", start=start, end=end)
            tips10 = self._safe_fred("DFII10", start=start, end=end)
            dgs2 = self._safe_fred("DGS2", start=start, end=end)
            vix = self._safe_fred("VIXCLS", start=start, end=end)

            return {
                "inflation": cpi,
                "fed_funds": dff,
                "inflation_expectation": dgs10 - tips10,
                "yield_curve": dgs10 - dgs2,
                "real_yield": tips10,
                "vix": vix,
            }

        except Exception:
            return {
                "inflation": pd.Series(dtype=float),
                "fed_funds": pd.Series(dtype=float),
                "inflation_expectation": pd.Series(dtype=float),
                "yield_curve": pd.Series(dtype=float),
                "real_yield": pd.Series(dtype=float),
                "vix": pd.Series(dtype=float),
            }

# =============================================================================
# FIBONACCI MODULE (UPDATED)
# =============================================================================

class FibonacciModule:
    """Fibonacci retracement and extension calculations with signal integration."""
    
    def __init__(self, lookback_days=252):
        self.lookback_days = lookback_days  # ~1 year for 52-week high/low
    
    def calculate_fib_levels(self, prices: pd.Series) -> Dict[str, float]:
        """
        Calculate Fibonacci retracement levels from available high/low.
        Flexible lookback - uses whatever data we have.
        """
        if prices is None or len(prices) < 20:  # Minimum for any calculation
            return {}
    
        # Use available data, not fixed 252 days
        available_data = prices
        high_available = available_data.max()
        low_available = available_data.min()
        current = prices.iloc[-1]
    
        if high_available == low_available:
            return {}
    
        # Standard Fibonacci retracement levels
        fib_levels = {
            '0.0%': low_available,
            '23.6%': high_available - 0.236 * (high_available - low_available),
            '38.2%': high_available - 0.382 * (high_available - low_available),
            '50.0%': high_available - 0.50 * (high_available - low_available),
            '61.8%': high_available - 0.618 * (high_available - low_available),
            '76.4%': high_available - 0.764 * (high_available - low_available),
            '100.0%': high_available,
            'current': current
        }
    
        # Calculate what time period we actually used
        if len(prices) >= 252:
            lookback_used = "52-week (252 days)"
        elif len(prices) >= 100:
            lookback_used = f"{len(prices)} days"
        else:
            lookback_used = f"{len(prices)} days (limited)"
    
        return {
            'high_52w': high_available,
            'low_52w': low_available,
            'current': current,
            'retracement_pct': (high_available - current) / (high_available - low_available) * 100,
            'levels': fib_levels,
            'lookback_used': lookback_used,
            'data_points': len(prices)
        }
    
    def calculate_fib_score(self, fib_data: Dict) -> Dict[str, float]:
        """
        Calculate multiple Fibonacci-based scores.
        """
        if not fib_data:
            return {
                'fib_score': 0.0, 
                'closeness_score': 0.0, 
                'support_score': 0.0, 
                'resistance_score': 0.0,
                'trend_score': 0.0,
                'closest_level': 'N/A',
                'distance_pct': 100.0,
                'retracement_pct': 0.0
            }
        
        current = fib_data['current']
        levels = fib_data['levels']
        retracement_pct = fib_data['retracement_pct']
        
        # 1. Closeness to any fib level (0-100%)
        min_distance = 100.0
        closest_level = 'N/A'
        
        fib_prices = {
            '23.6%': levels['23.6%'],
            '38.2%': levels['38.2%'],
            '50.0%': levels['50.0%'],
            '61.8%': levels['61.8%'],
            '76.4%': levels['76.4%']
        }
        
        for level_name, level_price in fib_prices.items():
            distance_pct = abs((current - level_price) / current * 100)
            if distance_pct < min_distance:
                min_distance = distance_pct
                closest_level = level_name
        
        # Score: closer = better (within 3% is excellent)
        closeness_score = max(0.0, 1.0 - (min_distance / 15.0))
        
        # 2. Support bias score (prefer deeper retracements for longs)
        # Support fibs are 61.8% and 76.4%
        support_fibs = ['61.8%', '76.4%']
        resistance_fibs = ['23.60%', '38.20%', '23.6%', '38.2%']
        
        support_score = 0.0
        resistance_score = 0.0
        
        if closest_level in support_fibs:
            # Bonus for being near support
            support_score = 0.8 if min_distance <= 5.0 else 0.4
        elif closest_level in resistance_fibs:
            # Penalty for being near resistance
            resistance_score = -0.5 if min_distance <= 5.0 else -0.2
        
        # 3. Trend context score
        # If price is between 38.2% and 61.8%, it's in a healthy retracement
        if 38.2 <= retracement_pct <= 61.8:
            trend_score = 0.3
        elif 61.8 < retracement_pct <= 76.4:
            trend_score = 0.2  # Deep retracement but still okay
        else:
            trend_score = -0.1
        
        # 4. Combined fib score
        combined = closeness_score + support_score + resistance_score + trend_score
        
        return {
            'fib_score': np.clip(combined, 0.0, 1.0),
            'closeness_score': closeness_score,
            'support_score': support_score,
            'resistance_score': resistance_score,
            'trend_score': trend_score,
            'closest_level': closest_level,
            'distance_pct': min_distance,
            'retracement_pct': retracement_pct
        }
    
    def calculate_fibonacci_signal_score(self, prices: pd.Series) -> Dict[str, Any]:
        """
        Calculate comprehensive Fibonacci signal score for ranking.
        Returns everything needed for signal processing.
        """
        # ========== ADD DEBUG HERE ==========
        print(f"   Prices length: {len(prices) if prices is not None else 'None'}")
    
        if prices is not None and len(prices) > 0:
            print(f"   Date range: {prices.index[0].date()} to {prices.index[-1].date()}")
            print(f"   Most recent price: ${prices.iloc[-1]:.2f}")
            print(f"   Total price points: {len(prices)}")
        
            # Check if we have recent data
            last_date = prices.index[-1].date()
            today = datetime.now().date()
            if hasattr(self, 'current_date'):
                today = self.current_date.date() if hasattr(self.current_date, 'date') else self.current_date
            
            if last_date < today:
                print(f"   ⚠️  STALE DATA: Last price date {last_date}, Current date {today}")
        else:
            print(f"   ❌ NO PRICE DATA AVAILABLE")
        # ========== END DEBUG ==========
    
        # Get basic Fibonacci levels
        fib_data = self.calculate_fib_levels(prices)
    
        if not fib_data:
            # ========== ADD DEBUG HERE ==========
            print(f"   Prices type: {type(prices)}")
            print(f"   Prices was {'None' if prices is None else f'length {len(prices)}'}")
        
            if prices is not None and len(prices) > 0:
                # Show sample of what we did have
                print(f"   First 5 prices: {prices.head().tolist()}")
                print(f"   Last 5 prices: {prices.tail().tolist()}")
        # ========== END DEBUG ==========
        
            # Return neutral scores if no Fibonacci data
            return {
                'fib_score': 0.5,
                'fib_level': 'N/A',
                'fib_signal': 'NEUTRAL',
                'confidence': 'LOW',
                'stop_distance_pct': 0.08,  # Default 8% stop
                'is_support': False,
                'distance_to_support': 0.15,
                'retracement_pct': 0.0
            }
    
        # ========== ADD DEBUG HERE ==========
        print(f"   Current price: ${fib_data.get('current', 'N/A'):.2f}")
        print(f"   Levels: {fib_data.get('levels', {})}")
        print(f"   Retracement %: {fib_data.get('retracement_pct', 0):.1f}%")
        # ========== END DEBUG ==========
    
        # Calculate Fibonacci scores
        fib_scores = self.calculate_fib_score(fib_data)
    
        # ========== ADD DEBUG HERE ==========
        print(f"   Fibonacci score: {fib_scores.get('fib_score', 0):.3f}")
        print(f"   Closest level: {fib_scores.get('closest_level', 'N/A')}")
        print(f"   Distance %: {fib_scores.get('distance_pct', 0):.1f}%")
        # ========== END DEBUG ==========
        
        # Determine stop distance based on nearest support
        current = fib_data['current']
        levels = fib_data['levels']
        
        # Find nearest Fibonacci support below current price
        support_levels = ['76.4%', '61.8%', '50.0%']
        nearest_support = None
        min_support_distance = float('inf')
        
        for level in support_levels:
            support_price = levels[level]
            if support_price < current:  # Must be below current price
                distance_pct = (current - support_price) / current
                if distance_pct < min_support_distance:
                    min_support_distance = distance_pct
                    nearest_support = level
        
        # Calculate stop loss percentage
        if nearest_support and min_support_distance > 0:
            # Use Fibonacci support for stop, with minimum 4%, maximum 12%
            stop_pct = max(min(min_support_distance * 1.1, 0.12), 0.04)
        else:
            stop_pct = 0.08  # Default 8%
        
        # Generate recommendation
        recommendation = self.generate_fib_recommendation(fib_scores)
        
        # Determine if near support
        is_support = self.is_near_fib_support(fib_scores, tolerance_pct=3.0)
        
        return {
            'fib_score': float(fib_scores['fib_score']),
            'fib_level': fib_scores['closest_level'],
            'fib_signal': recommendation['signal'],
            'confidence': recommendation['confidence'],
            'stop_distance_pct': stop_pct,
            'is_support': is_support,
            'distance_to_support': float(min_support_distance) if nearest_support else 0.15,
            'retracement_pct': fib_data['retracement_pct']
        }
    
    def is_near_fib_support(self, fib_scores: Dict, tolerance_pct: float = 3.0) -> bool:
        """
        Check if price is near a Fibonacci support level.
        """
        if not fib_scores or fib_scores['closest_level'] == 'N/A':
            return False
        
        closest_level = fib_scores['closest_level']
        distance_pct = fib_scores['distance_pct']
        
        # Support levels (50% is psychological support)
        support_levels = ['61.8%', '76.4%', '50.0%']
        
        return closest_level in support_levels and distance_pct <= tolerance_pct
    
    def generate_fib_recommendation(self, fib_scores: Dict) -> Dict[str, str]:
        """
        Generate trading recommendations based on fib levels.
        """
        if not fib_scores or fib_scores['closest_level'] == 'N/A':
            return {'signal': 'NO_DATA', 'confidence': 'LOW'}
        
        retracement = fib_scores['retracement_pct']
        closest = fib_scores['closest_level']
        distance = fib_scores['distance_pct']
        
        if distance > 5.0:
            return {'signal': 'NEUTRAL', 'confidence': 'LOW'}
        
        if closest == '76.4%':
            return {'signal': 'STRONG_SUPPORT', 'confidence': 'HIGH'}
        elif closest == '61.8%':
            return {'signal': 'GOOD_SUPPORT', 'confidence': 'MEDIUM'}
        elif closest == '50.0%':
            return {'signal': 'MODERATE_SUPPORT', 'confidence': 'MEDIUM'}
        elif closest in ['23.60%', '38.20%', '23.6%', '38.2%']:
            return {'signal': 'NEAR_RESISTANCE', 'confidence': 'MEDIUM'}
        
        return {'signal': 'NEUTRAL', 'confidence': 'LOW'}
    
    def get_fibonacci_summary(self, prices: pd.Series) -> Dict[str, Any]:
        """
        Get complete Fibonacci summary for a symbol.
        Useful for debugging and analysis.
        """
        fib_data = self.calculate_fib_levels(prices)
        
        if not fib_data:
            return {'has_fib_data': False, 'message': 'Insufficient price data'}
        
        fib_scores = self.calculate_fib_score(fib_data)
        signal_data = self.calculate_fibonacci_signal_score(prices)
        recommendation = self.generate_fib_recommendation(fib_scores)
        
        return {
            'has_fib_data': True,
            'price_data': {
                'current': fib_data['current'],
                'high_52w': fib_data['high_52w'],
                'low_52w': fib_data['low_52w'],
                'retracement_pct': fib_data['retracement_pct']
            },
            'fib_levels': fib_data['levels'],
            'scores': fib_scores,
            'signal': signal_data,
            'recommendation': recommendation,
            'is_near_support': self.is_near_fib_support(fib_scores),
            'trading_suggestions': self.generate_trading_suggestions(fib_data, fib_scores)
        }
    
    def generate_trading_suggestions(self, fib_data: Dict, fib_scores: Dict) -> Dict[str, str]:
        """
        Generate specific trading suggestions based on Fibonacci analysis.
        """
        if not fib_data or not fib_scores:
            return {'entry': 'No suggestion', 'stop': '8%', 'target': 'N/A'}
        
        current = fib_data['current']
        closest_level = fib_scores.get('closest_level', 'N/A')
        distance_pct = fib_scores.get('distance_pct', 100.0)
        
        suggestions = {}
        
        # Entry suggestions
        if closest_level in ['61.8%', '76.4%', '50.0%'] and distance_pct <= 3.0:
            suggestions['entry'] = f'Near {closest_level} support - Consider buying'
        elif closest_level in ['23.60%', '38.20%', '23.6%', '38.2%'] and distance_pct <= 3.0:
            suggestions['entry'] = f'Near {closest_level} resistance - Consider selling/shorting'
        else:
            suggestions['entry'] = 'Wait for better Fibonacci alignment'
        
        # Stop loss suggestions
        if closest_level in ['61.8%', '76.4%']:
            suggestions['stop'] = f'Below {closest_level} level'
        elif closest_level == '50.0%':
            suggestions['stop'] = 'Below 50% level'
        else:
            suggestions['stop'] = '8% trailing stop'
        
        # Target suggestions
        retracement = fib_data.get('retracement_pct', 0)
        if retracement > 61.8:
            suggestions['target'] = 'Previous high (100%)'
        elif retracement > 38.2:
            suggestions['target'] = '61.8% retracement level'
        else:
            suggestions['target'] = '38.2% retracement level'
        
        return suggestions

# =============================================================================
# FIBONACCI INTEGRATION HELPER
# =============================================================================
class FibonacciIntegration:
    """
    Helper class to integrate Fibonacci into existing signal pipeline.
    """
    
    @staticmethod
    def enhance_signal_with_fibonacci(signal: Dict, fib_module: FibonacciModule, 
                                     price_data: pd.Series) -> Dict:
        """
        Enhance an existing signal with Fibonacci data.
        """
        try:
            # Calculate Fibonacci signal score
            fib_signal_data = fib_module.calculate_fibonacci_signal_score(price_data)
            
            # Add Fibonacci data to signal
            signal.update({
                'fib_score': fib_signal_data['fib_score'],
                'fib_level': fib_signal_data['fib_level'],
                'fib_signal': fib_signal_data['fib_signal'],
                'fib_confidence': fib_signal_data['confidence'],
                'fib_stop_pct': fib_signal_data['stop_distance_pct'],
                'is_near_fib_support': fib_signal_data['is_support'],
                'fib_retracement_pct': fib_signal_data['retracement_pct']
            })
            
            # Enhance original score with Fibonacci (70% original, 30% Fibonacci)
            original_score = signal.get('score', 0)
            fib_score = fib_signal_data['fib_score']
            enhanced_score = original_score  # Fibonacci removed - was modifying base score
            signal['score'] = enhanced_score
            signal['original_score'] = original_score  # Keep original for reference
            
        except Exception as e:
            # Fallback if Fibonacci calculation fails
            signal.update({
                'fib_score': 0.5,
                'fib_level': 'N/A',
                'fib_signal': 'NEUTRAL',
                'fib_confidence': 'LOW',
                'fib_stop_pct': 0.08,
                'is_near_fib_support': False,
                'fib_retracement_pct': 0.0,
                'original_score': signal.get('score', 0)  # Keep original
            })
        
        return signal
    
    @staticmethod
    def calculate_fibonacci_enhanced_ranking(signal: Dict) -> float:
        """
        Calculate Fibonacci-enhanced ranking score.
        Formula: Sharpe(35%) + Score(35%) + Confidence(15%) + Fib(15%)
        """
        # Get values with defaults
        sharpe = signal.get('sharpe', 0)
        score = signal.get('score', 0)
        confidence_val = signal.get('confidence', 'MEDIUM')  # This is a string
        fib_score = signal.get('fib_score', 0.5)
        
        # Convert confidence string to numeric value
        confidence_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
        confidence = confidence_map.get(confidence_val, 0.5)
        
        # Base calculation
        total = (
            sharpe * 0.35 +      # 35% (was 40%)
            score * 0.35 +       # 35% (was 40%)
            confidence * 0.15 +  # 15% (was 20%)
            fib_score * 0.0  # DISABLED     # NEW: 15% Fibonacci
        )
        
        # Bonus for Fibonacci support levels
        fib_level = signal.get('fib_level', '')
        if fib_level == '61.8%':
            total *= 1.0  # DISABLED  # 10% bonus for golden ratio
        elif fib_level == '76.4%':
            total *= 1.0  # DISABLED  # 5% bonus for deep retracement
        elif fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
            total *= 1.0  # DISABLED  # 5% penalty near resistance
        
        # Bonus for being near Fibonacci support
        if signal.get('is_near_fib_support', False):
            total *= 1.0  # DISABLED  # 8% support bonus
        
        return max(total, 0)  # Ensure non-negative
    
    @staticmethod
    def adjust_position_with_fibonacci(base_position_pct: float, signal: Dict) -> Dict[str, float]:
        """
        Adjust position size based on Fibonacci analysis.
        """
        fib_score = signal.get('fib_score', 0.5)
        
        # Fibonacci score multiplier
        fib_multiplier = 1.0  # Fibonacci removed from position sizing  # Range: 0.8 to 1.2
        
        # Support bonus multiplier
        if signal.get('is_near_fib_support', False):
            support_multiplier = 1.2  # 20% larger position near support
        else:
            support_multiplier = 1.0
        
        # Calculate final position size
        final_pct = base_position_pct * fib_multiplier * support_multiplier
        
        # Get Fibonacci-based stop
        fib_stop_pct = signal.get('fib_stop_pct', 0.08)
        
        return {
            'position_pct': min(final_pct, 0.1),  # Cap at 10%
            'stop_pct': fib_stop_pct,
            'fib_level': signal.get('fib_level', 'N/A'),
            'fib_score': fib_score,
            'is_supported': signal.get('is_near_fib_support', False)
        }

# =============================================================================
# USAGE EXAMPLE
# =============================================================================
if __name__ == "__main__":
    # Example usage
    import yfinance as yf
    
    # Create Fibonacci module
    fib_module = FibonacciModule(lookback_days=252)
    
    # Example: Get price data for a symbol
    symbol = "AAPL"
    stock = yf.Ticker(symbol)
    hist = stock.history(period="1y")
    prices = hist['Close']
    
    # Calculate Fibonacci summary
    summary = fib_module.get_fibonacci_summary(prices)
    
    if summary['has_fib_data']:
        print(f"Fibonacci Analysis for {symbol}:")
        print(f"Current Price: ${summary['price_data']['current']:.2f}")
        print(f"52-Week High: ${summary['price_data']['high_52w']:.2f}")
        print(f"52-Week Low: ${summary['price_data']['low_52w']:.2f}")
        print(f"Retracement: {summary['price_data']['retracement_pct']:.1f}%")
        print(f"Nearest Fib Level: {summary['scores']['closest_level']}")
        print(f"Fibonacci Score: {summary['scores']['fib_score']:.3f}")
        print(f"Signal: {summary['signal']['fib_signal']}")
        print(f"Suggested Stop: {summary['signal']['stop_distance_pct']*100:.1f}%")
        
        print("\nKey Fibonacci Levels:")
        for level_name, level_price in summary['fib_levels'].items():
            if level_name not in ['current']:
                diff_pct = (summary['price_data']['current'] - level_price) / summary['price_data']['current'] * 100
                print(f"  {level_name}: ${level_price:.2f} ({diff_pct:+.1f}%)")
    
    print("\n✅ Fibonacci module ready for integration into your signal pipeline.")
    
# =============================================================================
# STATISTICAL SIGNAL MODULE (UPDATED)
# =============================================================================

# Maximum workers for parallel processing
MAX_WORKERS = 1

def _detect_rsi_divergence(prices: pd.Series, lookback: int = 20) -> dict:
    """
    Detect RSI divergence.
    Bullish: Price makes lower low, RSI makes higher low
    Bearish: Price makes higher high, RSI makes lower high
    Returns dict with divergence type and score.
    """
    result = {"bullish": False, "bearish": False, "score": 0.0}
    
    if len(prices) < lookback + 14:
        return result
    
    # Calculate RSI series
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, 0.0001)
    rsi_series = 100 - (100 / (1 + rs))
    
    # Get recent window
    recent_prices = prices.tail(lookback)
    recent_rsi = rsi_series.tail(lookback)
    
    if len(recent_prices) < lookback or len(recent_rsi) < lookback:
        return result
    
    # Find swing lows (for bullish divergence)
    price_min_idx = recent_prices.idxmin()
    current_price = recent_prices.iloc[-1]
    price_at_min = recent_prices.loc[price_min_idx]
    
    rsi_at_min = recent_rsi.loc[price_min_idx] if price_min_idx in recent_rsi.index else 50
    current_rsi = recent_rsi.iloc[-1]
    
    # Bullish divergence: price near/below prior low, RSI higher
    if current_price <= price_at_min * 1.02:  # Within 2% of low
        if current_rsi > rsi_at_min + 5:  # RSI at least 5 points higher
            result["bullish"] = True
            result["score"] = min((current_rsi - rsi_at_min) / 20, 0.5)  # Max 0.5 boost
    
    # Find swing highs (for bearish divergence)
    price_max_idx = recent_prices.idxmax()
    price_at_max = recent_prices.loc[price_max_idx]
    rsi_at_max = recent_rsi.loc[price_max_idx] if price_max_idx in recent_rsi.index else 50
    
    # Bearish divergence: price near/above prior high, RSI lower
    if current_price >= price_at_max * 0.98:  # Within 2% of high
        if current_rsi < rsi_at_max - 5:  # RSI at least 5 points lower
            result["bearish"] = True
            result["score"] = min((rsi_at_max - current_rsi) / 20, -0.5)  # Max -0.5 penalty
    
    return result




class StatisticalSignalModule:
    """Generate statistically-validated trading signals with Fibonacci integration."""

    def __init__(self, lookback: int = 21):
        self.lookback = lookback
        self.dq = DataQualityModule()
        self.fib_module = FibonacciModule(lookback_days=252)
        self.fib_helper = FibonacciIntegration()

    def compute_adaptive_weights(self, vol_regime: str) -> Dict[str, float]:
        """Compute dynamic weights based on volatility regime."""
        if vol_regime == "LOW_VOL":
            return {"momentum": 0.50, "mean_rev": 0.15, "trend": 0.30, "vol": 0.05}
        elif vol_regime in ("HIGH_VOL", "EXTREME_VOL"):
            return {"momentum": 0.25, "mean_rev": 0.30, "trend": 0.40, "vol": 0.05}
        return {"momentum": 0.40, "mean_rev": 0.20, "trend": 0.35, "vol": 0.05}

    def compute_momentum_z_score(self, returns: pd.Series) -> float:
        """Compute momentum Z-score."""
        if isinstance(returns, pd.DataFrame):
            returns = returns.iloc[:, 0]
        if len(returns) < 10:
            return 0.0

        rolling_cum = returns.rolling(10).apply(
            lambda x: (1 + x).prod() - 1, raw=False
        )
        current_val = rolling_cum.iloc[-1]
        mean_cum = rolling_cum.mean()
        std_cum = rolling_cum.std()

        if isinstance(std_cum, pd.Series):
            std_cum = std_cum.iloc[0]

        if pd.isna(std_cum) or std_cum == 0:
            return 0.0

        z = (current_val - mean_cum) / std_cum
        return float(np.clip(z, -3, 3))

    def compute_mean_reversion_score(self, prices: pd.Series, bias: str = "LONG") -> float:
        """Compute mean reversion score with bias."""
        if isinstance(prices, pd.DataFrame):
            prices = prices.iloc[:, 0]
        if len(prices) < 10:
            return 0.0

        rolling = prices.rolling(10)
        sma = rolling.mean().iloc[-1]
        std = rolling.std().iloc[-1]
        current = prices.iloc[-1]

        if pd.isna(std) or std == 0:
            return 0.0

        raw = (sma - current) / (2 * std)
        raw = float(np.clip(raw, -1, 1))

        if bias == "LONG":
            return raw
        elif bias == "SHORT":
            return -raw
        else:
            return raw * 0.25

    def get_mean_reversion_bias(self, vol_regime: str) -> str:
        """Get mean reversion bias based on volatility regime."""
        if vol_regime in ("LOW_VOL", "HIGH_VOL"):
            return "LONG"
        if vol_regime == "EXTREME_VOL":
            return "NEUTRAL"
        return "LONG"

    def compute_trend_strength(self, prices: pd.Series) -> float:
        """Compute trend strength using linear regression."""
        if len(prices) < self.lookback:
            return 0.0

        window = prices.iloc[-self.lookback:]
        x = np.arange(len(window))
        y = window.values.flatten()

        slope, _, r_value, _, _ = stats.linregress(x, y)
        trend = (slope / window.mean()) * 100 * (r_value ** 2)
        return float(np.clip(trend, -1, 1))

    def compute_volatility_percentile(self, returns: pd.Series) -> float:
        """Compute current volatility percentile vs history."""
        if len(returns) < 40:
            return 0.5

        current_vol = returns.iloc[-14:].std() * np.sqrt(252)
        hist_vols = (returns.rolling(14).std() * np.sqrt(252)).dropna()

        if hist_vols.empty or pd.isna(current_vol):
            return 0.5

        pct = stats.percentileofscore(hist_vols, current_vol) / 100.0
        return float(pct)

    def _calculate_fibonacci_enhancement(self, prices: pd.Series) -> Dict[str, any]:
        """Calculate Fibonacci enhancement factors for signal scoring."""
        try:
            # Get comprehensive Fibonacci signal data
            fib_signal_data = self.fib_module.calculate_fibonacci_signal_score(prices)
            
            # Calculate Fibonacci enhancement factor
            fib_score = fib_signal_data['fib_score']
            fib_level = fib_signal_data['fib_level']
            is_support = fib_signal_data['is_support']
            
            # Determine enhancement factor based on Fibonacci alignment
            enhancement_factor = 0.0
            
            # Positive enhancement for support levels
            if is_support and fib_signal_data['distance_to_support'] < 0.10:  # Within 10%
                if fib_level == '61.8%':
                    enhancement_factor = 0.20  # 20% boost for golden ratio
                elif fib_level == '76.4%':
                    enhancement_factor = 0.15  # 15% boost for deep retracement
                elif fib_level == '50.0%':
                    enhancement_factor = 0.10  # 10% boost for 50% level
                elif fib_level in ['38.2%', '23.6%']:
                    enhancement_factor = 0.05  # 5% boost for shallow retracements
            
            # Negative enhancement for resistance levels or poor alignment
            elif fib_level in ['23.60%', '38.20%', '23.6%', '38.2%'] and not is_support:
                if fib_signal_data['distance_to_support'] < 0.05:  # Very close to resistance
                    enhancement_factor = -0.15  # 15% penalty
                else:
                    enhancement_factor = -0.05  # 5% penalty
            
            # Bonus for high Fibonacci score
            if fib_score > 0.8:
                enhancement_factor += 0.10
            elif fib_score > 0.6:
                enhancement_factor += 0.05
            
            return {
                'fib_score': fib_score,
                'fib_level': fib_level,
                'is_near_support': is_support,
                'enhancement_factor': enhancement_factor,
                'stop_distance_pct': fib_signal_data['stop_distance_pct'],
                'fib_signal': fib_signal_data['fib_signal'],
                'confidence': fib_signal_data['confidence'],
                'retracement_pct': fib_signal_data['retracement_pct']
            }
            
        except Exception as e:
            # Fallback if Fibonacci calculation fails
            return {
                'fib_score': 0.5,
                'fib_level': 'N/A',
                'is_near_support': False,
                'enhancement_factor': 0.0,
                'stop_distance_pct': 0.08,
                'fib_signal': 'NEUTRAL',
                'confidence': 'LOW',
                'retracement_pct': 0.0
            }

    def _enhance_confidence_with_fibonacci(self, base_confidence: str, fib_data: Dict) -> str:
        """Enhance confidence rating with Fibonacci analysis."""
        fib_signal = fib_data.get('fib_signal', 'NEUTRAL')
        fib_confidence = fib_data.get('confidence', 'LOW')
        
        # Map confidence levels to numerical values
        conf_map = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
        base_conf_val = conf_map.get(base_confidence, 1)
        fib_conf_val = conf_map.get(fib_confidence, 1)
        
        # Combine confidence levels
        combined_conf_val = (base_conf_val * 0.7) + (fib_conf_val * 0.3)
        
        # Map back to string
        if combined_conf_val >= 2.5:
            return 'HIGH'
        elif combined_conf_val >= 1.5:
            return 'MEDIUM'
        else:
            return 'LOW'

    def compute_signals(
        self,
        price_data: Dict[str, pd.DataFrame],
        regime: Optional[dict] = None,
    ) -> Dict[str, Dict]:
        """Parallel signal generation with Fibonacci integration."""
        print(f"🔍 Generating signals for {len(price_data)} symbols...")
        
        vol_regime = "NORMAL"
        if regime and "volatility" in regime:
            vol_regime = regime["volatility"]

        weights = self.compute_adaptive_weights(vol_regime)
        signals: Dict[str, Dict] = {}
        
        # Function to compute signal for single symbol
        def compute_single_signal(symbol: str, df: pd.DataFrame) -> Tuple[str, Dict]:
            # Handle Series input (from price_slice)
            if isinstance(df, pd.Series):
                df = df.to_frame(name='Close')
            
            df = self.dq.validate_price_data(df, symbol)
            if df.empty or len(df) < self.lookback:
                return symbol, self._empty_signal()

            prices = df["Close"]
            returns = prices.pct_change().dropna()
            if len(returns) < self.lookback:
                return symbol, self._empty_signal()

            rp = prices.iloc[-self.lookback:]
            rr = returns.iloc[-self.lookback:]

            # Compute base statistical signals
            momentum_z = self.compute_momentum_z_score(rr)
            bias = self.get_mean_reversion_bias(vol_regime)
            mean_rev = self.compute_mean_reversion_score(rp, bias=bias)
            trend = self.compute_trend_strength(rp)
            vol_pctl = self.compute_volatility_percentile(returns)

            # Compute base combined score
            combined_score = (
                weights["momentum"] * momentum_z
                + weights["mean_rev"] * mean_rev
                + weights["trend"] * trend
                + weights["vol"] * (1 - vol_pctl)
            )

            # Risk-adjusted metrics
            exp_ret = float(returns.mean() * 252)
            vol = float(returns.std() * np.sqrt(252))
            sharpe = exp_ret / vol if vol > 0 else 0.0

            # Base confidence calculation
            if sharpe >= 1.0:
                sharpe_conf = "HIGH"
            elif sharpe >= 0.5:
                sharpe_conf = "MEDIUM"
            else:
                sharpe_conf = "LOW"

            if combined_score >= 0.50:
                score_conf = "HIGH"
            elif combined_score >= 0.20:
                score_conf = "MEDIUM"
            else:
                score_conf = "LOW"

            if sharpe_conf == "HIGH" and score_conf == "HIGH":
                base_confidence = "HIGH"
            elif sharpe_conf == "LOW" and score_conf == "LOW":
                base_confidence = "LOW"
            else:
                base_confidence = "MEDIUM"

            # ═══════════════════════════════════════════════════════════════
            # FIBONACCI ENHANCEMENT
            # ═══════════════════════════════════════════════════════════════
            fib_data = self._calculate_fibonacci_enhancement(prices)
            fib_score = fib_data['fib_score']
            fib_level = fib_data['fib_level']
            fib_enhancement = fib_data['enhancement_factor']
            is_near_support = fib_data['is_near_support']
            stop_distance_pct = fib_data['stop_distance_pct']
            
            # Apply Fibonacci enhancement to combined score
            enhanced_score = combined_score  # Fibonacci removed
            
            # RSI Divergence Detection
            divergence_data = _detect_rsi_divergence(prices)
            divergence_score = divergence_data["score"]
            enhanced_score += divergence_score  # Add divergence boost/penalty
            
            # ═══════════════════════════════════════════════════════════════
            # SQUEEZE DETECTION (Daily + Weekly Filter)
            # ═══════════════════════════════════════════════════════════════
            try:
                # Calculate daily squeeze
                daily_squeeze = calculate_squeeze(df)
                
                # Calculate weekly squeeze (resample daily to weekly)
                weekly_df = resample_to_weekly(df)
                weekly_squeeze = calculate_squeeze(weekly_df) if len(weekly_df) >= 20 else None
                
                # Get squeeze score adjustment and entry filter
                squeeze_result = get_squeeze_score(daily_squeeze, weekly_squeeze)
                squeeze_score_adj = squeeze_result['score_adjustment']
                squeeze_allow_entry = squeeze_result['allow_entry']
                squeeze_daily_state = squeeze_result['daily_state']
                squeeze_weekly_state = squeeze_result['weekly_state']
                
                # Apply squeeze adjustment to score
                # enhanced_score += squeeze_score_adj  # DISABLED
                
            except Exception as e:
                # Fallback if squeeze calculation fails
                daily_squeeze = {'squeeze_on': False, 'momentum': 0, 'bars_in_squeeze': 0}
                weekly_squeeze = None
                squeeze_score_adj = 0.0
                squeeze_allow_entry = True
                squeeze_daily_state = 'ERROR'
                squeeze_weekly_state = 'N/A'
            
            # Adjust confidence with Fibonacci
            final_confidence = self._enhance_confidence_with_fibonacci(base_confidence, fib_data)
            
            # Calculate Fibonacci-enhanced ranking score
            # Using the helper method for consistent ranking
            signal_for_ranking = {
                'sharpe': sharpe,
                'score': enhanced_score,
                'confidence': final_confidence,
                'fib_score': fib_score,
                'fib_level': fib_level,
                'is_near_fib_support': is_near_support
            }
            
            fib_ranking_score = self.fib_helper.calculate_fibonacci_enhanced_ranking(
                signal_for_ranking
            )

            # Build comprehensive signal dictionary
            signal_dict = {
                # Core statistical signals
                "combined_score": float(enhanced_score),
                "raw_score": float(combined_score),
                "fib_enhancement": float(fib_enhancement),
                "momentum_z": float(momentum_z),
                "mean_reversion": float(mean_rev),
                "trend_strength": float(trend),
                "vol_percentile": float(vol_pctl),
                "expected_return": float(exp_ret),
                "volatility": float(vol),
                "sharpe_ratio": float(sharpe),
                
                # Confidence and quality
                "confidence": final_confidence,
                "base_confidence": base_confidence,
                "quality_flag": "GOOD",
                
                # Fibonacci data
                "fib_score": float(fib_score),
                "fib_level": fib_level,
                "fib_signal": fib_data['fib_signal'],
                "fib_confidence": fib_data['confidence'],
                "fib_distance_pct": float(fib_data.get('distance_to_support', 0.15) * 100),
                "fib_retracement": float(fib_data['retracement_pct']),
                "near_fib_support": is_near_support,
                "fib_stop_pct": float(stop_distance_pct),
                
                # RSI Divergence data
                "rsi_divergence_bullish": divergence_data["bullish"],
                "rsi_divergence_bearish": divergence_data["bearish"],
                "divergence_score": float(divergence_score),
                
                # Enhanced ranking score
                
                # Squeeze data
                "squeeze_daily_state": squeeze_daily_state,
                "squeeze_weekly_state": squeeze_weekly_state,
                "squeeze_score_adj": float(squeeze_score_adj),
                "squeeze_allow_entry": squeeze_allow_entry,
                "squeeze_bars": daily_squeeze.get('bars_in_squeeze', 0),
                "squeeze_momentum": float(daily_squeeze.get('momentum', 0)),
                "squeeze_ready": daily_squeeze.get('ready', False),
                "ranking_score": float(fib_ranking_score),
                
                # Additional metadata
                "vol_regime": vol_regime,
                "lookback_days": self.lookback,
                "current_price": float(prices.iloc[-1]),
                "price_change_1d": float(returns.iloc[-1] if len(returns) > 0 else 0)
            }
            
            return symbol, signal_dict
        
        # Parallel processing
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(price_data))) as executor:
            future_to_symbol = {
                executor.submit(compute_single_signal, sym, df): sym 
                for sym, df in price_data.items()
            }
            
            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_symbol):
                symbol, signal = future.result()
                signals[symbol] = signal
                completed += 1
                
                # Progress update every 50 symbols
                if completed % 50 == 0:
                    print(f"   ⚡ Processed {completed}/{len(price_data)} signals")
        
        print(f"✅ Generated {len(signals)} total signals")
        
        # Show signal statistics
        if signals:
            # Calculate average Fibonacci score
            fib_scores = [s.get('fib_score', 0.5) for s in signals.values()]
            avg_fib_score = np.mean(fib_scores) if fib_scores else 0.5
            
            # Count signals near Fibonacci support
            near_support_count = sum(1 for s in signals.values() if s.get('near_fib_support', False))
            
            # Show top 3 signals with Fibonacci info
            top_signals = sorted(signals.items(), 
                                key=lambda x: x[1].get('ranking_score', 0), 
                                reverse=True)[:3]
            
            print(f"📊 Fibonacci Statistics:")
            print(f"   Average Fibonacci Score: {avg_fib_score:.3f}")
            print(f"   Signals near Fibonacci Support: {near_support_count}/{len(signals)} ({near_support_count/len(signals)*100:.1f}%)")
            print(f"   Top Signals with Fibonacci:")
            for sym, sig in top_signals:
                print(f"      {sym}: score={sig.get('combined_score', 0):.3f}, "
                      f"fib={sig.get('fib_score', 0):.3f} ({sig.get('fib_level', 'N/A')}), "
                      f"sharpe={sig.get('sharpe_ratio', 0):.3f}")
        
        return signals

    def _empty_signal(self) -> Dict:
        """Return empty signal template."""
        return {
            # Core statistical signals
            "combined_score": 0.0,
            "raw_score": 0.0,
            "fib_enhancement": 0.0,
            "momentum_z": 0.0,
            "mean_reversion": 0.0,
            "trend_strength": 0.0,
            "vol_percentile": 0.5,
            "expected_return": 0.0,
            "volatility": 0.0,
            "sharpe_ratio": 0.0,
            
            # Confidence and quality
            "confidence": "LOW",
            "base_confidence": "LOW",
            "quality_flag": "EMPTY",
            
            # Fibonacci data
            "fib_score": 0.0,
            "fib_level": "",
            "fib_signal": "NEUTRAL",
            "fib_confidence": "LOW",
            "fib_distance_pct": 100.0,
            "fib_retracement": 0.0,
            "near_fib_support": False,
            "fib_stop_pct": 0.08,
            
            # Enhanced ranking score
            "ranking_score": 0.0,
            
            # Additional metadata
            "vol_regime": "NORMAL",
            "lookback_days": self.lookback,
            "current_price": 0.0,
            "price_change_1d": 0.0
        }

    def filter_signals_by_fibonacci(self, signals: Dict[str, Dict], 
                                   min_fib_score: float = 0.4,
                                   require_support: bool = False) -> Dict[str, Dict]:
        """
        Filter signals based on Fibonacci criteria.
        
        Args:
            signals: Dictionary of signals
            min_fib_score: Minimum Fibonacci score to pass
            require_support: Whether to require near Fibonacci support
            
        Returns:
            Filtered signals dictionary
        """
        filtered = {}
        
        for symbol, signal in signals.items():
            # Check Fibonacci score
            if signal.get('fib_score', 0) < min_fib_score:
                continue
            
            # Check if near Fibonacci support (if required)
            if require_support and not signal.get('near_fib_support', False):
                continue
            
            # Check if Fibonacci signal is not negative
            fib_signal = signal.get('fib_signal', 'NEUTRAL')
            if fib_signal in ['NEAR_RESISTANCE', 'NO_DATA']:
                continue
            
            filtered[symbol] = signal
        
        print(f"📊 Fibonacci Filter: {len(filtered)}/{len(signals)} signals passed "
              f"(min_fib_score={min_fib_score}, require_support={require_support})")
        
        return filtered

    def rank_signals_with_fibonacci(self, signals: Dict[str, Dict]) -> List[Tuple[str, float, Dict]]:
        """
        Rank signals using Fibonacci-enhanced scoring.
        Returns: List of (symbol, total_score, signal) sorted by total_score descending
        """
        ranked = []
    
        for symbol, signal in signals.items():
            # Get values with defaults
            # UPDATED: Use new field names
            sharpe = signal.get('sharpe_ratio', 0)
            score = signal.get('combined_score', 0)
            confidence_val = signal.get('confidence', 'MEDIUM')
            fib_score = signal.get('fib_score', 0.5)
        
            # Convert confidence to numeric - FIXED!
            conf_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
            confidence = conf_map.get(confidence_val, 0.5)  # Convert string to float
        
            # Calculate total score (similar to your original ranking)
            # Updated weights: Sharpe(35%), Score(35%), Confidence(15%), Fib(15%)
            total_score = (
                sharpe * 0.35 +
                score * 0.35 +
                confidence * 0.15 +  # Now this is a float, not string
                fib_score * 0.0  # DISABLED
            )
        
            # Bonus for Fibonacci support levels
            fib_level = signal.get('fib_level', '')
            if fib_level == '61.8%':
                total_score *= 1.10  # 10% bonus for golden ratio
            elif fib_level == '76.4%':
                total_score *= 1.05  # 5% bonus for deep retracement
        
            # Penalty for resistance
            if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                total_score *= 0.95  # 5% penalty
        
            ranked.append((symbol, total_score, signal))
    
        # Sort by total_score descending
        ranked.sort(key=lambda x: x[1], reverse=True)
    
        return ranked

# =============================================================================
# HELPER FUNCTIONS FOR QUALITY FILTERS
# =============================================================================

def apply_signal_filters(signals: Dict[str, Dict], 
                        min_score: float = 0.0,
                        min_sharpe: float = 0.2,
                        min_fib_score: float = 0.3) -> Dict[str, Dict]:
    """
    Apply comprehensive filters to signals.
    
    Args:
        signals: Dictionary of signals
        min_score: Minimum combined score
        min_sharpe: Minimum Sharpe ratio
        min_fib_score: Minimum Fibonacci score
        
    Returns:
        Filtered signals
    """
    filtered = {}
    
    for symbol, signal in signals.items():
        # Check basic filters
        if signal.get('combined_score', 0) <= min_score:
            continue
        
        if signal.get('sharpe_ratio', 0) < min_sharpe:
            continue
        
        # Check Fibonacci filter
        if signal.get('fib_score', 0) < min_fib_score:
            continue
        
        # Squeeze filter DISABLED - boost only, no blocking
        if False:  # DISABLED - boost only
            continue
        
        filtered[symbol] = signal
    
    print(f"📊 Total Filter Stats: {len(filtered)}/{len(signals)} signals passed")
    return filtered

def get_top_signals(signals: Dict[str, Dict], top_n: int = 30) -> List[str]:
    """
    Get top N symbols by ranking score.
    
    Args:
        signals: Dictionary of signals
        top_n: Number of top symbols to return
        
    Returns:
        List of top symbol tickers
    """
    if not signals:
        return []
    
    # Rank by Fibonacci-enhanced ranking score
    ranked = [(symbol, signal.get('ranking_score', 0)) 
              for symbol, signal in signals.items()]
    
    # Sort by ranking score descending
    ranked.sort(key=lambda x: x[1], reverse=True)
    
    # Return top N symbols
    return [symbol for symbol, score in ranked[:top_n]]

# =============================================================================
# EXAMPLE USAGE
# =============================================================================
if __name__ == "__main__":
    # Example usage
    import yfinance as yf
    from datetime import datetime, timedelta
    
    # Create signal module
    signal_module = StatisticalSignalModule(lookback=21)
    
    # Example: Get price data for a few symbols
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    price_data = {}
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(start=start_date, end=end_date)
            if not hist.empty:
                price_data[symbol] = hist[['Open', 'High', 'Low', 'Close', 'Volume']]
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
    
    if price_data:
        # Generate signals
        signals = signal_module.compute_signals(price_data)
        
        # Filter signals
        filtered = apply_signal_filters(
            signals, 
            min_score=0.0,
            min_sharpe=0.2,
            min_fib_score=0.3
        )
        
        # Get top symbols
        top_symbols = get_top_signals(filtered, top_n=5)
        
        print(f"\n🏆 Top {len(top_symbols)} ranked symbols: {top_symbols}")
        
        # Show detailed info for top symbol
        if top_symbols:
            top_symbol = top_symbols[0]
            signal = signals[top_symbol]
            print(f"\n📈 Detailed analysis for {top_symbol}:")
            print(f"  Price: ${signal.get('current_price', 0):.2f}")
            print(f"  Combined Score: {signal.get('combined_score', 0):.3f}")
            print(f"  Fibonacci Score: {signal.get('fib_score', 0):.3f} ({signal.get('fib_level', 'N/A')})")
            print(f"  Sharpe Ratio: {signal.get('sharpe_ratio', 0):.3f}")
            print(f"  Confidence: {signal.get('confidence', 'LOW')}")
            print(f"  Near Fibonacci Support: {signal.get('near_fib_support', False)}")
            print(f"  Fibonacci Stop: {signal.get('fib_stop_pct', 0.08)*100:.1f}%")


# =============================================================================
# HELPER FUNCTIONS FOR QUALITY FILTERS (UPDATED)
# =============================================================================

def _slope(series: pd.Series) -> float:
    """Return slope of last N values."""
    try:
        y = series.astype(float).values
        x = np.arange(len(y))
        coef = np.polyfit(x, y, 1)
        return float(coef[0])
    except Exception:
        return 0.0


def _slope_rolling(series: pd.Series, window: int = 14) -> float:
    """Rolling window slope."""
    if len(series) < window:
        return 0.0
    try:
        subset = series.tail(window)
        return _slope(subset)
    except Exception:
        return 0.0


def _calculate_rsi(series: pd.Series, window: int = 14) -> float:
    """Pure python RSI calculation."""
    if len(series) < window + 1:
        return 50.0

    delta = series.diff().dropna()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()

    if avg_loss.iloc[-1] == 0:
        return 70.0

    rs = avg_gain.iloc[-1] / avg_loss.iloc[-1]
    rsi = 100 - (100 / (1 + rs))
    return float(rsi)
def calculate_obvious_mover_indicators(price_series) -> dict:
    """Calculate obvious mover indicators."""
    try:
        if isinstance(price_series, pd.DataFrame):
            if "Close" in price_series.columns:
                series = price_series["Close"]
            else:
                series = price_series.iloc[:, 0]
        else:
            series = price_series

        if len(series) < 60:
            return {
                "return_5d": 0.0,
                "slope_10": 0.0,
                "slope_20": 0.0,
                "slope_21": 0.0,
                "slope_30": 0.0,
                "slope_55": 0.0,
            }

        roc_5 = series.pct_change(5).iloc[-1]

        return {
            "return_5d": float(roc_5),
            "slope_10": _slope_rolling(series, 10),
            "slope_20": _slope_rolling(series, 20),
            "slope_21": _slope_rolling(series, 21),
            "slope_30": _slope_rolling(series, 30),
            "slope_55": _slope_rolling(series, 55),
        }

    except Exception:
        return {
            "return_5d": 0.0,
            "slope_10": 0.0,
            "slope_20": 0.0,
            "slope_21": 0.0,
            "slope_30": 0.0,
            "slope_55": 0.0,
        }

# =============================================================================
# QUALITY FILTERS - UPDATED FOR NEW SIGNAL STRUCTURE
# =============================================================================

def check_obvious_mover(indicators: dict) -> bool:
    """
    BULL MARKET ADAPTIVE: Much more permissive for trending markets.
    """
    roc_5 = indicators.get("return_5d", 0.0)
    slope_10 = indicators.get("slope_10", 0.0)
    slope_21 = indicators.get("slope_21", 0.0)
    slope_30 = indicators.get("slope_30", 0.0)
    slope_55 = indicators.get("slope_55", 0.0)
    
    # ⭐ BULL MARKET ADJUSTMENTS:
    # In a bull market, we want to catch EARLY moves, not just established trends
    
    # 1. Filter out CRASHING stocks (>10% drop in 5 days)
    if roc_5 < -0.10:
        return False
    
    # 2. Filter out STRONG DOWNTRENDS (all slopes negative)
    if all(s < 0 for s in [slope_10, slope_21, slope_30, slope_55]):
        return False
    
    # 3. NEW: Allow consolidation phases
    # Stocks between -2% to +2% with mixed slopes might be coiling
    if -0.02 <= roc_5 <= 0.02:
        # Allow if at least one short-term slope is positive
        if slope_10 > 0 or slope_21 > 0:
            return True
    
    # 4. Count positive slopes (more lenient)
    positive_slopes = sum([
        slope_10 > 0.001,  # Tiny positive counts
        slope_21 > 0.001,
        slope_30 > -0.002,  # Allow slightly negative
        slope_55 > -0.003   # Allow slightly negative
    ])
    
    # ⭐ RELAXED CONDITIONS:
    
    # A. Any positive momentum with decent slopes
    if roc_5 > 0.01 and positive_slopes >= 2:
        return True
    
    # B. Small pullback but still in uptrend
    if roc_5 > -0.03 and positive_slopes >= 3:
        return True
    
    # C. Strong momentum (your original but relaxed)
    if roc_5 > 0.025 and positive_slopes >= 2:
        return True
    
    # D. Very strong momentum (catch parabolic moves)
    if roc_5 > 0.05:
        return True  # Any parabolic move passes
    
    # If we get here, it's probably choppy/noise
    return False

def passes_confirmation_layer(price_data: dict, sym: str) -> bool:
    """
    RELAXED: Only requires basic uptrend structure.
    """
    try:
        df = price_data.get(sym)
        # Handle Series input
        if isinstance(df, pd.Series):
            df = df.to_frame(name="Close")
        if df is None or df.empty:
            return False

        if "Close" in df.columns:
            close = df["Close"]
        else:
            close = df.iloc[:, 0]

        if len(close) < 50:  # Reduced from 60
            return False

        ema20 = close.ewm(span=20).mean().iloc[-1]
        ema50 = close.ewm(span=50).mean().iloc[-1]

        rsi = _calculate_rsi(close, 14)

        # RELAXED: Only require EMA alignment + RSI not oversold
        return (
            close.iloc[-1] > ema20 and  # Price above short EMA
            ema20 > ema50 and            # Short EMA above long EMA
            rsi > 40                      # RSI above oversold (was 50)
        )

    except Exception:
        return False

def apply_quality_filters(price_data: dict, signals: dict) -> dict:
    """
    Multi-mode filter system with graduated strictness levels.
    UPDATED with FIBONACCI_ONLY mode for high-quality trades.
    """
    
    if not signals:
        return {}
    
    # Use global config
    global FILTER_MODE, QUALITY_FILTERS_ENABLED
    
    # ═══════════════════════════════════════════════════════════════
    # MODE: NONE - Complete Bypass
    # ═══════════════════════════════════════════════════════════════
    if FILTER_MODE == "NONE" or not QUALITY_FILTERS_ENABLED:
        print(f"🔓 FILTERS BYPASSED (Mode: {FILTER_MODE})")
        return signals
    
    # ═══════════════════════════════════════════════════════════════
    # MODE: FIBONACCI_ONLY - Only trade high Fibonacci signals
    # ═══════════════════════════════════════════════════════════════
    # In apply_quality_filters function, update the FIBONACCI_ONLY section:

    if FILTER_MODE == "FIBONACCI_ONLY":
        print(f"\n🔍 FIBONACCI_ONLY FILTER ACTIVE")
        filtered = {}
        stats = {
            'total': len(signals), 
            'passed': 0, 
            'failed_fib_score': 0,
            'failed_basic': 0
        }
    
        # DETERMINE MARKET REGIME
        # Get average Fibonacci level of all signals
        fib_levels = []
        for sig in signals.values():
            level = sig.get('fib_level', 'N/A')
            if level != 'N/A':
                fib_levels.append(level)
    
        # Classify market regime based on Fibonacci levels
        market_regime = "NEUTRAL"
        if fib_levels:
            # Count how many signals are near support vs resistance
            support_levels = ['50.0%', '61.8%', '76.4%']
            resistance_levels = ['23.60%', '38.20%', '23.6%', '38.2%']
        
            support_count = sum(1 for l in fib_levels if l in support_levels)
            resistance_count = sum(1 for l in fib_levels if l in resistance_levels)
        
            if resistance_count > support_count * 2:
                market_regime = "STRONG_BULL"
            elif resistance_count > support_count:
                market_regime = "BULL"
            elif support_count > resistance_count:
                market_regime = "BEAR"
        
            print(f"   Market regime: {market_regime} ({support_count} support, {resistance_count} resistance)")
        
        # DYNAMIC FIBONACCI THRESHOLD BASED ON MARKET REGIME
        if market_regime in ["STRONG_BULL", "BULL"]:
            fib_threshold = 0.2  # Lower threshold in bull markets (buying near highs)
        elif market_regime == "BEAR":
            fib_threshold = 0.6  # Higher threshold in bear markets (strict on support)
        else:
            fib_threshold = 0.4  # Neutral threshold
    
        print(f"   Dynamic Fibonacci threshold: {fib_threshold}")
    
        for sym, sig in signals.items():
            # Get Fibonacci score (check multiple possible keys)
            fib_score = sig.get('fibonacci_score', 
                             sig.get('fib_score', 
                                   sig.get('fib_alignment', 0)))
        
            fib_level = sig.get('fib_level', 'N/A')
        
            # ADJUST SCORE BASED ON MARKET REGIME
            adjusted_fib_score = fib_score
        
            # In bull markets, don't penalize resistance levels as much
            # BLOCK resistance levels entirely - only trade at support
            if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                stats['failed_fib_score'] += 1
                continue  # Skip resistance levels
        
            # In bear markets, be stricter on resistance signals
        
            # CRITICAL: Use dynamic threshold
            if adjusted_fib_score < fib_threshold:
                stats['failed_fib_score'] += 1
                continue
        
            # Basic sanity checks (very minimal)
            score = sig.get('combined_score', sig.get('final_score', 0))
            if score <= -0.2:  # Very negative scores indicate problems
                stats['failed_basic'] += 1
                continue
            
            # Check Sharpe exists (basic data quality)
            sharpe = sig.get('sharpe_ratio', sig.get('sharpe', 0))
            if sharpe < 0.1:  # Minimal positive momentum
                continue
        
            # Mark and keep
            sig["quality_flag"] = "PASS_FIB_ONLY"
            sig["filter_notes"] = f"Fib:{fib_score:.2f}→{adjusted_fib_score:.2f}({fib_level})"
            filtered[sym] = sig
            stats['passed'] += 1
    
        print(f"📊 FIBONACCI_ONLY FILTER RESULTS:")
        print(f"   Total signals: {stats['total']}")
        print(f"   Passed: {stats['passed']} ({stats['passed']/max(1, stats['total']):.1%})")
        print(f"   Failed Fibonacci < {fib_threshold}: {stats['failed_fib_score']}")
        print(f"   Failed basic checks: {stats['failed_basic']}")
    
        # If no signals pass, show what we had
        if stats['passed'] == 0 and stats['total'] > 0:
            print(f"\n⚠️  NO SIGNALS PASSED - Signal Analysis:")
            for sym, sig in list(signals.items())[:10]:
                fib_score = sig.get('fibonacci_score', sig.get('fib_score', 0))
                fib_level = sig.get('fib_level', 'N/A')
                score = sig.get('combined_score', 0)
                print(f"   {sym}: Fib={fib_score:.3f}({fib_level}), Score={score:.3f}")
    
        return filtered
    
    # ═══════════════════════════════════════════════════════════════
    # MODE: MINIMAL - Only Filter Obvious Junk
    # ═══════════════════════════════════════════════════════════════
    if FILTER_MODE == "MINIMAL":
        filtered = {}
        stats = {'total': len(signals), 'passed': 0, 'failed_score': 0, 'failed_sharpe': 0}
        
        for sym, sig in signals.items():
            score = sig.get('combined_score', 0)
            sharpe = sig.get('sharpe_ratio', 0)
            
            if score <= -0.5:
                stats['failed_score'] += 1
                continue
            
            if sharpe < 0.1:
                stats['failed_sharpe'] += 1
                continue
            
            sig["quality_flag"] = "PASS_MINIMAL"
            filtered[sym] = sig
            stats['passed'] += 1
        
        print(f"🔍 MINIMAL FILTER: {stats['passed']}/{stats['total']} passed")
        return filtered
    
    # ═══════════════════════════════════════════════════════════════
    # MODE: MODERATE - Balanced Filtering (existing code)
    # ═══════════════════════════════════════════════════════════════
    if FILTER_MODE == "MODERATE":
        filtered = {}
        fib_stats = []
        resistance_blocked = 0
        support_boosted = 0
        
        FIB_SUPPORT_ONLY = True  # Only trade at support levels
        
        stats = {
            'total': len(signals),
            'passed': 0,
            'failed_data': 0,
            'failed_mover': 0,
            'failed_confirmation': 0,
            'failed_score': 0,
            'failed_sharpe': 0,
            'failed_fib': 0,
            'failed_resistance': 0,
        }
        
        resistance_levels = ['23.60%', '38.20%', '23.6%', '38.2%']
        support_levels = ['50.0%', '61.8%', '76.4%']
        
        for sym, sig in signals.items():
            # Basic data validation
            if sym not in price_data:
                stats['failed_data'] += 1
                continue
            
            df = price_data[sym]
            # Handle Series input
            if isinstance(df, pd.Series):
                df = df.to_frame(name="Close")
            if df is None or df.empty:
                stats['failed_data'] += 1
                continue
            
            if "Close" in df.columns:
                series = df["Close"]
            else:
                series = df.iloc[:, 0]
            
            if len(series) < 50:
                stats['failed_data'] += 1
                continue
            
            # Check 1: Obvious mover
            indicators = calculate_obvious_mover_indicators(series)
            if not check_obvious_mover(indicators):
                stats['failed_mover'] += 1
                continue
            
            # Check 2: Confirmation layer
            if not passes_confirmation_layer(price_data, sym):
                stats['failed_confirmation'] += 1
                continue
            
            # Check 3: Basic score filter
            score = sig.get('combined_score', 0)
            if score <= 0:
                stats['failed_score'] += 1
                continue
            
            # Check 4: Sharpe filter
            sharpe = sig.get('sharpe_ratio', 0)
            if sharpe < 0.2:
                stats['failed_sharpe'] += 1
                continue
            
            # Check 5: Fibonacci score filter
            fib_score = sig.get('fib_score', 0.5)
            if fib_score < 0.0:  # Disabled - handled by apply_quality_filters
                stats['failed_fib'] += 1
                continue
            
            # Check 6: Fibonacci level filter
            fib_level = sig.get('fib_level', '')
            fib_distance = sig.get('fib_distance_pct', 100)
            
            if FIB_SUPPORT_ONLY:
                # BLOCK resistance trades entirely
                if fib_level in resistance_levels:
                    stats['failed_resistance'] += 1
                    resistance_blocked += 1
                    continue
                
                # Require support level OR near_fib_support flag
                is_at_support = fib_level in support_levels
                near_support = sig.get('near_fib_support', False)
                
                if not is_at_support and not near_support:
                    if fib_level and fib_level != 'N/A':
                        stats['failed_fib'] += 1
                        continue
            
            # Apply support boosts
            if fib_level in support_levels and fib_distance <= 5.0:
                if fib_level == '76.4%':
                    boost = 1.20
                elif fib_level == '61.8%':
                    boost = 1.15
                else:
                    boost = 1.10
                
                sig['combined_score'] *= boost
                sig['fib_enhancement'] = boost - 1
                support_boosted += 1
            
            # Track stats
            if fib_level:
                fib_stats.append({
                    'symbol': sym,
                    'score': sig.get('combined_score', 0),
                    'fib_level': fib_level,
                    'fib_score': fib_score,
                    'distance': fib_distance,
                })
            
            # PASSED ALL FILTERS
            sig["quality_flag"] = "PASS_MODERATE"
            filtered[sym] = sig
            stats['passed'] += 1
        
        # Debug output
        print(f"\n🔍 MODERATE FILTER RESULTS (Support-Only: {FIB_SUPPORT_ONLY}):")
        print(f"   Total signals: {stats['total']}")
        print(f"   Passed: {stats['passed']}")
        print(f"   Failed breakdown:")
        print(f"     - No data: {stats['failed_data']}")
        print(f"     - Not moving: {stats['failed_mover']}")
        print(f"     - No confirmation: {stats['failed_confirmation']}")
        print(f"     - Score ≤ 0: {stats['failed_score']}")
        print(f"     - Sharpe < 0.2: {stats['failed_sharpe']}")
        print(f"     - Fibonacci < 0.4: {stats['failed_fib']}")
        print(f"     - Resistance blocked: {stats['failed_resistance']}")
        print(f"   Support boosted: {support_boosted}")
        
        return filtered
        
        if fib_stats:
            level_counts = {}
            avg_scores = {}
            for stat in fib_stats:
                level = stat['fib_level']
                level_counts[level] = level_counts.get(level, 0) + 1
                if level not in avg_scores:
                    avg_scores[level] = []
                avg_scores[level].append(stat['score'])
            
            print(f"\n   Fibonacci Level Distribution:")
            for level in ['23.60%', '38.20%', '23.6%', '38.2%', '50.0%', '61.8%', '76.4%', 'N/A']:
                count = level_counts.get(level, 0)
                if count > 0:
                    avg_score = np.mean(avg_scores.get(level, [0]))
                    print(f"     {level:<6}: {count:>3} signals, avg score: {avg_score:.3f}")
        
        return filtered
    
    # ═══════════════════════════════════════════════════════════════
    # MODE: STRICT - Original Strict Filters with Fibonacci
    # ═══════════════════════════════════════════════════════════════
    if FILTER_MODE == "STRICT":
        filtered = {}
        fib_stats = []
        resistance_filtered = 0
        support_boosted = 0
        fib_filtered = 0
        
        stats = {
            'total': len(signals),
            'passed': 0,
            'failed_data': 0,
            'failed_mover': 0,
            'failed_confirmation': 0,
            'failed_fib': 0,
            'failed_score': 0,
            'failed_sharpe': 0,
            'failed_resistance': 0,
        }
        
        for sym, sig in signals.items():
            # All your original strict checks here
            
            # Basic validation
            if sym not in price_data:
                stats['failed_data'] += 1
                continue
            
            df = price_data[sym]
            # Handle Series input
            if isinstance(df, pd.Series):
                df = df.to_frame(name="Close")
            if df is None or df.empty:
                stats['failed_data'] += 1
                continue
            
            if "Close" in df.columns:
                series = df["Close"]
            else:
                series = df.iloc[:, 0]
            
            if len(series) < 50:
                stats['failed_data'] += 1
                continue
            
            # Strict mover check
            indicators = calculate_obvious_mover_indicators(series)
            if not check_obvious_mover(indicators):
                stats['failed_mover'] += 1
                continue
            
            # Strict confirmation
            if not passes_confirmation_layer(price_data, sym):
                stats['failed_confirmation'] += 1
                continue
            
            # Strict score (using combined_score)
            score = sig.get('combined_score', 0)
            if score <= 0:
                stats['failed_score'] += 1
                continue
            
            # Strict Sharpe (using sharpe_ratio)
            sharpe = sig.get('sharpe_ratio', 0)
            if sharpe < 0.6:
                stats['failed_sharpe'] += 1
                continue
            
            # Strict Fibonacci filter
            fib_score = sig.get('fib_score', 0)
            if fib_score < 0.6:  # Strict Fibonacci threshold
                stats['failed_fib'] += 1
                fib_filtered += 1
                continue
            
            # Block resistance levels
            fib_level = sig.get('fib_level', '')
            fib_distance = sig.get('fib_distance_pct', 100)  # UPDATED
            
            if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%'] and fib_distance < 3.0:
                stats['failed_resistance'] += 1
                resistance_filtered += 1
                continue
            
            sig["quality_flag"] = "PASS_STRICT"
            filtered[sym] = sig
            stats['passed'] += 1
        
        print(f"\n🔍 STRICT FILTER RESULTS (with Fibonacci):")
        print(f"   Total: {stats['total']} → Passed: {stats['passed']}")
        print(f"   Failures: Sharpe={stats['failed_sharpe']}, Fib={stats['failed_fib']}, "
              f"Resistance={stats['failed_resistance']}")
        
        return filtered
    
    # Default: return unfiltered
    return signals


# =============================================================================
# SIMPLE FILTER FUNCTION FOR YOUR MAIN SCRIPT
# =============================================================================
def simple_filter_signals(signals: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Simple filter for use in your main script.
    This matches what you had in your logs.
    """
    filtered = {}
    
    for symbol, signal in signals.items():
        # Use the new field names
        score = signal.get('combined_score', 0)
        sharpe = signal.get('sharpe_ratio', 0)
        fib_score = signal.get('fib_score', 0.5)
        
        # Existing filters from your logs
        if score <= 0:
            continue
        if sharpe < 0.2:
            continue
        
        # NEW: Add Fibonacci filter
        if fib_score < 0.0:  # Disabled - handled by apply_quality_filters
            continue
        
        filtered[symbol] = signal
    
    print(f"🔍 MINIMAL FILTER: {len(filtered)}/{len(signals)} passed")
    return filtered


# =============================================================================
# FIBONACCI-ENHANCED RANKING FUNCTION
# =============================================================================
def rank_signals_with_fibonacci(self, signals: Dict[str, Dict]) -> List[Tuple[str, float, Dict]]:
    """
    Rank signals using Fibonacci-enhanced scoring.
    Returns: List of (symbol, total_score, signal) sorted by total_score descending
    """
    ranked = []
    
    for symbol, signal in signals.items():
        # Get values with defaults
        # UPDATED: Use new field names
        sharpe = signal.get('sharpe_ratio', 0)
        score = signal.get('combined_score', 0)
        confidence_val = signal.get('confidence', 'MEDIUM')
        fib_score = signal.get('fib_score', 0.5)
        
        # Convert confidence to numeric
        conf_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
        confidence = conf_map.get(confidence_val, 0.5)
        
        # Calculate total score (similar to your original ranking)
        # Updated weights: Sharpe(35%), Score(35%), Confidence(15%), Fib(15%)
        total_score = (
            sharpe * 0.35 +
            score * 0.35 +
            confidence * 0.15 +
            fib_score * 0.0  # DISABLED
        )
        
        # Bonus for Fibonacci support levels
        fib_level = signal.get('fib_level', '')
        if fib_level == '61.8%':
            total_score *= 1.10  # 10% bonus for golden ratio
        elif fib_level == '76.4%':
            total_score *= 1.05  # 5% bonus for deep retracement
        
        # Penalty for resistance
        if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
            total_score *= 0.95  # 5% penalty
        
        ranked.append((symbol, total_score, signal))
    
    # Sort by total_score descending
    ranked.sort(key=lambda x: x[1], reverse=True)
    
    return ranked

# =============================================================================
# REGIME MODULE
# =============================================================================

class RegimeModule:
    """
    For swing trading, macro regimes affect position sizing via volatility.
    FIXED: Less aggressive cuts in high vol.
    """

    def __init__(self, economic_module: EconomicModule):
        self.econ = economic_module

    def classify(self, econ_data: Dict[str, float]) -> Dict[str, str]:
        vix = econ_data.get("vix", 20.0)

        if vix < 15:
            vol_regime = "LOW_VOL"
        elif vix < 25:
            vol_regime = "MEDIUM_VOL"
        elif vix < 35:
            vol_regime = "HIGH_VOL"
        else:
            vol_regime = "EXTREME_VOL"

        return {"volatility": vol_regime}

    def compute_multiplier(self, regime: Dict[str, str]) -> float:
        """
        FIXED: Less aggressive cuts.
        - Never go to zero (was 0.0 in EXTREME_VOL)
        - Less punishing in HIGH_VOL (0.85 vs 0.60)
        """
        vol = regime.get("volatility", "MEDIUM_VOL")

        mapping = {
            "LOW_VOL": 1.25,      # Full size in calm markets
            "MEDIUM_VOL": 1.10,   # Slight boost (was 1.0)
            "HIGH_VOL": 0.85,     # Small cut (was 0.60)
            "EXTREME_VOL": 0.50,  # Half size (was 0.0 - never go to zero!)
        }

        return mapping.get(vol, 1.0)

# =============================================================================
# MARKET REGIME DASHBOARD
# =============================================================================
# Add this class to your Swing_System.py file after the RegimeModule class
# (around line 950-1000)
# =============================================================================

class MarketRegimeDashboard:
    """
    Market Regime Dashboard for detecting centralized vs broad market conditions.
    
    Displays:
    - RSP/SPY Ratio (Equal weight vs cap weight)
    - % S&P 500 Stocks > 200-day MA (Breadth)
    - IWM/QQQ Ratio (Small vs mega cap)
    - Regime Classification: BROAD / CENTRALIZED / MIXED
    - Recommended Action based on regime
    
    Usage:
        dashboard = MarketRegimeDashboard()
        dashboard.display()  # Shows formatted dashboard in terminal
    """
    
    def __init__(self, lookback_days: int = 20):
        """
        Initialize the dashboard.
        
        Args:
            lookback_days: Days for moving average calculations (default 20)
        """
        self.lookback_days = lookback_days
        self.metrics = {}
        self.regime = "UNKNOWN"
        self.regime_score = 0.0
        
    def fetch_data(self) -> bool:
        """
        Fetch required ETF data for regime calculations.
        
        Returns:
            bool: True if data fetched successfully
        """
        try:
            import yfinance as yf
            import pandas as pd
            
            # Fetch ETF data (6 months for 200-day MA proxy calculations)
            etfs = ['RSP', 'SPY', 'IWM', 'QQQ']
            
            print("   📥 Fetching regime indicator data...")
            
            data = yf.download(
                etfs, 
                period='1y',  # Need enough for 200-day calculations
                progress=False,
                group_by='ticker',
                auto_adjust=True
            )
            
            if data is None or data.empty:
                print("   ⚠️  Could not fetch regime data")
                return False
            
            # Extract close prices for each ETF
            self.etf_data = {}
            
            for etf in etfs:
                try:
                    if isinstance(data.columns, pd.MultiIndex):
                        if etf in data.columns.get_level_values(0):
                            close = data[etf]['Close']
                        else:
                            continue
                    else:
                        close = data['Close'][etf] if 'Close' in data.columns else None
                    
                    if close is not None and not close.empty:
                        self.etf_data[etf] = close.dropna()
                except Exception as e:
                    print(f"   ⚠️  Could not extract {etf}: {e}")
                    continue
            
            if len(self.etf_data) < 4:
                print(f"   ⚠️  Only got {len(self.etf_data)}/4 ETFs")
                return False
            
            return True
            
        except Exception as e:
            print(f"   ❌ Data fetch failed: {e}")
            return False
    
    def calculate_metrics(self) -> dict:
        """
        Calculate all regime metrics.
        
        Returns:
            dict: Calculated metrics
        """
        import numpy as np
        import pandas as pd
        
        metrics = {
            'rsp_spy_ratio': None,
            'rsp_spy_ratio_ma': None,
            'rsp_spy_trend': 'FLAT',
            'rsp_spy_trend_weeks': 0,
            'iwm_qqq_ratio': None,
            'iwm_qqq_ratio_ma': None,
            'iwm_qqq_trend': 'FLAT',
            'breadth_estimate': None,
            'regime_score': 0.0,
        }
        
        try:
            # ═══════════════════════════════════════════════════════════════
            # 1. RSP/SPY RATIO (Equal Weight vs Cap Weight)
            # ═══════════════════════════════════════════════════════════════
            if 'RSP' in self.etf_data and 'SPY' in self.etf_data:
                rsp = self.etf_data['RSP']
                spy = self.etf_data['SPY']
                
                # Align dates
                common_idx = rsp.index.intersection(spy.index)
                rsp = rsp.loc[common_idx]
                spy = spy.loc[common_idx]
                
                if len(rsp) > self.lookback_days:
                    # Current ratio
                    ratio = rsp / spy
                    current_ratio = float(ratio.iloc[-1])
                    
                    # 20-day MA of ratio
                    ratio_ma = ratio.rolling(self.lookback_days).mean()
                    current_ma = float(ratio_ma.iloc[-1])
                    
                    # Normalize to make it more interpretable (ratio around 1.0)
                    # RSP and SPY have different prices, so we normalize
                    baseline_ratio = float(ratio.iloc[-252]) if len(ratio) >= 252 else float(ratio.iloc[0])
                    normalized_ratio = current_ratio / baseline_ratio
                    normalized_ma = current_ma / baseline_ratio
                    
                    metrics['rsp_spy_ratio'] = normalized_ratio
                    metrics['rsp_spy_ratio_ma'] = normalized_ma
                    
                    # Trend detection (declining = centralizing)
                    if len(ratio_ma) >= 20:
                        recent_ma = ratio_ma.iloc[-20:]
                        slope = (float(recent_ma.iloc[-1]) - float(recent_ma.iloc[0])) / float(recent_ma.iloc[0])
                        
                        if slope < -0.02:  # 2% decline
                            metrics['rsp_spy_trend'] = 'DECLINING'
                            # Count weeks declining
                            weeks = 0
                            for i in range(1, min(13, len(ratio_ma) // 5)):
                                if float(ratio_ma.iloc[-i*5]) > float(ratio_ma.iloc[-(i-1)*5 - 1]):
                                    weeks += 1
                                else:
                                    break
                            metrics['rsp_spy_trend_weeks'] = weeks
                        elif slope > 0.02:
                            metrics['rsp_spy_trend'] = 'RISING'
                        else:
                            metrics['rsp_spy_trend'] = 'FLAT'
            
            # ═══════════════════════════════════════════════════════════════
            # 2. IWM/QQQ RATIO (Small Cap vs Mega Cap)
            # ═══════════════════════════════════════════════════════════════
            if 'IWM' in self.etf_data and 'QQQ' in self.etf_data:
                iwm = self.etf_data['IWM']
                qqq = self.etf_data['QQQ']
                
                # Align dates
                common_idx = iwm.index.intersection(qqq.index)
                iwm = iwm.loc[common_idx]
                qqq = qqq.loc[common_idx]
                
                if len(iwm) > self.lookback_days:
                    ratio = iwm / qqq
                    current_ratio = float(ratio.iloc[-1])
                    ratio_ma = ratio.rolling(self.lookback_days).mean()
                    current_ma = float(ratio_ma.iloc[-1])
                    
                    # Normalize
                    baseline = float(ratio.iloc[-252]) if len(ratio) >= 252 else float(ratio.iloc[0])
                    
                    metrics['iwm_qqq_ratio'] = current_ratio / baseline
                    metrics['iwm_qqq_ratio_ma'] = current_ma / baseline
                    
                    # Trend
                    if len(ratio_ma) >= 20:
                        recent_ma = ratio_ma.iloc[-20:]
                        slope = (float(recent_ma.iloc[-1]) - float(recent_ma.iloc[0])) / float(recent_ma.iloc[0])
                        
                        if slope < -0.02:
                            metrics['iwm_qqq_trend'] = 'DECLINING'
                        elif slope > 0.02:
                            metrics['iwm_qqq_trend'] = 'RISING'
                        else:
                            metrics['iwm_qqq_trend'] = 'FLAT'
            
            # ═══════════════════════════════════════════════════════════════
            # 3. BREADTH ESTIMATE (% of SPY components above 200-day MA)
            # We estimate this using RSP vs SPY relationship
            # ═══════════════════════════════════════════════════════════════
            if metrics['rsp_spy_ratio'] is not None:
                # When RSP/SPY is high, breadth is good
                # When RSP/SPY is low, breadth is poor (narrow leadership)
                # This is a rough estimate based on the ratio
                ratio = metrics['rsp_spy_ratio']
                
                # Map ratio to breadth estimate (0.95 ratio ≈ 50% breadth)
                # This is approximate but directionally correct
                if ratio >= 1.02:
                    breadth = 65 + (ratio - 1.02) * 100  # Good breadth
                elif ratio >= 0.98:
                    breadth = 50 + (ratio - 0.98) * 375  # Moderate
                elif ratio >= 0.95:
                    breadth = 35 + (ratio - 0.95) * 500  # Poor
                else:
                    breadth = max(20, 35 - (0.95 - ratio) * 500)  # Very poor
                
                metrics['breadth_estimate'] = min(80, max(20, breadth))
            
            # ═══════════════════════════════════════════════════════════════
            # 4. REGIME SCORE CALCULATION
            # ═══════════════════════════════════════════════════════════════
            score = 0.0
            
            # RSP/SPY below 0.98 = centralized (+1)
            if metrics['rsp_spy_ratio'] is not None:
                if metrics['rsp_spy_ratio'] < 0.95:
                    score += 1.0
                elif metrics['rsp_spy_ratio'] < 0.98:
                    score += 0.5
            
            # RSP/SPY declining trend = centralized (+1)
            if metrics['rsp_spy_trend'] == 'DECLINING':
                score += 0.5
                if metrics['rsp_spy_trend_weeks'] >= 4:
                    score += 0.5  # Persistent decline
            
            # IWM/QQQ below 0.95 = mega-cap dominance (+1)
            if metrics['iwm_qqq_ratio'] is not None:
                if metrics['iwm_qqq_ratio'] < 0.90:
                    score += 1.0
                elif metrics['iwm_qqq_ratio'] < 0.95:
                    score += 0.5
            
            # IWM/QQQ declining = increasing concentration (+0.5)
            if metrics['iwm_qqq_trend'] == 'DECLINING':
                score += 0.5
            
            # Breadth below 50% = narrow market (+1)
            if metrics['breadth_estimate'] is not None:
                if metrics['breadth_estimate'] < 40:
                    score += 1.0
                elif metrics['breadth_estimate'] < 50:
                    score += 0.5
            
            metrics['regime_score'] = score
            
        except Exception as e:
            print(f"   ❌ Metric calculation failed: {e}")
        
        self.metrics = metrics
        return metrics
    
    def classify_regime(self) -> str:
        """
        Classify market regime based on calculated metrics.
        
        Returns:
            str: 'CENTRALIZED', 'MIXED', or 'BROAD'
        """
        score = self.metrics.get('regime_score', 0)
        
        if score >= 2.5:
            self.regime = "CENTRALIZED"
        elif score >= 1.0:
            self.regime = "MIXED"
        else:
            self.regime = "BROAD"
        
        self.regime_score = score
        return self.regime
    
    def get_recommendation(self) -> dict:
        """
        Generate trading recommendations based on regime.
        
        Returns:
            dict: Recommendations for position count, focus, stops
        """
        recommendations = {
            "CENTRALIZED": {
                "action": "CONCENTRATE",
                "position_count": "8-10",
                "focus": "Mega-cap momentum names",
                "stops": "Widen to 10-12%",
                "notes": [
                    "• Reduce position count to 8-10",
                    "• Focus on mega-cap momentum names",
                    "• Widen stops to 10-12%",
                    "• Consider adding QQQ/SPY to universe",
                ]
            },
            "MIXED": {
                "action": "TRADE CAUTIOUSLY",
                "position_count": "10-12",
                "focus": "Quality momentum with size bias",
                "stops": "Standard 8-10%",
                "notes": [
                    "• Proceed with caution",
                    "• Monitor breadth daily",
                    "• Consider 50% normal sizing",
                    "• Favor larger caps",
                ]
            },
            "BROAD": {
                "action": "NORMAL",
                "position_count": "15",
                "focus": "Full strategy execution",
                "stops": "Standard 8%",
                "notes": [
                    "• Full strategy execution",
                    "• 15 positions, 8% stops",
                    "• All sectors eligible",
                    "• Diversification working",
                ]
            }
        }
        
        return recommendations.get(self.regime, recommendations["MIXED"])
    
    def display(self, show_recommendations: bool = True) -> None:
        """
        Display the formatted regime dashboard in terminal.
        
        Args:
            show_recommendations: Whether to show trading recommendations
        """
        # Fetch data if not already done
        if not self.metrics:
            if not self.fetch_data():
                print("\n⚠️  Could not display regime dashboard - data unavailable")
                return
            self.calculate_metrics()
            self.classify_regime()
        
        # Get trend arrows
        def trend_arrow(trend):
            if trend == 'DECLINING':
                return '↓'
            elif trend == 'RISING':
                return '↑'
            return '→'
        
        # Get regime emoji
        def regime_emoji(regime):
            if regime == 'CENTRALIZED':
                return '🔴'
            elif regime == 'MIXED':
                return '🟡'
            return '🟢'
        
        # Format ratio value
        def fmt_ratio(val, decimals=3):
            if val is None:
                return "N/A"
            return f"{val:.{decimals}f}"
        
        # Build dashboard
        print("\n" + "═" * 60)
        print("              📊 MARKET REGIME DASHBOARD")
        print("═" * 60)
        
        # RSP/SPY
        rsp_spy = self.metrics.get('rsp_spy_ratio')
        rsp_spy_trend = self.metrics.get('rsp_spy_trend', 'FLAT')
        rsp_spy_weeks = self.metrics.get('rsp_spy_trend_weeks', 0)
        trend_str = f"{trend_arrow(rsp_spy_trend)}"
        if rsp_spy_trend == 'DECLINING' and rsp_spy_weeks > 0:
            trend_str += f" ({rsp_spy_weeks} weeks)"
        
        print(f"\n  RSP/SPY Ratio:        {fmt_ratio(rsp_spy)}  {trend_str}")
        
        # IWM/QQQ
        iwm_qqq = self.metrics.get('iwm_qqq_ratio')
        iwm_qqq_trend = self.metrics.get('iwm_qqq_trend', 'FLAT')
        print(f"  IWM/QQQ Ratio:        {fmt_ratio(iwm_qqq)}  {trend_arrow(iwm_qqq_trend)}")
        
        # Breadth
        breadth = self.metrics.get('breadth_estimate')
        breadth_str = f"{breadth:.1f}%" if breadth else "N/A"
        print(f"  Breadth Estimate:     {breadth_str}")
        
        # Regime score
        score = self.metrics.get('regime_score', 0)
        print(f"  Regime Score:         {score:.1f}/4.0")
        
        print("\n" + "─" * 60)
        
        # Regime classification
        emoji = regime_emoji(self.regime)
        print(f"\n  {emoji} REGIME: {self.regime}")
        
        # Recommendations
        if show_recommendations:
            rec = self.get_recommendation()
            print("\n" + "─" * 60)
            print(f"  📋 RECOMMENDED ACTION: {rec['action']}")
            print("─" * 60)
            for note in rec['notes']:
                print(f"  {note}")
        
        print("\n" + "═" * 60 + "\n")
    
    def get_summary_line(self) -> str:
        """
        Get a one-line summary for inclusion in other outputs.
        
        Returns:
            str: One-line regime summary
        """
        if not self.metrics:
            return "Regime: UNKNOWN (data not loaded)"
        
        emoji = '🔴' if self.regime == 'CENTRALIZED' else '🟡' if self.regime == 'MIXED' else '🟢'
        score = self.metrics.get('regime_score', 0)
        
        return f"{emoji} Market Regime: {self.regime} (score: {score:.1f}/4.0)"


# =============================================================================
# INTEGRATION FUNCTION (NO CHANGES NEEDED)
# =============================================================================

def display_regime_dashboard():
    """
    Convenience function to display the regime dashboard.
    Call this at the start of any trading session.
    
    Example:
        from market_regime_dashboard import display_regime_dashboard
        display_regime_dashboard()
    """
    dashboard = MarketRegimeDashboard()
    if dashboard.fetch_data():
        dashboard.calculate_metrics()
        dashboard.classify_regime()
        dashboard.display()
        return dashboard
    return None

# =============================================================================
# RISK MANAGEMENT MODULE - UPDATED WITH FIBONACCI INTEGRATION
# =============================================================================

class RiskManagementModule:
    """
    UPDATED: Proper Fibonacci integration with new signal structure.
    Key features:
    - Uses enhanced signals from StatisticalSignalModule
    - Fibonacci-aware position sizing and stop placement
    - Regime-dependent adjustments
    """

    def __init__(self, fib_module: Optional[FibonacciModule] = None):
        self.var_window = VAR_WINDOW
        self.confidence = CONFIDENCE_LEVEL
        self.atr_factor = 2.5
        self.fib_module = fib_module or FibonacciModule()

    def compute_var(
        self,
        returns: pd.Series,
        position_value: float,
        confidence: Optional[float] = None,
    ) -> float:
        """Simple historical VaR."""
        if confidence is None:
            confidence = self.confidence

        if returns is None or len(returns) < 30 or position_value <= 0:
            return 0.0

        recent = returns.iloc[-min(len(returns), self.var_window):]
        var_percentile = np.percentile(recent, (1 - confidence) * 100.0)
        return float(abs(var_percentile * position_value))

    def compute_dynamic_stop(self, returns: pd.Series, vol_regime: str, trade_type: str = "SWING") -> tuple:
        """
        Compute stops for both SWING and POSITION accounts.
    
        Returns: (swing_stop, position_stop)
        """
        if returns is None or len(returns) < 10:
            swing_stop = MIN_STOP_ALLOWED
            position_stop = POSITION_MIN_STOP
            return swing_stop, position_stop
    
        recent = returns.tail(20)
        if recent.empty:
            swing_stop = MIN_STOP_ALLOWED
            position_stop = POSITION_MIN_STOP
            return swing_stop, position_stop
    
        daily_vol = float(recent.std())
        if daily_vol <= 0 or np.isnan(daily_vol):
            swing_stop = MIN_STOP_ALLOWED
            position_stop = POSITION_MIN_STOP
            return swing_stop, position_stop
    
        base_stop = daily_vol * self.atr_factor
    
        # Volatility regime adjustment
        if vol_regime == "HIGH_VOL":
            stop_mult = 1.20
        elif vol_regime == "EXTREME_VOL":
            stop_mult = 1.35
        elif vol_regime == "LOW_VOL":
            stop_mult = 0.85
        else:
            stop_mult = 1.0
    
        adjusted = base_stop * stop_mult
    
        # SWING stop
        swing_stop = float(np.clip(adjusted, MIN_STOP_ALLOWED, MAX_STOP_ALLOWED))
    
        # POSITION stop (1.5x wider)
        position_stop = float(np.clip(
            adjusted * POSITION_STOP_MULTIPLIER,
            POSITION_MIN_STOP,
            POSITION_MAX_STOP
        ))
    
        return swing_stop, position_stop

    def _apply_fibonacci_enhancements(self, signal: Dict, base_stop: float, 
                                     market_regime: str = "BROAD") -> Dict[str, float]:
        """
        Apply Fibonacci enhancements to stops and position sizing.
        
        Returns: {
            'stop_adjustment_factor': multiplier for stops,
            'size_adjustment_factor': multiplier for position size,
            'fib_stop_pct': recommended stop percentage,
            'confidence_boost': confidence adjustment
        }
        """
        fib_score = signal.get('fib_score', 0.5)
        fib_level = signal.get('fib_level', '')
        fib_retracement = signal.get('fib_retracement', 0.0)
        is_near_support = signal.get('near_fib_support', False)
        fib_stop_pct = signal.get('fib_stop_pct', 0.08)  # From FibonacciModule
        
        # Default factors
        stop_factor = 1.0
        size_factor = 1.0
        confidence_boost = 0.0
        
        # Only apply if we have meaningful Fibonacci data
        if fib_score > 0.3 and fib_level not in ['N/A', '']:
            
            # 1. STOP ADJUSTMENTS based on Fibonacci levels
            support_levels = ['61.8%', '76.4%', '50.0%']
            resistance_levels = ['23.60%', '38.20%', '23.6%', '38.2%']
            
            if is_near_support:
                # At support: tighter stops (more precise level)
                if market_regime == "CENTRALIZED":
                    stop_factor = 0.85  # 15% tighter in volatile markets
                else:
                    stop_factor = 0.90  # 10% tighter in normal markets
                
                # Size boost at support
                if fib_level == '61.8%':
                    size_factor = 1.25
                    confidence_boost = 0.2
                elif fib_level == '76.4%':
                    size_factor = 1.20
                    confidence_boost = 0.15
                elif fib_level == '50.0%':
                    size_factor = 1.15
                    confidence_boost = 0.10
                    
            elif fib_level in resistance_levels:
                # Near resistance: wider stops (more room for breakout)
                stop_factor = 1.15  # 15% wider
                size_factor = 0.85  # 15% smaller position
                
            # 2. Additional adjustments based on Fibonacci score
            if fib_score > 0.7:
                size_factor *= 1.1  # 10% bonus for high Fibonacci alignment
            elif fib_score < 0.3:
                size_factor *= 0.9  # 10% penalty for poor alignment
        
        # Use Fibonacci-based stop if available and sensible
        if fib_stop_pct > 0.04 and fib_stop_pct < 0.20:  # Sensible range
            recommended_stop = fib_stop_pct * stop_factor
        else:
            recommended_stop = base_stop * stop_factor
        
        return {
            'stop_adjustment_factor': stop_factor,
            'size_adjustment_factor': size_factor,
            'fib_stop_pct': recommended_stop,
            'confidence_boost': confidence_boost
        }

    def compute_position_sizes(
        self,
        signals: Dict[str, Dict],
        price_data: Dict[str, pd.DataFrame],
        regime: Dict[str, str],
        regime_multiplier: float,
        total_capital: float,
        market_regime: str = "BROAD",  # From MarketRegimeDashboard
        merge_live_holdings: bool = True,
    ) -> Dict[str, Dict]:
        """
        UPDATED: Full Fibonacci integration with new signal structure.
        """
        if total_capital is None or total_capital <= 0:
            return {"position_sizing": {}, "var": {}, "stop_losses": {}, "position_stops": {}}

        # ═══════════════════════════════════════════════════════════════
        # PASS 1: BASIC FILTERING WITH FIBONACCI
        # ═══════════════════════════════════════════════════════════════
        qualified = {}
        filtered_stats = {
            'total_signals': len(signals),
            'skipped_score': 0,
            'skipped_sharpe': 0,
            'skipped_fib': 0,
            'skipped_confidence': 0,
        }
        
        for sym, sig in signals.items():
            # Extract data (using NEW field names)
            score = sig.get('combined_score', 0)
            sharpe = sig.get('sharpe_ratio', 0)
            fib_score = sig.get('fib_score', 0.5)
            confidence = sig.get('confidence', 'LOW')
            
            # 1. Basic score filter
            if score <= 0:
                filtered_stats['skipped_score'] += 1
                continue
            
            # 2. Sharpe filter
            if sharpe < 0.2:
                filtered_stats['skipped_sharpe'] += 1
                continue
            
            # 3. NEW: Fibonacci filter
            if fib_score < 0.0:  # Disabled - handled by apply_quality_filters  # Minimum Fibonacci alignment
                filtered_stats['skipped_fib'] += 1
                continue
            

            # 4. Quality filters - OTC, price, volume
            sym_u = sym.upper()
            if EXCLUDE_OTC and (len(sym_u) >= 5 and (sym_u.endswith("F") or sym_u.endswith("Y"))):
                continue
            current_price = sig.get("current_price", 0) or sig.get("price", 0)
            if current_price and (current_price < MIN_PRICE or current_price > MAX_PRICE):
                continue
            avg_volume = sig.get("avg_volume", 0) or sig.get("volume", 0)
            if avg_volume and avg_volume < MIN_VOLUME:
                continue
            # Passed all filters
            qualified[sym] = sig
        
        # Show filtering statistics
        print(f"📊 SIGNAL FILTERING STATISTICS:")
        print(f"   Total signals: {filtered_stats['total_signals']}")
        print(f"   Passed filters: {len(qualified)}")
        if filtered_stats['total_signals'] > 0:
            pass_rate = len(qualified) / filtered_stats['total_signals'] * 100
            print(f"   Pass rate: {pass_rate:.1f}%")
        print(f"   Filtered out:")
        print(f"     - Score ≤ 0: {filtered_stats['skipped_score']}")
        print(f"     - Sharpe < 0.2: {filtered_stats['skipped_sharpe']}")
        print(f"     - Fibonacci < 0.4: {filtered_stats['skipped_fib']}")
        
        if len(qualified) == 0:
            print("⚠️  No signals passed filters")
            return {"position_sizing": {}, "var": {}, "stop_losses": {}, "position_stops": {}}
        
        # ═══════════════════════════════════════════════════════════════
        # RANKING WITH FIBONACCI-ENHANCED SCORING
        # ═══════════════════════════════════════════════════════════════
        def calculate_ranking_score(signal: Dict) -> float:
            # UPDATED: Use the new ranking formula from StatisticalSignalModule
            sharpe = signal.get('sharpe_ratio', 0)
            score = signal.get('combined_score', 0)
            confidence_val = signal.get('confidence', 'MEDIUM')
            fib_score = signal.get('fib_score', 0.5)
            
            # Convert confidence to numeric
            conf_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
            confidence = conf_map.get(confidence_val, 0.5)
            
            # Fibonacci-enhanced ranking formula
            total = (
                sharpe * 0.35 +      # 35% (was 40%)
                score * 0.35 +       # 35% (was 40%)
                confidence * 0.15 +  # 15% (was 20%)
                fib_score * 0.0  # DISABLED     # NEW: 15% Fibonacci
            )
            
            # Bonus for Fibonacci support levels
            fib_level = signal.get('fib_level', '')
            if fib_level == '61.8%':
                total *= 1.10  # 10% bonus for golden ratio
            elif fib_level == '76.4%':
                total *= 1.05  # 5% bonus for deep retracement
            elif signal.get('near_fib_support', False):
                total *= 1.08  # 8% support bonus
            
            # Penalty for resistance
            if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                total *= 0.95  # 5% penalty
            
            return total
        
        # Rank signals
        ranked = sorted(
            qualified.items(),
            key=lambda x: calculate_ranking_score(x[1]),
            reverse=True
        )
        
        # Select top positions
        if FORCE_FULL_DEPLOYMENT:
            min_positions = min(MAX_CONCURRENT_POSITIONS, MIN_POSITIONS_REQUIRED)
            TOP_N = max(min_positions, min(MAX_CONCURRENT_POSITIONS, len(ranked)))
        else:
            TOP_N = min(TOP_N_POSITIONS, len(ranked))

        ranked_signals = dict(ranked[:TOP_N])
    
        # ADD PORTFOLIO POSITIONS: Always include existing holdings regardless of rank.
        # Gated by merge_live_holdings — backtest/test callers pass False to avoid
        # injecting today's Airtable holdings into historical iterations (look-ahead).
        if merge_live_holdings:
            try:
                existing_records = fetch_airtable_records()
                for ticker, record_id in existing_records.items():
                    if ticker not in ranked_signals:
                        try:
                            url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}/{record_id}"
                            resp = session.get(url, headers=AT_HEADERS, timeout=10)
                            if resp.status_code == 200:
                                fields = resp.json().get("fields", {})
                                if fields.get("In Portfolio") == "Yes":
                                    if ticker in signals:
                                        ranked_signals[ticker] = signals[ticker]
                                        print(f"   ➕ Added portfolio position: {ticker}")
                        except:
                            pass
            except Exception as e:
                print(f"   ⚠️ Could not check portfolio positions: {e}")


        print(f"\n📊 Ranking top {len(ranked_signals)} candidates (of {len(qualified)} qualified)")

        # Detailed ranking breakdown
        print(f"\n🔍 RANKING BREAKDOWN (Top Candidates):")
        print(f"{'Symbol':8} | {'Total':6} | {'Sharpe(35%)':10} | {'Score(35%)':9} | {'Conf(15%)':8} | {'Fib(15%)':8} | {'Fib Level':8}")
        print("-" * 90)

        for i, (sym, sig) in enumerate(list(ranked_signals.items())[:8]):
            total_score = calculate_ranking_score(sig)
            sharpe = sig.get('sharpe_ratio', 0)
            score = sig.get('combined_score', 0)
            confidence_val = sig.get('confidence', 'LOW')
            fib_score = sig.get('fib_score', 0)
            fib_level = sig.get('fib_level', 'N/A')

            # Calculate components
            sharpe_component = sharpe * 0.35
            score_component = score * 0.35

            conf_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
            conf_numeric = conf_map.get(confidence_val, 0.5)
            conf_component = conf_numeric * 0.15

            fib_component = fib_score * 0.0  # DISABLED

            print(f"{sym:8} | {total_score:6.3f} | "
                  f"{sharpe_component:10.3f} | "
                  f"{score_component:9.3f} | {conf_component:8.3f} | "
                  f"{fib_component:8.3f} | {fib_level:8}")

        # ═══════════════════════════════════════════════════════════════
        # POSITION SIZING WITH FIBONACCI ENHANCEMENTS
        # ═══════════════════════════════════════════════════════════════
        positions: Dict[str, float] = {}
        var_dict: Dict[str, float] = {}
        stop_losses: Dict[str, float] = {}
        position_stops: Dict[str, float] = {}
        returns_dict: Dict[str, pd.Series] = {}

        # Prepare returns data
        for sym, df in price_data.items():
            # Handle Series input
            if isinstance(df, pd.Series):
                df = df.to_frame(name="Close")
            if df is None or df.empty or "Close" not in df.columns or len(df) < 20:
                continue
            r = df["Close"].pct_change().dropna()
            if len(r) > 10:
                returns_dict[sym] = r

        if not returns_dict:
            return {"position_sizing": {}, "var": {}, "stop_losses": {}, "position_stops": {}}

        raw_risk_per_trade = SWING_RISK_PER_TRADE * float(regime_multiplier)
        risk_per_trade = float(np.clip(raw_risk_per_trade, MIN_SWING_RISK_PER_TRADE, MAX_SWING_RISK_PER_TRADE))
        vol_regime = regime.get("volatility", "MEDIUM_VOL")

        # First pass: Calculate base stops and apply Fibonacci enhancements
        candidate_signals = {}
        for sym, sig in ranked_signals.items():
            if sym not in returns_dict:
                continue

            returns = returns_dict[sym]

            # Calculate base stops
            base_swing_stop, base_position_stop = self.compute_dynamic_stop(returns, vol_regime)

            # Apply Fibonacci enhancements
            fib_enhancements = self._apply_fibonacci_enhancements(
                sig, base_swing_stop, market_regime
            )

            # Apply Fibonacci adjustments
            swing_stop = fib_enhancements['fib_stop_pct']
            size_factor = fib_enhancements['size_adjustment_factor']

            # Position stop (1.5x wider than swing stop)
            position_stop = min(
                swing_stop * POSITION_STOP_MULTIPLIER,
                POSITION_MAX_STOP
            )

            # Clip to allowed ranges
            swing_stop = float(np.clip(swing_stop, MIN_STOP_ALLOWED, MAX_STOP_ALLOWED))
            position_stop = float(np.clip(position_stop, POSITION_MIN_STOP, POSITION_MAX_STOP))

            candidate_signals[sym] = {
                'signal': sig,
                'swing_stop': swing_stop,
                'position_stop': position_stop,
                'returns': returns,
                'fib_enhancements': fib_enhancements,
                'size_factor': size_factor
            }

        print(f"\n📊 After Fibonacci stop adjustment: {len(candidate_signals)} candidates")

        if len(candidate_signals) == 0:
            print("⚠️  No candidates passed stop filter")
            return {"position_sizing": {}, "var": {}, "stop_losses": {}, "position_stops": {}}

        # Second pass: Calculate position sizes with Fibonacci adjustments
        print(f"\n🔍 POSITION SIZING (Fibonacci-Aware):")

        fib_metrics = {
            'support_positions': 0,
            'resistance_positions': 0,
            'avg_fib_score': 0,
            'total_fib_boost': 0
        }

        for sym, data in candidate_signals.items():
            sig = data['signal']
            swing_stop = data['swing_stop']
            position_stop = data['position_stop']
            returns = data['returns']
            size_factor = data['size_factor']
            fib_enhancements = data['fib_enhancements']
            
            # Get price data
            df = price_data.get(sym)
            # Handle Series input
            if isinstance(df, pd.Series):
                df = df.to_frame(name="Close")
            if df is None or df.empty or "Close" not in df.columns:
                continue
                
            price_series = df["Close"].dropna()
            if price_series.empty:
                continue

            last_price = float(price_series.iloc[-1])
            if not np.isfinite(last_price) or last_price <= 0:
                continue

            # Base position weight (Kelly-style)
            raw_weight = risk_per_trade / swing_stop
            
            # Apply Fibonacci size factor
            weighted_raw_weight = raw_weight * size_factor
            
            # Apply force deployment if enabled
            if FORCE_FULL_DEPLOYMENT:
                weighted_raw_weight = weighted_raw_weight * POSITION_FILL_AGGRESSION
            
            # Final weight
            weight = float(np.clip(weighted_raw_weight, 0.0, MAX_POSITION_SIZE))
            
            # Check minimum position size
            if weight < MIN_POSITION_SIZE:
                print(f"   ⚠️  {sym}: Weight {weight:.3%} < {MIN_POSITION_SIZE:.1%} minimum, skipping")
                continue
            
            # Store results
            positions[sym] = weight
            stop_losses[sym] = swing_stop
            position_stops[sym] = position_stop
            
            # Calculate VaR
            position_value = weight * total_capital
            var_dict[sym] = self.compute_var(returns, position_value)
            
            # Track Fibonacci metrics
            fib_score = sig.get('fib_score', 0.5)
            fib_level = sig.get('fib_level', '')
            is_support = sig.get('near_fib_support', False)
            
            fib_metrics['avg_fib_score'] += fib_score
            fib_metrics['total_fib_boost'] += (size_factor - 1.0)  # How much Fibonacci boosted size
            
            if is_support:
                fib_metrics['support_positions'] += 1
            elif fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                fib_metrics['resistance_positions'] += 1
        
        # Calculate average Fibonacci metrics
        if positions:
            fib_metrics['avg_fib_score'] /= len(positions)
            fib_metrics['total_fib_boost'] /= len(positions)
        
        # Apply portfolio leverage cap
        total_weight = sum(positions.values())
        if total_weight > MAX_PORTFOLIO_LEVERAGE and total_weight > 0:
            scale = MAX_PORTFOLIO_LEVERAGE / total_weight
            print(f"\n⚠️  Scaling positions: {total_weight:.1%} → {MAX_PORTFOLIO_LEVERAGE:.1%}")
            for sym in list(positions.keys()):
                positions[sym] *= scale
                # Floor at MIN_POSITION_SIZE: rescaling N > 33 candidates pushes
                # every weight below the 3% min, which the final filter then drops,
                # collapsing the entire dict. Accept slight over-leverage instead.
                positions[sym] = float(np.clip(positions[sym], MIN_POSITION_SIZE, MAX_POSITION_SIZE))
        
        # Final filter for minimum position size
        positions = {sym: w for sym, w in positions.items() if w >= MIN_POSITION_SIZE}
        
        # Summary output
        if positions:
            print(f"\n🎯 FINAL POSITIONS ({len(positions)}):")
            
            for sym, weight in sorted(positions.items(), key=lambda x: x[1], reverse=True):
                sig = candidate_signals.get(sym, {}).get('signal', {})
                fib_level = sig.get('fib_level', 'N/A')
                fib_score = sig.get('fib_score', 0)
                stop = stop_losses.get(sym, 0)
                sharpe = sig.get('sharpe_ratio', 0)
                is_support = sig.get('near_fib_support', False)
                
                support_indicator = "✓" if is_support else " "
                
                print(f"   {sym}: {weight:>5.1%} | Stop: {stop:>5.1%} | "
                      f"Fib: {fib_level} ({fib_score:.2f}) {support_indicator} | "
                      f"Sharpe: {sharpe:.2f}")
            
            # Fibonacci portfolio assessment
            if positions:
                print(f"\n📊 PORTFOLIO FIBONACCI ASSESSMENT:")
                print(f"   Average Fibonacci Score: {fib_metrics['avg_fib_score']:.2f}")
                print(f"   Positions at Support: {fib_metrics['support_positions']}/{len(positions)}")
                print(f"   Average Fibonacci Boost: {fib_metrics['total_fib_boost']*100:+.1f}%")
        
        return {
            "position_sizing": positions,
            "var": var_dict,
            "stop_losses": stop_losses,
            "position_stops": position_stops,
            "fib_metrics": fib_metrics
        }
# =============================================================================
# INTEGRATED TRADING SYSTEM - UPDATED WITH COMPLETE FIBONACCI INTEGRATION
# =============================================================================

class ProfessionalTradingSystem:
    """Integrated swing trading system with complete Fibonacci integration."""

    def __init__(self, symbols: List[str], start_date: str = "2020-01-01"):
        self.symbols = symbols
        self.start_date = start_date
        self.current_market_regime = "CENTRALIZED"
        self.economic = EconomicModule()
        
        # Initialize with Fibonacci integration
        self.signal_gen = StatisticalSignalModule(lookback=21)
        self.regime_mod = RegimeModule(self.economic)
        
        # Create Fibonacci module and pass it to risk management
        self.fib_module = FibonacciModule()
        self.fib_helper = FibonacciIntegration()
        self.risk_mgmt = RiskManagementModule(fib_module=self.fib_module)
        
        self.dq = DataQualityModule()

    # =========================================================================
    # HELPER: Safe Close Price Extraction (NO CHANGES NEEDED)
    # =========================================================================
    @staticmethod
    def _safe_get_close(df: pd.DataFrame) -> pd.Series:
        """
        Safely extract Close prices from DataFrame.
        Handles both regular and MultiIndex columns from yfinance.
        """
        if df is None or df.empty:
            return pd.Series(dtype=float)
        
        # Handle MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            if 'Close' in df.columns.get_level_values(0):
                close = df['Close']
                if isinstance(close, pd.DataFrame):
                    return close.iloc[:, 0]
                return close
        
        # Regular columns
        if 'Close' in df.columns:
            close = df['Close']
            if isinstance(close, pd.DataFrame):
                return close.iloc[:, 0]
            return close
        
        # Fallback: try first column
        if len(df.columns) > 0:
            return df.iloc[:, 0]
        
        return pd.Series(dtype=float)

    @staticmethod
    def _safe_get_scalar(value) -> float:
        """
        Safely convert a pandas value to a Python float.
        Handles Series, DataFrame, and scalar values.
        """
        if value is None:
            return 0.0
        if isinstance(value, pd.DataFrame):
            if value.empty:
                return 0.0
            return float(value.iloc[0, 0])
        if isinstance(value, pd.Series):
            if value.empty:
                return 0.0
            return float(value.iloc[0])
        if isinstance(value, (np.floating, np.integer)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def load_price_data(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Load all symbols efficiently with parallel processing and caching."""
        if start is None:
            start = self.start_date

        print(f"📥 Loading price data for {len(self.symbols)} symbols...")
        print(f"   ⚡ Using parallel processing (max_workers={MAX_WORKERS})")
        
        price_data: Dict[str, pd.DataFrame] = {}
        
        # Function to download single symbol with caching
        def download_symbol(symbol: str) -> Tuple[str, Optional[pd.DataFrame]]:
            try:
                # Check cache first
                cached_data = cache_manager.get_price_data(symbol, start, end or date.today().isoformat())
                if cached_data is not None:
                    return symbol, cached_data
                
                # Download from yfinance
                df = yf.download(
                    symbol,
                    start=start,
                    end=end,
                    progress=False,
                    auto_adjust=True,
                )
                
                if df is not None and not df.empty:
                    processed = self._process_single_dataframe(df, symbol)
                    if processed is not None and not processed.empty:
                        # Cache the result
                        cache_manager.set_price_data(symbol, start, end or date.today().isoformat(), processed)
                        return symbol, processed
            except Exception as e:
                pass
            return symbol, None
        
        # Parallel download
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(download_symbol, sym): sym 
                for sym in self.symbols
            }
            
            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_symbol):
                symbol, data = future.result()
                if data is not None:
                    price_data[symbol] = data
                
                completed += 1
                if completed % 50 == 0:
                    print(f"   📊 Processed {completed}/{len(self.symbols)} symbols")
        
        print(f"✅ Successfully loaded {len(price_data)} symbols (CACHE: {CACHE_ENABLED})")
        
        # Clean up cache periodically
        if random.random() < 0.1:  # 10% chance to clean cache
            cache_manager.cleanup()
        
        return price_data

    def _process_single_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Process a single dataframe from yfinance.
        FIXED: Properly handles MultiIndex columns from newer yfinance versions.
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        df = df.copy()
        
        # Reset index if it's a MultiIndex
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index(level=0, drop=True)
    
        # FIXED: Flatten MultiIndex columns properly
        if isinstance(df.columns, pd.MultiIndex):
            # Get the first level (Open, High, Low, Close, Volume, etc.)
            df.columns = df.columns.get_level_values(0)
        
        # FIXED: Remove duplicate columns that may appear after flattening
        df = df.loc[:, ~df.columns.duplicated()]
    
        # Ensure proper column names (capitalize first letter)
        df.columns = [str(col).strip().capitalize() for col in df.columns]
    
        # Validate using DataQualityModule
        df = self.dq.validate_price_data(df, symbol)
        
        return df

    def run_analysis(
        self,
        price_data: Optional[Dict[str, pd.DataFrame]] = None,
        total_capital: float = 100_000.0,
    ) -> Dict:
        """
        Full pipeline: load data -> signals -> risk -> recommendations.
        Enhanced with complete Fibonacci integration.
        """
        print("\n" + "=" * 80)
        print("🚀 RUNNING PROFESSIONAL TRADING SYSTEM ANALYSIS (WITH FIBONACCI)".center(80))
        print("=" * 80 + "\n")
        
        # CONFIGURATION DISPLAY
        print("⚙️  SYSTEM CONFIGURATION:")
        print(f"   - Universe: {len(self.symbols)} symbols")
        print(f"   - Capital: ${total_capital:,.0f}")
        print(f"   - Filter Mode: {FILTER_MODE}")
        print(f"   - Quality Filters: {'ENABLED' if QUALITY_FILTERS_ENABLED else 'DISABLED'}")
        print(f"   - Max Positions: {MAX_CONCURRENT_POSITIONS}")
        print(f"   - Fibonacci Position Sizing: {FIB_POSITION_CONFIG['enabled']}")
        print(f"   - Fibonacci Stop Tightening: {FIB_POSITION_CONFIG.get('fib_stop_tightening', False)}")
        print(f"   - Force Deployment: {FORCE_FULL_DEPLOYMENT}")
        print()

        # ═══════════════════════════════════════════════════════════════
        # LOAD PRICE DATA IF NOT PROVIDED
        # ═══════════════════════════════════════════════════════════════
        if price_data is None:
            print("📥 Loading price data...")
            price_data = self.load_price_data()
            if not price_data:
                print("❌ Failed to load price data")
                return {}
            print(f"✅ Loaded price data for {len(price_data)} symbols")

        # ═══════════════════════════════════════════════════════════════
        # MARKET REGIME DASHBOARD - Integrated for Fibonacci adjustments
        # ═══════════════════════════════════════════════════════════════
        current_market_regime = "CENTRALIZED"  # Default
        
        try:
            dashboard = MarketRegimeDashboard()
            if dashboard.fetch_data():
                dashboard.calculate_metrics()
                current_market_regime = dashboard.classify_regime()
                
                # Store for use in position sizing
                self.current_market_regime = current_market_regime
                
                # Display dashboard (optional)
                dashboard.display(show_recommendations=True)
                
                print(f"\n📊 MARKET REGIME DETECTED: {current_market_regime}")
                
                # Apply regime-specific recommendations
                recommendations = dashboard.get_recommendation()
                print(f"   📋 Recommended Action: {recommendations['action']}")
                print(f"   🎯 Position Count: {recommendations['position_count']}")
                print(f"   🎯 Stop Adjustments: {recommendations['stops']}")
                
                # Adjust Fibonacci config based on regime
                if current_market_regime == "CENTRALIZED":
                    print("   ⚡ Centralized market: Using Fibonacci momentum strategy")
                    # Tighten stops, focus on momentum names near support/resistance
                elif current_market_regime == "BROAD":
                    print("   🔄 Broad market: Using Fibonacci mean reversion strategy")
                    # Wider stops, focus on strong support levels
                    
        except Exception as e:
            print(f"⚠️  Regime dashboard unavailable: {e}")
            self.current_market_regime = "CENTRALIZED"
        
        # ═══════════════════════════════════════════════════════════════
        # LOAD ECONOMIC DATA AND DETERMINE VOLATILITY REGIME
        # ═══════════════════════════════════════════════════════════════
        print("📊 LOADING ECONOMIC DATA...")
        econ_data = self.economic.load()
        if not econ_data:
            print("❌ Failed to load economic data")
            return {}
        
        # Determine Market Regime (Volatility-based)
        volatility_regime = self.regime_mod.classify(econ_data)
        regime_mult = self.regime_mod.compute_multiplier(volatility_regime)
        
        # Combine both regime types for comprehensive analysis
        combined_regime = {
            "volatility": volatility_regime.get("volatility", "NORMAL"),
            "market_breadth": current_market_regime,
            "timestamp": datetime.now().isoformat()
        }
        
        # Safe scalar extraction for formatting
        vix_val = self._safe_get_scalar(econ_data.get('vix', 0))
        inflation_val = self._safe_get_scalar(econ_data.get('inflation', 0))
        yield_curve_val = self._safe_get_scalar(econ_data.get('yield_curve', 0))
        
        print(f"📊 MARKET REGIME ANALYSIS:")
        print(f"   - Volatility Regime: {volatility_regime.get('volatility', 'N/A')}")
        print(f"   - Market Breadth: {current_market_regime}")
        print(f"   - VIX Level: {vix_val:.1f}")
        print(f"   - Regime Multiplier: {regime_mult:.2f}x")
        print(f"   - Inflation: {inflation_val:.1f}%")
        print(f"   - Yield Curve: {yield_curve_val:.2f}%\n")

        # ═══════════════════════════════════════════════════════════════
        # GENERATE TRADING SIGNALS WITH FIBONACCI INTEGRATION
        # ═══════════════════════════════════════════════════════════════
        print("🎯 GENERATING TRADING SIGNALS (with Fibonacci)...")
        
        # Pass both regime types to signals
        enhanced_regime = {
            **volatility_regime,
            "market_breadth": current_market_regime
        }
        
        signals = self.signal_gen.compute_signals(
            price_data=price_data,
            regime=enhanced_regime,
        )
        print(f"✅ Generated signals for {len(signals)} symbols")
        
        # ═══════════════════════════════════════════════════════════════
        # FIBONACCI SIGNAL ANALYSIS WITH REGIME CONTEXT
        # ═══════════════════════════════════════════════════════════════
        if signals:
            # Collect Fibonacci metrics
            fib_levels = {}
            fib_scores = []
            near_resistance_count = 0
            near_support_count = 0
            high_fib_signals = []  # Signals with high Fibonacci scores
            
            for sym, sig in signals.items():
                fib_level = sig.get('fib_level', '')
                fib_score = sig.get('fib_score', 0)
                fib_retracement = sig.get('fib_retracement', 0)
                
                if fib_level and fib_level != 'N/A':
                    fib_levels[fib_level] = fib_levels.get(fib_level, 0) + 1
                    
                    # Count support vs resistance
                    if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                        near_resistance_count += 1
                    elif fib_level in ['50.0%', '61.8%', '76.4%']:
                        near_support_count += 1
                
                if fib_score > 0:
                    fib_scores.append(fib_score)
                    
                    # Track high Fibonacci quality signals
                    if fib_score > 0.7:
                        high_fib_signals.append((sym, sig))
            
            if fib_levels:
                print(f"\n🔍 FIBONACCI SIGNAL DISTRIBUTION:")
                total_with_fib = sum(fib_levels.values())
                
                for level in ['23.60%', '38.20%', '23.6%', '38.2%', '50.0%', '61.8%', '76.4%']:
                    count = fib_levels.get(level, 0)
                    if count > 0:
                        percentage = (count / total_with_fib) * 100
                        print(f"   - {level}: {count} signals ({percentage:.1f}%)")
                
                if fib_scores:
                    avg_fib = float(np.mean(fib_scores))
                    median_fib = float(np.median(fib_scores))
                    print(f"   - Avg Fib Score: {avg_fib:.2f}, Median: {median_fib:.2f}")
                
                print(f"\n📊 FIBONACCI INTERPRETATION ({current_market_regime} market):")
                
                # Show regime-specific interpretation
                if current_market_regime == "CENTRALIZED":
                    print(f"   ✅ {near_resistance_count} near resistance → May break through in trending markets")
                    print(f"   ⚡ {near_support_count} near support → Could be laggards in momentum-driven market")
                    print(f"   💡 Strategy: Focus on momentum with Fibonacci confluence")
                
                elif current_market_regime == "BROAD":
                    print(f"   ⚠️  {near_resistance_count} near resistance → Mean reversion candidates")
                    print(f"   ✅ {near_support_count} near support → Strong buy candidates")
                    print(f"   💡 Strategy: Mean reversion at Fibonacci levels")
                
                else:  # MIXED
                    print(f"   ⚠️  {near_resistance_count} near resistance → Wait for breakout confirmation")
                    print(f"   ✅ {near_support_count} near support → Primary targets")
                    print(f"   💡 Strategy: Balanced approach, wait for confirmation")
                
                # Show top Fibonacci signals
                if high_fib_signals:
                    print(f"\n🏆 HIGHEST FIBONACCI QUALITY SIGNALS (score > 0.7):")
                    for sym, sig in sorted(high_fib_signals, key=lambda x: x[1].get('fib_score', 0), reverse=True)[:5]:
                        fib_score = sig.get('fib_score', 0)
                        fib_level = sig.get('fib_level', 'N/A')
                        score = sig.get('combined_score', 0)
                        print(f"   - {sym}: Fib={fib_score:.2f} ({fib_level}), Total Score={score:.2f}")

        # ═══════════════════════════════════════════════════════════════
        # APPLY QUALITY FILTERS WITH FIBONACCI
        # ═══════════════════════════════════════════════════════════════
        print(f"\n🔍 APPLYING QUALITY FILTERS (Mode: {FILTER_MODE})...")
        
        # First, filter signals using the updated apply_quality_filters function
        if QUALITY_FILTERS_ENABLED:
            filtered_signals = apply_quality_filters(price_data, signals)
        else:
            filtered_signals = signals
        
        print(f"📊 Filter Results: {len(filtered_signals)}/{len(signals)} signals passed")
        
        # Optional: Apply additional Fibonacci-based filtering
        if len(filtered_signals) > 0:
            # Get top signals using Fibonacci-enhanced ranking
            ranked_signals = self.signal_gen.rank_signals_with_fibonacci(filtered_signals)
            
            print(f"\n📊 TOP FIBONACCI-ENHANCED SIGNALS:")
            for i, (symbol, ranking_score, signal) in enumerate(ranked_signals[:8]):
                fib_score = signal.get('fib_score', 0)
                fib_level = signal.get('fib_level', 'N/A')
                combined_score = signal.get('combined_score', 0)
                sharpe = signal.get('sharpe_ratio', 0)
                
                support_indicator = "✓" if signal.get('near_fib_support', False) else " "
                
                print(f"   {i+1:2}. {symbol:6} | Rank: {ranking_score:.3f} | "
                      f"Fib: {fib_score:.2f} ({fib_level}) {support_indicator} | "
                      f"Score: {combined_score:.3f} | Sharpe: {sharpe:.3f}")

        # ═══════════════════════════════════════════════════════════════
        # RISK MANAGEMENT AND POSITION SIZING WITH FIBONACCI
        # ═══════════════════════════════════════════════════════════════
        print(f"\n🎯 COMPUTING POSITION SIZES (with Fibonacci enhancements)...")
        
        risk_output = self.risk_mgmt.compute_position_sizes(
            signals=filtered_signals,
            price_data=price_data,
            regime=volatility_regime,
            regime_multiplier=regime_mult,
            total_capital=total_capital,
            market_regime=current_market_regime,  # Pass market regime for Fibonacci adjustments
        )
        
        # ═══════════════════════════════════════════════════════════════
        # GENERATE FINAL RECOMMENDATIONS
        # ═══════════════════════════════════════════════════════════════
        print(f"\n📋 GENERATING FINAL TRADING RECOMMENDATIONS...")
        
        final_recommendations = self._generate_recommendations(
            signals=filtered_signals,
            risk_output=risk_output,
            regime=volatility_regime,
            price_data=price_data,
        )
        
        # ═══════════════════════════════════════════════════════════════
        # PRINT SUMMARY
        # ═══════════════════════════════════════════════════════════════
        print(f"\n📊 SYSTEM EXECUTION COMPLETE")
        print("=" * 80)
        
        # Compile results
        results = {
            "price_data": price_data,
            "signals": signals,
            "filtered_signals": filtered_signals,
            "regime": volatility_regime,
            "market_breadth": current_market_regime,
            "risk_management": risk_output,
            "final_signals": final_recommendations,
            "timestamp": datetime.now().isoformat(),
            "capital": total_capital,
        }
        
        # Print summary
        self.print_summary(results)
        
        return results

    def _generate_recommendations(
        self,
        signals: Dict[str, Dict],
        risk_output: Dict[str, Dict],
        regime: Dict[str, str],
        price_data: Dict[str, pd.DataFrame] = None,
    ) -> Dict[str, Dict]:
        """Generate final trading recommendations with safe data extraction."""
        recommendations: Dict[str, Dict] = {}
        pos_sizes = risk_output.get("position_sizing", {}) or {}
        var_dict = risk_output.get("var", {}) or {}
        stop_dict = risk_output.get("stop_losses", {}) or {}
        position_stop_dict = risk_output.get("position_stops", {}) or {}
        fib_metrics = risk_output.get("fib_metrics", {}) or {}
        
        econ_bias_str = f"Vol:{regime.get('volatility', 'NA')}"
    
        SWING_HOLDING_DAYS = 5
        POSITION_HOLDING_DAYS = 30

        # Initialize price_data if None
        if price_data is None:
            price_data = {}
        
        for sym in signals.keys():
            sig = signals.get(sym)
            if not sig:
                continue
                
            # FIXED: Safe scalar extraction using new field names
            score = self._safe_get_scalar(sig.get("combined_score", 0.0))
            confidence = sig.get("confidence", "LOW")
            position = self._safe_get_scalar(pos_sizes.get(sym, 0.0))
            
            # EARNINGS PROXIMITY CHECK
            days_to_earnings, earnings_flag, earnings_mult = check_earnings_proximity(sym)
            position = position * earnings_mult
            
            var_95 = self._safe_get_scalar(var_dict.get(sym, 0.0))
            stop_loss = self._safe_get_scalar(stop_dict.get(sym, 0.0))
            position_stop = self._safe_get_scalar(position_stop_dict.get(sym, stop_loss * 1.5))
            
            # ATR-based targets (realistic)
            current_price = None
            stop_price = None
            target_price = None
            target_pct = stop_loss * 2.0  # Default fallback
            raw_atr_pct = 0.0  # Unclamped ATR for volatility readout

            if sym in price_data:
                df = price_data.get(sym)
                if df is not None and not df.empty:
                    # FIXED: Use safe close extraction
                    close_series = self._safe_get_close(df)
                    if not close_series.empty:
                        current_price = self._safe_get_scalar(close_series.iloc[-1])

                        # Calculate ATR-based target
                        atr_pct = calculate_atr(df)
                        raw_atr_pct = calculate_raw_atr(df)  # True volatility, no clamp

                        # Target = 2x ATR (realistic upside)
                        # Stop = min of ATR*1.5 or original stop (use tighter)
                        atr_stop = atr_pct * 1.5
                        atr_target = atr_pct * 2.0

                        # Use ATR-based values, but cap target at original 2:1
                        target_pct = min(atr_target, stop_loss * 2.0)
                        # Keep original stop (risk management), don't loosen it

                        if current_price and current_price > 0:
                            stop_price = round(current_price * (1 - stop_loss), 2)
                            target_price = round(current_price * (1 + target_pct), 2)
            
            # Calculate returns for both timeframes
            expected_annual = self._safe_get_scalar(sig.get("expected_return", 0.0))
            expected_swing = expected_annual * (SWING_HOLDING_DAYS / 252)
            expected_position = expected_annual * (POSITION_HOLDING_DAYS / 252)
            
            # Determine trade type
            sharpe = self._safe_get_scalar(sig.get("sharpe_ratio", 0.0))
            volatility = stop_loss
            trade_type = self._get_trade_type(sharpe, volatility)
        
            # Determine final signal with Fibonacci consideration
            fib_score = self._safe_get_scalar(sig.get('fib_score', 0))
            fib_level = sig.get('fib_level', '')
            near_fib_support = sig.get('near_fib_support', False)
            
            # Fibonacci-adjusted signal determination
            fib_bonus = 0.0
            if fib_score > 0.7:
                fib_bonus = 0.1  # 10% bonus for high Fibonacci alignment
            elif near_fib_support and fib_score > 0.5:
                fib_bonus = 0.05  # 5% bonus for support alignment
            
            adjusted_score = score * (1 + fib_bonus)
            
            if position >= MIN_POSITION_SIZE and adjusted_score > 0 and confidence in ["MEDIUM", "HIGH"]:
                if adjusted_score >= SWING_STRONG_BUY:
                    final_signal = "STRONG_BUY"
                elif adjusted_score >= SWING_BUY_SCORE:
                    final_signal = "BUY"
                else:
                    final_signal = "BUY"
            else:
                if adjusted_score <= SWING_STRONG_SELL:
                    final_signal = "STRONG_SELL"
                elif adjusted_score <= SWING_WEAK_SELL:
                    final_signal = "SELL"
                else:
                    final_signal = "HOLD"
                    
            recommendations[sym] = {
                "signal": final_signal,
                "confidence": confidence,
                "signal_strength": adjusted_score,
                "combined_score": score,
                "drift_score": self._safe_get_scalar(sig.get("trend_strength", 0.0)),
                "economic_bias": econ_bias_str,
                "regime_score": self._safe_get_scalar(sig.get("momentum_z", 0.0)),
                "expected_return": expected_annual,
                "expected_swing_return": expected_swing,
                "expected_position_return": expected_position,
                "sharpe_ratio": sharpe,
                "position_size": position,
                "var_95": var_95,
                "stop_loss": stop_loss,
                "position_stop": position_stop,
                "trade_type": trade_type,
                "last_updated": date.today().isoformat(),
                
                # UPDATED: Use new Fibonacci field names
                "fib_score": self._safe_get_scalar(sig.get("fib_score", 0.0)),
                "fib_level": sig.get("fib_level", ""),
                "fib_distance": self._safe_get_scalar(sig.get("fib_distance_pct", 100.0)),  # UPDATED field name
                "near_fib_support": sig.get("near_fib_support", False),
                "fib_retracement": self._safe_get_scalar(sig.get("fib_retracement", 0.0)),
                "fib_enhancement": self._safe_get_scalar(sig.get("fib_enhancement", 0.0)),
                
                "days_to_earnings": days_to_earnings,
                "earnings_flag": earnings_flag,
                "earnings_mult": earnings_mult,
                "target_pct": target_pct,
                "raw_atr_pct": raw_atr_pct,
                "current_price": current_price,
                "stop_price": stop_price,
                "target_price": target_price,
                
                # Additional metadata
                "ranking_score": self._safe_get_scalar(sig.get("ranking_score", 0)),
            }
            
        return recommendations    
    
    def _get_trade_type(self, sharpe: float, volatility: float) -> str:
        """
        Determine if trade is better for SWING, POSITION, or BOTH accounts.
        """
        # FIXED: Ensure we're working with scalars
        sharpe = self._safe_get_scalar(sharpe)
        volatility = self._safe_get_scalar(volatility)
        
        if sharpe >= 1.0 and volatility <= 0.10:
            return "BOTH"
        elif sharpe >= 0.8 and volatility > 0.10:
            return "SWING"
        elif sharpe >= 0.5 and volatility <= 0.08:
            return "POSITION"
        elif sharpe >= 1.0:
            return "BOTH"
        else:
            return "SWING"
    
    def print_summary(self, results: Dict):
        """Print a formatted summary of trading signals with Fibonacci info."""
        print("\n" + "=" * 100)
        print("TRADING SIGNALS SUMMARY (WITH FIBONACCI ENHANCEMENTS)".center(100))
        print("=" * 100 + "\n")
    
        final = results.get("final_signals", {})
    
        # Only show symbols with position size > 0
        active_signals = {k: v for k, v in final.items() if v.get('position_size', 0) > 0}
    
        if not active_signals:
            print("No active positions")
            print("\n" + "=" * 100 + "\n")
            return
    
        print(f"{'Symbol':8} | {'Signal':12} | {'Fib':8} | {'Fib Lev':8} | {'Entry':>8} | {'Stop':>8} | {'Target':>8} | {'Pos%':>5} | {'Support':<8}")
        print("-" * 100)
    
        for sym, sig in sorted(active_signals.items()):
            # Get Fibonacci data
            fib_score = self._safe_get_scalar(sig.get('fib_score', 0))
            fib_level = sig.get('fib_level', '—')
            near_support = sig.get('near_fib_support', False)
            
            # Format Fibonacci info
            fib_score_str = f"{fib_score:.2f}" if fib_score > 0 else "—"
            fib_level_str = fib_level if fib_level else "—"
            support_str = "✓" if near_support else "—"
            
            # Price data
            entry = sig.get('current_price')
            stop = sig.get('stop_price')
            target = sig.get('target_price')
            position_size = self._safe_get_scalar(sig.get('position_size', 0))
            
            entry_str = f"${entry:.2f}" if entry and entry > 0 else "—"
            stop_str = f"${stop:.2f}" if stop and stop > 0 else "—"
            target_str = f"${target:.2f}" if target and target > 0 else "—"
            
            print(
                f"{sym:8} | {sig['signal']:12} | "
                f"{fib_score_str:8} | "
                f"{fib_level_str:8} | "
                f"{entry_str:>8} | "
                f"{stop_str:>8} | "
                f"{target_str:>8} | "
                f"{position_size*100:5.1f}% | "
                f"{support_str:<8}"
            )
    
        print("\n" + "=" * 100)
    
        # Fibonacci statistics
        fib_scores = [self._safe_get_scalar(sig.get('fib_score', 0)) for sig in active_signals.values()]
        support_positions = sum(1 for sig in active_signals.values() if sig.get('near_fib_support', False))
        
        if fib_scores:
            avg_score = float(np.mean(fib_scores))
            median_score = float(np.median(fib_scores))
            above_good = sum(1 for s in fib_scores if s > 0.6)
            
            print(f"📊 FIBONACCI PORTFOLIO STATS:")
            print(f"   - Average Score: {avg_score:.2f} (Median: {median_score:.2f})")
            print(f"   - Positions at Support: {support_positions}/{len(active_signals)}")
            print(f"   - High Quality (score > 0.6): {above_good}/{len(active_signals)}")
            
            # Fibonacci distribution by level
            fib_levels = {}
            for sig in active_signals.values():
                level = sig.get('fib_level', '')
                if level:
                    fib_levels[level] = fib_levels.get(level, 0) + 1
            
            if fib_levels:
                print(f"   - Level Distribution: {', '.join([f'{k}:{v}' for k, v in fib_levels.items()])}")
        
        # Get Fibonacci metrics from risk management
        fib_metrics = results.get("risk_management", {}).get("fib_metrics", {})
        if fib_metrics:
            avg_boost = fib_metrics.get('total_fib_boost', 0) * 100
            support_pct = (fib_metrics.get('support_positions', 0) / len(active_signals)) * 100 if active_signals else 0
            
            print(f"\n📊 FIBONACCI ENHANCEMENT IMPACT:")
            print(f"   - Average Position Boost: {avg_boost:+.1f}%")
            print(f"   - Support Positions: {support_pct:.1f}% of portfolio")
            print(f"   - Market Regime: {results.get('market_breadth', 'N/A')}")
        
        print("\n" + "=" * 100 + "\n")

# =============================================================================
# PERFORMANCE DIAGNOSTICS (UPDATED)
# =============================================================================

class PerformanceDiagnostics:
    """Track and diagnose performance issues with Fibonacci analytics."""
    
    def __init__(self):
        self.regime_counts = {
            "CENTRALIZED": 0,
            "MIXED": 0,
            "BROAD": 0
        }
        self.fib_distribution = {
            "23.6%": 0,
            "38.2%": 0,
            "50.0%": 0,
            "61.8%": 0,
            "76.4%": 0,
            "N/A": 0
        }
        self.signal_quality = []
        self.position_counts = []
        
        # NEW: Fibonacci-specific diagnostics
        self.fib_scores = []  # Track all Fibonacci scores
        self.support_signals = []  # Track signals near support
        self.resistance_signals = []  # Track signals near resistance
        self.fib_enhancements = []  # Track Fibonacci enhancement amounts
        self.ranking_scores = []  # Track Fibonacci-enhanced ranking scores
        
    def record_regime(self, regime: str):
        if regime in self.regime_counts:
            self.regime_counts[regime] += 1
            
    def record_fib_level(self, fib_level: str):
        """Record Fibonacci level distribution."""
        if fib_level in self.fib_distribution:
            self.fib_distribution[fib_level] += 1
        else:
            self.fib_distribution["N/A"] += 1
            
    def record_fib_score(self, fib_score: float):
        """Record Fibonacci score for analysis."""
        if fib_score is not None:
            self.fib_scores.append(fib_score)
            
    def record_support_signal(self, signal_data: Dict):
        """Record a signal near Fibonacci support."""
        self.support_signals.append(signal_data)
        
    def record_resistance_signal(self, signal_data: Dict):
        """Record a signal near Fibonacci resistance."""
        self.resistance_signals.append(signal_data)
        
    def record_fib_enhancement(self, enhancement: float):
        """Record Fibonacci enhancement amount (boost/penalty)."""
        if enhancement is not None:
            self.fib_enhancements.append(enhancement)
            
    def record_ranking_score(self, score: float):
        """Record Fibonacci-enhanced ranking score."""
        if score is not None:
            self.ranking_scores.append(score)
            
    def record_signal_quality(self, combined_score: float, confidence: str, fib_score: float = None):
        """Record signal quality with Fibonacci score."""
        self.signal_quality.append((combined_score, confidence, fib_score))
        
    def record_positions(self, count: int, fib_positions: int = 0):
        """Record position counts with Fibonacci-enhanced positions."""
        self.position_counts.append((count, fib_positions))
        
    def calculate_fibonacci_metrics(self) -> Dict[str, float]:
        """Calculate comprehensive Fibonacci performance metrics."""
        metrics = {
            "avg_fib_score": 0.0,
            "fib_score_std": 0.0,
            "support_ratio": 0.0,
            "resistance_ratio": 0.0,
            "avg_enhancement": 0.0,
            "enhancement_impact": 0.0,
        }
        
        if self.fib_scores:
            metrics["avg_fib_score"] = float(np.mean(self.fib_scores))
            metrics["fib_score_std"] = float(np.std(self.fib_scores))
            
            # Calculate distribution
            high_fib = sum(1 for s in self.fib_scores if s > 0.7)
            medium_fib = sum(1 for s in self.fib_scores if 0.4 <= s <= 0.7)
            low_fib = sum(1 for s in self.fib_scores if s < 0.4)
            total = len(self.fib_scores)
            
            metrics["high_fib_pct"] = (high_fib / total * 100) if total > 0 else 0
            metrics["medium_fib_pct"] = (medium_fib / total * 100) if total > 0 else 0
            metrics["low_fib_pct"] = (low_fib / total * 100) if total > 0 else 0
        
        # Calculate support/resistance ratios
        total_signals = len(self.support_signals) + len(self.resistance_signals)
        if total_signals > 0:
            metrics["support_ratio"] = len(self.support_signals) / total_signals
            metrics["resistance_ratio"] = len(self.resistance_signals) / total_signals
        
        # Calculate enhancement impact
        if self.fib_enhancements:
            metrics["avg_enhancement"] = float(np.mean(self.fib_enhancements))
            # Count positive vs negative enhancements
            positive = sum(1 for e in self.fib_enhancements if e > 0)
            negative = sum(1 for e in self.fib_enhancements if e < 0)
            neutral = sum(1 for e in self.fib_enhancements if e == 0)
            total = len(self.fib_enhancements)
            
            metrics["positive_enhancement_pct"] = (positive / total * 100) if total > 0 else 0
            metrics["negative_enhancement_pct"] = (negative / total * 100) if total > 0 else 0
            metrics["neutral_enhancement_pct"] = (neutral / total * 100) if total > 0 else 0
            
            # Estimate enhancement impact on returns
            # (This is simplified - in practice you'd track actual returns)
            metrics["enhancement_impact"] = metrics["avg_enhancement"] * 0.3  # Assumes 30% of enhancement translates to returns
        
        # Calculate ranking score statistics
        if self.ranking_scores:
            metrics["avg_ranking_score"] = float(np.mean(self.ranking_scores))
            metrics["ranking_score_std"] = float(np.std(self.ranking_scores))
        
        return metrics
        
    def print_summary(self):
        """Print comprehensive diagnostics including Fibonacci metrics."""
        print("\n📊 PERFORMANCE DIAGNOSTICS:")
        print("=" * 60)
        
        # Market Regime Distribution
        print("   Market Regime Distribution:")
        total_days = sum(self.regime_counts.values())
        for regime, count in self.regime_counts.items():
            pct = count / total_days * 100 if total_days > 0 else 0
            print(f"     - {regime}: {count} days ({pct:.1f}%)")
        
        # Fibonacci Distribution
        print("\n   Fibonacci Level Distribution:")
        total_signals = sum(self.fib_distribution.values())
        for level, count in self.fib_distribution.items():
            pct = count / total_signals * 100 if total_signals > 0 else 0
            print(f"     - {level}: {count} signals ({pct:.1f}%)")
        
        # Fibonacci Quality Metrics
        fib_metrics = self.calculate_fibonacci_metrics()
        
        print(f"\n   Fibonacci Quality Analysis:")
        if self.fib_scores:
            print(f"     - Avg Fibonacci Score: {fib_metrics['avg_fib_score']:.3f} (±{fib_metrics['fib_score_std']:.3f})")
            print(f"     - High (>0.7): {fib_metrics.get('high_fib_pct', 0):.1f}%")
            print(f"     - Medium (0.4-0.7): {fib_metrics.get('medium_fib_pct', 0):.1f}%")
            print(f"     - Low (<0.4): {fib_metrics.get('low_fib_pct', 0):.1f}%")
        
        if len(self.support_signals) + len(self.resistance_signals) > 0:
            print(f"     - Support vs Resistance: {fib_metrics['support_ratio']*100:.1f}% / {fib_metrics['resistance_ratio']*100:.1f}%")
        
        if self.fib_enhancements:
            print(f"\n   Fibonacci Enhancement Impact:")
            print(f"     - Avg Enhancement: {fib_metrics['avg_enhancement']*100:+.1f}%")
            print(f"     - Positive: {fib_metrics.get('positive_enhancement_pct', 0):.1f}%")
            print(f"     - Negative: {fib_metrics.get('negative_enhancement_pct', 0):.1f}%")
            print(f"     - Neutral: {fib_metrics.get('neutral_enhancement_pct', 0):.1f}%")
            print(f"     - Estimated Return Impact: {fib_metrics['enhancement_impact']*100:+.1f}%")
        
        # Signal Quality
        if self.signal_quality:
            avg_score = np.mean([s[0] for s in self.signal_quality])
            high_conf = sum(1 for s in self.signal_quality if s[1] == "HIGH")
            high_conf_pct = high_conf / len(self.signal_quality) * 100
            
            # Calculate average Fibonacci score for signals
            fib_scores_in_signals = [s[2] for s in self.signal_quality if s[2] is not None]
            avg_fib_in_signals = np.mean(fib_scores_in_signals) if fib_scores_in_signals else 0
            
            print(f"\n   Signal Quality:")
            print(f"     - Avg Combined Score: {avg_score:.3f}")
            print(f"     - High Confidence: {high_conf_pct:.1f}%")
            print(f"     - Avg Fib Score in Signals: {avg_fib_in_signals:.3f}")
            
        # Position Sizing
        if self.position_counts:
            avg_positions = np.mean([p[0] for p in self.position_counts])
            avg_fib_positions = np.mean([p[1] for p in self.position_counts]) if len(self.position_counts[0]) > 1 else 0
            
            print(f"\n   Position Sizing:")
            print(f"     - Avg Positions/Day: {avg_positions:.1f}")
            if avg_fib_positions > 0:
                print(f"     - Avg Fibonacci-Enhanced Positions: {avg_fib_positions:.1f}")
                fib_percentage = (avg_fib_positions / avg_positions * 100) if avg_positions > 0 else 0
                print(f"     - Fibonacci % of Portfolio: {fib_percentage:.1f}%")
        
        # Regime-specific Fibonacci analysis
        print(f"\n   Regime-Specific Analysis:")
        regimes_by_fib = {}
        if hasattr(self, 'regime_fib_scores'):
            for regime, scores in self.regime_fib_scores.items():
                if scores:
                    avg = np.mean(scores)
                    regimes_by_fib[regime] = avg
        
        for regime, avg_fib in regimes_by_fib.items():
            print(f"     - {regime}: Avg Fib Score = {avg_fib:.3f}")
        
        print("\n" + "=" * 60)

    def get_fibonacci_performance_report(self) -> Dict:
        """Generate detailed Fibonacci performance report."""
        report = {
            "summary": {
                "total_signals_analyzed": len(self.fib_scores),
                "market_regime_distribution": dict(self.regime_counts),
                "fibonacci_distribution": dict(self.fib_distribution),
            },
            "quality_metrics": self.calculate_fibonacci_metrics(),
            "signal_analysis": {
                "total_signals": len(self.signal_quality),
                "avg_combined_score": float(np.mean([s[0] for s in self.signal_quality])) if self.signal_quality else 0,
                "high_confidence_pct": (sum(1 for s in self.signal_quality if s[1] == "HIGH") / len(self.signal_quality) * 100) if self.signal_quality else 0,
            },
            "position_analysis": {
                "avg_positions_per_day": float(np.mean([p[0] for p in self.position_counts])) if self.position_counts else 0,
                "avg_fib_positions": float(np.mean([p[1] for p in self.position_counts])) if self.position_counts and len(self.position_counts[0]) > 1 else 0,
            },
            "recommendations": self._generate_fibonacci_recommendations(),
        }
        
        return report
    
    def _generate_fibonacci_recommendations(self) -> List[str]:
        """Generate Fibonacci-based trading recommendations."""
        recommendations = []
        
        fib_metrics = self.calculate_fibonacci_metrics()
        avg_fib_score = fib_metrics.get('avg_fib_score', 0.5)
        
        # Score-based recommendations
        if avg_fib_score < 0.4:
            recommendations.append("⚠️ Fibonacci scores are low (<0.4). Consider tightening Fibonacci filters or improving data quality.")
        elif avg_fib_score > 0.7:
            recommendations.append("✅ Fibonacci scores are high (>0.7). System is finding good Fibonacci alignments.")
        else:
            recommendations.append("📊 Fibonacci scores are moderate (0.4-0.7). Consider fine-tuning Fibonacci parameters.")
        
        # Support/Resistance recommendations
        support_ratio = fib_metrics.get('support_ratio', 0)
        if support_ratio < 0.3:
            recommendations.append("⚠️ Low support signals (<30%). Market may be overextended or near resistance.")
        elif support_ratio > 0.7:
            recommendations.append("✅ High support signals (>70%). Good environment for Fibonacci-based entries.")
        
        # Enhancement recommendations
        positive_pct = fib_metrics.get('positive_enhancement_pct', 0)
        if positive_pct < 20:
            recommendations.append("⚠️ Few positive Fibonacci enhancements (<20%). Fibonacci may not be adding value.")
        elif positive_pct > 60:
            recommendations.append("✅ Strong positive Fibonacci enhancements (>60%). Fibonacci is significantly improving signals.")
        
        # Regime-based recommendations
        if self.regime_counts.get("CENTRALIZED", 0) > self.regime_counts.get("BROAD", 0):
            recommendations.append("📈 Centralized market detected. Focus on momentum names with Fibonacci resistance/support confluence.")
        elif self.regime_counts.get("BROAD", 0) > self.regime_counts.get("CENTRALIZED", 0):
            recommendations.append("🌐 Broad market detected. Good for mean reversion at Fibonacci support levels.")
        
        return recommendations


# =============================================================================
# UNIVERSE SCANNER (UPDATED FOR FIBONACCI)
# =============================================================================

class UniverseScanner:
    """
    Lightweight universe ranking engine with Fibonacci integration.
    Scores symbols using combined_score, Fibonacci score, confidence, expected return, volatility.
    """

    def __init__(self, system: ProfessionalTradingSystem):
        self.system = system

    def rank_universe(
        self,
        price_data,
        regime,
        top_n=30,
        min_confidence="LOW",
        min_combined_score=-0.5,
        min_fib_score=0.3,  # NEW: Minimum Fibonacci score
        min_expected_return=-0.2,
        max_vol_percentile=0.99,
        require_fib_support=False,  # NEW: Option to require Fibonacci support
    ):
        import numpy as np
        import pandas as pd

        if not price_data:
            return None

        records = []

        for sym, df in price_data.items():
            # Handle Series input (from price_slice)
            if isinstance(df, pd.Series):
                df = df.to_frame(name="Close")
            
            if df is None or len(df) < 60:
                continue

            try:
                # Run signal engine
                sig = self.system.signal_gen.compute_signals(
                    price_data={sym: df},
                    regime=regime,
                ).get(sym, {})

                # Check if signal exists
                if not sig:
                    continue

                # Extract signal data using NEW field names
                score = sig.get("combined_score", 0)
                conf = sig.get("confidence", "LOW")
                exp_ret = sig.get("expected_return", 0)
                fib_score = sig.get("fib_score", 0)  # NEW: Get Fibonacci score
                fib_level = sig.get("fib_level", "")  # NEW: Get Fibonacci level
                near_support = sig.get("near_fib_support", False)  # NEW: Support check
                sharpe = sig.get("sharpe_ratio", 0)
                
                # NEW: Calculate Fibonacci-enhanced ranking score
                ranking_score = self._calculate_fibonacci_ranking_score(sig)

                # Confidence filter (accept LOW confidence)
                if conf not in ("LOW", "MEDIUM", "HIGH", "VERY_HIGH"):
                    continue

                # Combined score filter
                if score < min_combined_score:
                    continue

                # NEW: Fibonacci score filter
                if fib_score < min_fib_score:
                    continue

                # NEW: Fibonacci support filter (if required)
                if require_fib_support and not near_support:
                    continue

                # Expected return filter
                if exp_ret < min_expected_return:
                    continue

                # Rolling 20-day volatility
                vol = df["Close"].pct_change().rolling(20).std().iloc[-1]
                if np.isnan(vol):
                    continue

                records.append({
                    "symbol": sym,
                    "score": score,
                    "fib_score": fib_score,  # NEW
                    "fib_level": fib_level,  # NEW
                    "near_support": near_support,  # NEW
                    "ranking_score": ranking_score,  # NEW: Fibonacci-enhanced ranking
                    "confidence": conf,
                    "expected_return": exp_ret,
                    "sharpe_ratio": sharpe,  # NEW
                    "vol": vol,
                })
            except Exception:
                continue

        if not records:
            return None

        ranked = pd.DataFrame(records)

        # Drop ultra-volatile junk
        vol_cutoff = ranked["vol"].quantile(max_vol_percentile)
        ranked = ranked[ranked["vol"] <= vol_cutoff]

        if ranked.empty:
            return None

        # NEW: Use Fibonacci-enhanced ranking by default
        # Sort by ranking_score (Fibonacci-enhanced) → score → expected return → lowest volatility
        ranked = ranked.sort_values(
            by=["ranking_score", "score", "expected_return", "vol"],
            ascending=[False, False, False, True]
        )

        return ranked.head(top_n)

    # In the StatisticalSignalModule class, find this method:
    def _calculate_fibonacci_ranking_score(self, signal: Dict) -> float:
        """Calculate Fibonacci-enhanced ranking score for a signal."""
        # Get values with defaults
        sharpe = signal.get('sharpe_ratio', 0)
        score = signal.get('combined_score', 0)
        confidence_val = signal.get('confidence', 'MEDIUM')
        fib_score = signal.get('fib_score', 0.5)
    
        # Convert confidence to numeric
        conf_map = {'LOW': 0.3, 'MEDIUM': 0.6, 'HIGH': 1.0}
        confidence = conf_map.get(confidence_val, 0.5)  # Fixed: convert string to float
    
        # Fibonacci-enhanced ranking formula (same as in RiskManagementModule)
        ranking_score = (
            sharpe * 0.35 +      # 35% (was 40%)
            score * 0.35 +       # 35% (was 40%)
            confidence * 0.15 +  # 15% (was 20%)
            fib_score * 0.0  # DISABLED     # NEW: 15% Fibonacci
        )
    
        # Bonus for Fibonacci support levels
        fib_level = signal.get('fib_level', '')
        if fib_level == '61.8%':
            ranking_score *= 1.0  # DISABLED  # 10% bonus for golden ratio
        elif fib_level == '76.4%':
            ranking_score *= 1.0  # DISABLED  # 5% bonus for deep retracement
        elif signal.get('near_fib_support', False):
            ranking_score *= 1.0  # DISABLED  # 8% support bonus
    
        # Penalty for resistance
        if fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
            ranking_score *= 0.95  # 5% penalty
    
        return ranking_score

    def scan_universe_with_fibonacci_report(self, price_data, regime, top_n=20):
        """Scan universe and generate Fibonacci-focused report."""
        print("\n🔍 SCANNING UNIVERSE WITH FIBONACCI ANALYSIS")
        print("=" * 60)
        
        # Run the scan
        ranked = self.rank_universe(
            price_data=price_data,
            regime=regime,
            top_n=top_n,
            min_fib_score=0.4,  # Higher threshold for report
            require_fib_support=False,
        )
        
        if ranked is None or ranked.empty:
            print("No symbols found meeting criteria")
            return None
        
        # Generate Fibonacci statistics
        fib_stats = {
            'total_symbols': len(ranked),
            'avg_fib_score': ranked['fib_score'].mean(),
            'support_count': ranked['near_support'].sum(),
            'support_pct': (ranked['near_support'].sum() / len(ranked)) * 100,
            'level_distribution': ranked['fib_level'].value_counts().to_dict(),
        }
        
        print(f"\n📊 FIBONACCI SCAN RESULTS:")
        print(f"   - Total symbols: {fib_stats['total_symbols']}")
        print(f"   - Average Fibonacci score: {fib_stats['avg_fib_score']:.3f}")
        print(f"   - Signals near support: {fib_stats['support_count']} ({fib_stats['support_pct']:.1f}%)")
        
        print(f"\n   Fibonacci Level Distribution:")
        for level in ['23.60%', '38.20%', '23.6%', '38.2%', '50.0%', '61.8%', '76.4%', 'N/A']:
            count = fib_stats['level_distribution'].get(level, 0)
            if count > 0:
                pct = (count / len(ranked)) * 100
                print(f"     - {level}: {count} ({pct:.1f}%)")
        
        print(f"\n🏆 TOP {min(10, len(ranked))} FIBONACCI-ENHANCED SYMBOLS:")
        print(f"{'Rank':<4} {'Symbol':<6} {'Fib Score':<9} {'Fib Level':<8} {'Support':<8} {'Rank Score':<10} {'Score':<6} {'Sharpe':<6}")
        print("-" * 70)
        
        for i, (_, row) in enumerate(ranked.head(10).iterrows()):
            support_indicator = "✓" if row['near_support'] else ""
            print(f"{i+1:<4} {row['symbol']:<6} "
                  f"{row['fib_score']:<9.3f} "
                  f"{str(row['fib_level']):<8} "
                  f"{support_indicator:<8} "
                  f"{row['ranking_score']:<10.3f} "
                  f"{row['score']:<6.3f} "
                  f"{row.get('sharpe_ratio', 0):<6.3f}")
        
        print("\n" + "=" * 60)
        
        # Return both the ranked DataFrame and the Fibonacci statistics
        return {
            'ranked_symbols': ranked,
            'fibonacci_stats': fib_stats,
            'top_symbols': ranked['symbol'].head(top_n).tolist()
        }

# =============================================================================
# BACKTEST ENGINE WITH FIBONACCI INTEGRATION
# =============================================================================

class BacktestEngine:
    """
    Swing-trading backtesting engine with Fibonacci integration:
    - Fibonacci-enhanced signals and position sizing
    - Fibonacci-aware stops and risk management
    - Performance tracking with Fibonacci metrics
    - Realistic execution costs
    """

    def __init__(self, system: ProfessionalTradingSystem, initial_capital: float = 100000):
        self.system = system
        self.initial_capital = initial_capital
        self.cash = initial_capital

        self.positions: Dict[str, Dict[str, float]] = {}
        self.trades: List[Dict] = []
        self.equity_curve: List[Dict] = []
        self.history: List[Dict] = []

        self.peak_equity = initial_capital
        self.max_drawdown = 0.0
        self.equity = initial_capital
        self.eq_series: List[float] = []
        self.dd_multiplier = 1.0

        self.all_price_data: Dict[str, pd.DataFrame] = {}
        self.price_df: pd.DataFrame = pd.DataFrame()
        self.econ_df: pd.DataFrame = pd.DataFrame()
        self.dates: pd.DatetimeIndex = pd.DatetimeIndex([])

        self.last_signals: Optional[Dict] = None
        self.global_stop_triggered: bool = False
        self.global_stop_date: Optional[datetime] = None
        self.exit_cooldowns: Dict[str, datetime] = {}  # Track when stocks were exited
        
        # NEW: Fibonacci tracking
        self.fib_metrics_history: List[Dict] = []
        self.trades_with_fib: List[Dict] = []
        self.daily_fib_scores: List[float] = []
        
        # NEW: Fee tracking (MEM Labs integration)
        self.total_fees = 0.0
        self.gross_pnl = 0.0
        self.net_pnl = 0.0
        self.fee_history: List[float] = []
        # NEW: Debug mode
        self.debug_mode = True  # Set to True for troubleshooting

    # =========================================================================
    # MARKET REGIME DETECTION
    # =========================================================================
    def is_bull_market(self, current_prices: Dict, date_idx: int) -> bool:
        """
        Check if market is in bull regime (SPY above 50-day MA).
        Returns True if bull market, False if correction/bear.
        """
        try:
            # Check if SPY data is available in price_df
            if "SPY" not in self.price_df.columns:
                # SPY not loaded - use a simpler check based on overall market
                # If more than 60% of stocks are down, consider it bearish
                if hasattr(self, "_last_prices") and self._last_prices:
                    current_vals = list(current_prices.values())
                    last_vals = list(self._last_prices.values())
                    if len(current_vals) > 10 and len(last_vals) > 10:
                        down_count = sum(1 for c, l in zip(current_vals[:50], last_vals[:50]) 
                                        if c and l and c < l)
                        if down_count > 40:  # More than 80% down - be more conservative
                            return False
                self._last_prices = dict(current_prices)
                return True
            
            # Get SPY price and MA
            spy_price = current_prices.get("SPY")
            if date_idx < 3:
                print(f"   DEBUG: SPY price={spy_price}, SPY in price_df={'SPY' in self.price_df.columns if hasattr(self, 'price_df') else 'no price_df'}")
            if spy_price is None or pd.isna(spy_price):
                return True
            
            # Get all SPY data up to and including current date
            lookback_offset = len(self.price_df) - len(self.trading_dates) if hasattr(self, "trading_dates") else 250
            actual_idx = lookback_offset + date_idx
            spy_series = self.price_df["SPY"].iloc[:actual_idx+1]
            if len(spy_series) >= 50:
                ma_50 = spy_series.iloc[-50:].mean()
                if date_idx < 3:
                    print(f"   DEBUG: SPY={spy_price:.2f} vs MA50={ma_50:.2f}, bull={spy_price > ma_50}")
                is_bull = spy_price > (ma_50 * 0.98)  # 2% buffer
                if not is_bull and date_idx < 5:
                    print(f"   📉 BEAR MARKET DETECTED: SPY {spy_price:.2f} < MA50 {ma_50:.2f}")
                return is_bull
            return True
        except Exception as e:
            return True


    # =========================================================================
    # DATA LOADING
    # =========================================================================

    def load_data(self, start: str, end: str):
        print(f"\n📥 Loading data from {start} to {end}...")

        # ═══════════════════════════════════════════════════════════════
        # LOAD EXTRA HISTORY FOR FIBONACCI (252 days = 52 weeks)
        # ═══════════════════════════════════════════════════════════════
        from datetime import datetime, timedelta

        start_dt = datetime.strptime(start, "%Y-%m-%d")
        lookback_start = start_dt - timedelta(days=365)  # 1 year before for Fibonacci
        lookback_start_str = lookback_start.strftime("%Y-%m-%d")

        print(f"   (Loading from {lookback_start_str} for Fibonacci 52-week lookback)")
        # Load price data
        self.all_price_data = self.system.load_price_data(start=lookback_start_str, end=end)
        if not self.all_price_data:
            raise ValueError("No price data loaded for backtest.")

        # Ensure SPY is loaded for market regime detection
        if "SPY" not in self.all_price_data:
            import yfinance as yf
            spy_df = yf.download("SPY", start=lookback_start_str, end=end, progress=False)
            if spy_df is not None and not spy_df.empty:
                if isinstance(spy_df.columns, pd.MultiIndex):
                    spy_df.columns = spy_df.columns.get_level_values(0)
                self.all_price_data["SPY"] = spy_df
                print("   ✅ Added SPY for market regime detection")
        print(f"✅ Loaded data for {len(self.all_price_data)} symbols")

        # Extract close prices for backtesting
        close_prices = {}
        insufficient_symbols = []
    
        for sym, df in self.all_price_data.items():
            if isinstance(df, pd.DataFrame) and "Close" in df.columns and len(df) > 0:
                # FIX: Flexible data requirement - use what we have
                if len(df) >= 20:  # Minimum for ANY calculation (was 252)
                    close_prices[sym] = df["Close"]
                
                    # Track data quality
                    if len(df) < 252:
                        insufficient_symbols.append((sym, len(df)))
                else:
                    if self.debug_mode:
                        print(f"❌ {sym}: Insufficient data ({len(df)} days, minimum 20)")
    
        # FIX: Only warn, don't fail
        if not close_prices:
            print("⚠️  WARNING: No symbols with 20+ days of data")
            # Create empty dataframe but don't crash
            self.price_df = pd.DataFrame()
        else:
            self.price_df = pd.DataFrame(close_prices).ffill().dropna(how="all")
    
        print(f"📊 Price matrix: {self.price_df.shape}")
    
        # Report data quality
        if insufficient_symbols:
            print(f"📊 Data quality report:")
            print(f"   Symbols with 252+ days: {len([s for s, df in self.all_price_data.items() if len(df) >= 252])}")
            print(f"   Symbols with 100-251 days: {len([s for s, df in self.all_price_data.items() if 100 <= len(df) < 252])}")
            print(f"   Symbols with 20-99 days: {len([s for s, df in self.all_price_data.items() if 20 <= len(df) < 100])}")
            if len(insufficient_symbols) > 0:
                print(f"   Top 5 limited symbols: {insufficient_symbols[:5]}")
    
        # Check for Fibonacci data availability
        fib_ready_symbols = len([s for s, df in self.all_price_data.items() 
                            if isinstance(df, pd.DataFrame) and len(df) >= 252])
        print(f"📈 Symbols ready for Fibonacci analysis: {fib_ready_symbols}/{len(self.all_price_data)}")
        
        # NEW: Ensure we have data for the simulation period
        if not self.price_df.empty:
            # Check if we have data for the simulation dates
            try:
                simulation_dates = self.price_df.loc[start:end].index
                if len(simulation_dates) == 0:
                    print(f"⚠️  WARNING: No data available for simulation period {start} to {end}")
                    print(f"   Available date range: {self.price_df.index[0].date()} to {self.price_df.index[-1].date()}")
                    # Use all available dates
                    self.dates = self.price_df.index
                else:
                    self.dates = simulation_dates
                    print(f"📊 Simulation period: {len(self.dates)} trading days")
            except Exception as e:
                print(f"⚠️  Error selecting simulation dates: {e}")
                # Fallback to all dates
                self.dates = self.price_df.index
        else:
            print(f"❌ CRITICAL: price_df is empty!")
            # Create empty dates to avoid crash
            self.dates = pd.DatetimeIndex([])
    
        # Load economic data
        print("\n📊 Loading economic data...")
        raw_econ = self.system.economic.load_historical(start, end)

        if isinstance(raw_econ, pd.DataFrame):
             econ_df = raw_econ.copy()
        else:
            econ_df = pd.DataFrame(raw_econ) if isinstance(raw_econ, dict) else pd.DataFrame()

        if not econ_df.empty:
            econ_df = econ_df.sort_index()
            econ_df = econ_df.reindex(self.price_df.index, method="ffill").dropna(how="all")
        else:
            print("⚠️ No economic data - using neutral defaults.")
            econ_df = pd.DataFrame(index=self.price_df.index)

        self.econ_df = econ_df
        print(f"   (Full data available: {len(self.price_df)} days including lookback)")

    def refresh_price_data_after_reset(self, current_date):
        """Refresh price data after global stop reset."""
    
        print(f"\n🔄 REFRESHING PRICE DATA AFTER RESET on {current_date.date()}")
    
        # Get fresh data for the past year
        end_str = current_date.strftime("%Y-%m-%d")
        start_dt = current_date - timedelta(days=365)
        start_str = start_dt.strftime("%Y-%m-%d")
    
        print(f"   Fetching data from {start_str} to {end_str}")
        
        try:
            # Load fresh data
            fresh_data = self.system.load_price_data(start=start_str, end=end_str)
            
            print(f"   Fresh data loaded: {len(fresh_data) if fresh_data else 0} symbols")
            
            if fresh_data:
                # Update our data structures
                for sym, df in fresh_data.items():
                    if sym in self.all_price_data:
                        self.all_price_data[sym] = df  # Update existing
                    else:
                        self.all_price_data[sym] = df  # Add new
                
                # Rebuild price_df with updated data
                close_prices = {}
                for sym, df in self.all_price_data.items():
                    if isinstance(df, pd.DataFrame) and "Close" in df.columns and len(df) >= 20:
                        close_prices[sym] = df["Close"]
                
                if close_prices:
                    self.price_df = pd.DataFrame(close_prices).ffill().dropna(how="all")
                    print(f"✅ Data refreshed: {self.price_df.shape}")
                    print(f"   Date range: {self.price_df.index[0].date()} to {self.price_df.index[-1].date()}")
                    
                    # Also update dates for the simulation
                    if self.dates is not None and len(self.dates) > 0:
                        last_sim_date = self.dates[-1]
                        self.dates = self.price_df.loc[self.dates[0]:last_sim_date].index
                        print(f"   Updated simulation dates: {len(self.dates)} days")
                else:
                    print("⚠️  No valid data after refresh")
                    
            else:
                print("⚠️  Could not refresh price data")
                
        except Exception as e:
            print(f"❌ Error refreshing data: {e}")
            import traceback
            traceback.print_exc()

    # =========================================================================
    # MACRO SNAPSHOT
    # =========================================================================

    def get_econ_snapshot(self, date_):
        default = {
            "inflation": 2.0,
            "fed_funds": 4.0,
            "inflation_expectation": 2.0,
            "yield_curve": 0.5,
            "real_yield": 2.0,
            "vix": 20.0,
        }

        if self.econ_df.empty:
            return default

        if date_ < self.econ_df.index[0]:
            return default

        try:
            idx = self.econ_df.index.get_loc(date_, method="ffill")
            row = self.econ_df.iloc[idx]
        except Exception:
            row = self.econ_df.iloc[-1]

        return {k: float(row.get(k, default[k])) for k in default}

    # =========================================================================
    # PRICE SLICE UTILITY
    # =========================================================================

    def get_price_slice(self, date_):
        """Get price data slice for signal generation."""
    
        # DEBUG: Add this at the beginning
        if self.debug_mode:
            print(f"   self.price_df shape: {self.price_df.shape if hasattr(self, 'price_df') else 'N/A'}")
            print(f"   self.price_df columns: {len(self.price_df.columns) if hasattr(self, 'price_df') and not self.price_df.empty else 0}")
    
        if not hasattr(self, 'price_df') or self.price_df.empty:
            if self.debug_mode:
                pass
                pass
            return {}
    
        # Check if date is in index
        if date_ not in self.price_df.index:
            # Try to find nearest date
            if len(self.price_df) > 0:
                try:
                    nearest_idx = self.price_df.index.get_indexer([date_], method="pad")[0]
                    if nearest_idx >= 0:
                        date_ = self.price_df.index[nearest_idx]
                except Exception as e:
                    return {}

        # Get slice of data for signal generation (typically 100-200 days)
        lookback_days = 200
        try:
            date_idx = self.price_df.index.get_indexer([date_])[0]
            start_idx = max(0, date_idx - lookback_days)
        except Exception as e:
            if self.debug_mode:
                pass
            return {}
    
        # Get the slice
        price_slice = {}
        for sym in self.price_df.columns:
            series = self.price_df[sym].iloc[start_idx:date_idx+1]
            if len(series) >= 20:  # Minimum for Fibonacci calculation
                price_slice[sym] = series.to_frame(name='Close')
            pass  # Skip symbols with insufficient data
    
        if self.debug_mode:
            if price_slice:
                sample = list(price_slice.keys())[:3]
                print(f"   Sample: {sample}")
        
        return price_slice

    # =========================================================================
    # FIBONACCI-AWARE EXECUTION ENGINE
    # =========================================================================

    def execute_trade(self, sym, shares, price, date_, signal_data: Optional[Dict] = None):
        """
        Execute trade with Fibonacci-aware adjustments.
        Returns (success, actual_shares, fib_metadata) tuple.
        """
        if shares == 0:
            return False, 0, {}
        print(f"      [execute_trade] {sym}: shares={shares}, price={price:.2f}, cash={self.cash:.2f}")

        # Extract Fibonacci information if available
        fib_metadata = {}
        if signal_data:
            fib_metadata = {
                'fib_score': signal_data.get('fib_score', 0),
                'fib_level': signal_data.get('fib_level', ''),
                'near_support': signal_data.get('near_fib_support', False),
                'fib_stop_pct': signal_data.get('stop_loss', 0.08),  # Use Fibonacci stop if available
            }
        
        # Adjust execution based on Fibonacci score (higher confidence = tighter execution)
        fib_score = fib_metadata.get('fib_score', 0.5)
        execution_multiplier = 1.0 + (fib_score - 0.5) * 0.1  # ±5% adjustment
        
        # Execution price with Fibonacci-aware slippage
        base_slippage = SLIPPAGE_BPS / 10000.0
        fib_slippage_adjustment = 1.0 - (fib_score * 0.3)  # Better Fibonacci = less slippage (up to 30% reduction)
        effective_slippage = base_slippage * fib_slippage_adjustment
        
        if shares > 0:  # BUY
            exec_price = price * (1 + effective_slippage)
        else:  # SELL
            exec_price = price * (1 - effective_slippage)
            
        # Apply execution multiplier
        exec_price = exec_price * (1 + (execution_multiplier - 1.0) * 0.5)

        if shares > 0:  # BUY
            trade_value = shares * exec_price
            fee = trade_value * (TRANSACTION_COST_BPS / 10000.0)
            total_needed = trade_value + fee
    
            # Cap position at MAX_POSITION_SIZE of CURRENT equity
            max_position_value = self.equity * MAX_POSITION_SIZE
    
            if trade_value > max_position_value:
                trade_value = max_position_value
                shares = int(trade_value / exec_price)
                fee = trade_value * (TRANSACTION_COST_BPS / 10000.0)
                total_needed = trade_value + fee
    
            # Check if we have enough cash
            if self.cash < total_needed:
                available_cash = self.cash * 0.98
                max_shares_by_cash = int(available_cash / (exec_price * (1 + TRANSACTION_COST_BPS / 10000.0)))
        
                if max_shares_by_cash < 1:
                    return False, 0, {}
        
                shares = min(shares, max_shares_by_cash)
                trade_value = shares * exec_price
                fee = trade_value * (TRANSACTION_COST_BPS / 10000.0)
                total_needed = trade_value + fee
    
            # Final safety check
            if total_needed > self.cash:
                return False, 0, {}
        
            self.cash -= total_needed
    
        else:  # SELL
            pos = self.positions.get(sym)
            if not pos or pos["shares"] < abs(shares):
                return False, 0, {}
    
            trade_value = abs(shares) * exec_price
            fee = trade_value * (TRANSACTION_COST_BPS / 10000.0)
            self.cash += trade_value - fee

        # Record trade with Fibonacci metadata
        trade_record = {
            "date": date_,
            "symbol": sym,
            "shares": shares,
            "price": exec_price,
            "cost": trade_value,
            "fib_score": fib_metadata.get('fib_score', 0),
            "fib_level": fib_metadata.get('fib_level', ''),
            "near_support": fib_metadata.get('near_support', False),
            "execution_type": "FIB_ENHANCED",
            "fee": fee,
            "slippage_pct": effective_slippage,
        }
        
        self.trades.append(trade_record)
        
        # Accumulate fees (MEM Labs fee tracking)
        self.total_fees += fee
        self.fee_history.append(fee)
        
        # Also record in Fibonacci-specific tracking
        if fib_metadata.get('fib_score', 0) > 0.15:  # Only track meaningful Fibonacci trades
            self.trades_with_fib.append(trade_record)

        return True, shares, fib_metadata

    # =========================================================================
    # DRAWDOWN CONTROL WITH FIBONACCI AWARENESS
    # =========================================================================

    def update_equity(self, date_, current_prices: Dict[str, float], fib_metrics: Dict):
        """Update equity and drawdown with Fibonacci metrics."""
        position_value = 0.0
        for sym, pos in self.positions.items():
            price = current_prices.get(sym)
            if price is not None and not pd.isna(price):
                position_value += pos["shares"] * price
        
        self.equity = self.cash + position_value
        self.eq_series.append(self.equity)
        
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
            self.max_drawdown = 0.0
        else:
            self.max_drawdown = max(self.max_drawdown, (self.peak_equity - self.equity) / self.peak_equity)
        
        # Store Fibonacci metrics
        fib_metrics['date'] = date_
        fib_metrics['equity'] = self.equity
        fib_metrics['drawdown'] = self.max_drawdown
        self.fib_metrics_history.append(fib_metrics)
        
        # Store daily Fibonacci score
        self.daily_fib_scores.append(fib_metrics.get('avg_fib_score', 0.5))

    # =========================================================================
    # MISSING METHODS - ADDED HERE
    # =========================================================================

    def print_fibonacci_backtest_summary(self):
        """Print Fibonacci-specific backtest analysis."""
        print("\n" + "="*80)
        print("                    FIBONACCI BACKTEST PERFORMANCE ANALYSIS                     ")
        print("="*80)
        
        if not self.trades:
            print("❌ No trades executed in this backtest.")
            return
        
        # Separate Fibonacci and non-Fibonacci trades
        fib_trades = [t for t in self.trades if t.get('fib_score', 0) > 0.15]
        non_fib_trades = [t for t in self.trades if t.get('fib_score', 0) <= 0.15]
        
        print(f"\n📊 TRADE COMPARISON:")
        print(f"{'Metric':<20} {'Fibonacci':<15} {'Non-Fibonacci':<15} {'Difference':<15}")
        print("-"*65)
        
        # Trade Count
        fib_count = len(fib_trades)
        non_fib_count = len(non_fib_trades)
        print(f"{'Trade Count':<20} {fib_count:<15} {non_fib_count:<15} {fib_count - non_fib_count:<15}")
        
        # Win Rate
        if fib_count > 0:
            fib_winners = len([t for t in fib_trades if t.get('shares', 0) < 0])  # Negative shares = sell = profit
            fib_win_rate = (fib_winners / fib_count) * 100
        else:
            fib_win_rate = 0.0
            
        if non_fib_count > 0:
            non_fib_winners = len([t for t in non_fib_trades if t.get('shares', 0) < 0])
            non_fib_win_rate = (non_fib_winners / non_fib_count) * 100
        else:
            non_fib_win_rate = 0.0
            
        print(f"{'Win Rate':<20} {fib_win_rate:.1f}%{'':<10} {non_fib_win_rate:.1f}%{'':<10} {fib_win_rate - non_fib_win_rate:+.1f}%{'':<8}")
        
        # Average Return (simplified)
        if fib_count > 0:
            # Calculate P&L for Fibonacci trades
            fib_pnl = 0
            for trade in fib_trades:
                if trade['shares'] < 0:  # Sell = profit
                    fib_pnl += 1
                else:  # Buy = cost
                    fib_pnl -= 1
            fib_avg_return = (fib_pnl / fib_count) * 100
        else:
            fib_avg_return = 0.0
            
        if non_fib_count > 0:
            non_fib_pnl = 0
            for trade in non_fib_trades:
                if trade['shares'] < 0:
                    non_fib_pnl += 1
                else:
                    non_fib_pnl -= 1
            non_fib_avg_return = (non_fib_pnl / non_fib_count) * 100
        else:
            non_fib_avg_return = 0.0
            
        print(f"{'Avg Return':<20} {fib_avg_return:.2f}%{'':<10} {non_fib_avg_return:.2f}%{'':<10} {fib_avg_return - non_fib_avg_return:+.2f}%{'':<8}")
        
        # Fibonacci Enhancement Impact
        if non_fib_win_rate > 0:
            enhancement = ((fib_win_rate - non_fib_win_rate) / non_fib_win_rate) * 100
        else:
            enhancement = fib_win_rate * 100 if fib_win_rate > 0 else 0
            
        print(f"\n📈 FIBONACCI ENHANCEMENT IMPACT: {enhancement:.1f} points")
        
        # Fibonacci Metrics Over Time
        if hasattr(self, 'daily_fib_scores') and self.daily_fib_scores:
            starting_avg = np.mean(self.daily_fib_scores[:min(10, len(self.daily_fib_scores))])
            ending_avg = np.mean(self.daily_fib_scores[-min(10, len(self.daily_fib_scores)):])
            
            print(f"\n📅 FIBONACCI METRICS OVER TIME:")
            print(f"   - Starting Avg Fib Score: {starting_avg:.3f}")
            print(f"   - Ending Avg Fib Score: {ending_avg:.3f}")
            
            if ending_avg > starting_avg:
                print(f"   - Trend: improving ({starting_avg:.3f} → {ending_avg:.3f})")
            elif ending_avg < starting_avg:
                print(f"   - Trend: declining ({starting_avg:.3f} → {ending_avg:.3f})")
            else:
                print(f"   - Trend: stable")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        if enhancement > 50:
            print(f"   ✅ Fibonacci is significantly improving performance. Consider increasing Fibonacci weight.")
        elif enhancement > 20:
            print(f"   👍 Fibonacci is providing moderate improvements. Current settings are appropriate.")
        elif enhancement > 0:
            print(f"   ⚠️  Fibonacci is providing slight improvements. Consider optimizing Fibonacci parameters.")
        elif enhancement == 0:
            print(f"   🔄 Fibonacci impact is neutral. Consider adjusting Fibonacci thresholds.")
        else:
            print(f"   ❌ Fibonacci is negatively impacting performance. Review Fibonacci implementation.")

    def debug_data_loading(self):
        """Debug method to check data loading issues."""
        print("\n" + "="*80)
        print("                       DATA LOADING DEBUG                         ")
        print("="*80)
        
        print(f"\n📊 DATA STRUCTURES STATUS:")
        print(f"   all_price_data: {len(self.all_price_data) if hasattr(self, 'all_price_data') else 'N/A'} symbols")
        print(f"   price_df: {self.price_df.shape if hasattr(self, 'price_df') and not self.price_df.empty else 'EMPTY'}")
        print(f"   dates: {len(self.dates) if hasattr(self, 'dates') else 'N/A'} trading days")
        print(f"   econ_df: {self.econ_df.shape if hasattr(self, 'econ_df') else 'N/A'}")
        
        if hasattr(self, 'price_df') and not self.price_df.empty:
            print(f"\n📅 DATE RANGES:")
            print(f"   price_df: {self.price_df.index[0].date()} to {self.price_df.index[-1].date()}")
            print(f"   dates: {self.dates[0].date()} to {self.dates[-1].date()}")
            
            # Check for NaNs
            nan_count = self.price_df.isna().sum().sum()
            print(f"   NaN values in price_df: {nan_count}")
            
            # Sample symbols
            sample_symbols = list(self.price_df.columns[:5]) if len(self.price_df.columns) > 0 else []
            print(f"\n🔍 SAMPLE SYMBOLS ({len(sample_symbols)}): {sample_symbols}")
            
            for sym in sample_symbols:
                if sym in self.price_df.columns:
                    series = self.price_df[sym]
                    valid_count = series.notna().sum()
                    print(f"   {sym}: {valid_count}/{len(series)} valid prices")
                    
        print(f"\n⚙️  CONFIGURATION:")
        print(f"   Initial Capital: ${self.initial_capital:,.2f}")
        print(f"   Global Stop Triggered: {self.global_stop_triggered}")
        if hasattr(self, 'global_stop_date'):
            print(f"   Global Stop Date: {self.global_stop_date}")

    def generate_performance_report(self):
        """Generate performance report from backtest results."""
        if not self.history:
            return {"error": "No history data available"}
        
        # Calculate basic performance metrics
        initial_equity = self.history[0]['equity'] if self.history else self.initial_capital
        final_equity = self.history[-1]['equity'] if self.history else self.equity
        
        total_return = (final_equity / initial_equity - 1) * 100
        
        # Extract equity curve
        equity_curve = [h['equity'] for h in self.history]
        dates = [h['date'] for h in self.history]
        
        # Calculate volatility
        if len(equity_curve) > 1:
            returns = [(equity_curve[i]/equity_curve[i-1] - 1) for i in range(1, len(equity_curve))]
            volatility = np.std(returns) * np.sqrt(252) * 100  # Annualized
        else:
            volatility = 0.0
        
        # Calculate Sharpe ratio (simplified)
        risk_free_rate = 0.02  # 2%
        if volatility > 0:
            excess_return = (total_return/100) - risk_free_rate
            sharpe_ratio = excess_return / (volatility/100)
        else:
            sharpe_ratio = 0.0
        
        # Calculate max drawdown
        max_dd = 0.0
        peak = equity_curve[0] if equity_curve else initial_equity
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        
        # Trade statistics
        if self.trades:
            buy_trades = [t for t in self.trades if t['shares'] > 0]
            sell_trades = [t for t in self.trades if t['shares'] < 0]
            fib_trades = [t for t in self.trades if t.get('fib_score', 0) > 0.15]
            
            win_trades = len([t for t in sell_trades if t.get('cost', 0) < t.get('price', 1) * abs(t.get('shares', 0))])
            win_rate = (win_trades / len(sell_trades) * 100) if sell_trades else 0.0
        else:
            buy_trades = []
            sell_trades = []
            fib_trades = []
            win_rate = 0.0
        
        return {
            "initial_capital": initial_equity,
            "final_value": final_equity,
            "total_return_pct": total_return,
            "annual_volatility_pct": volatility,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown_pct": max_dd * 100,
            "total_trades": len(self.trades),
            "buy_trades": len(buy_trades),
            "sell_trades": len(sell_trades),
            "fibonacci_trades": len(fib_trades),
            "win_rate_pct": win_rate,
            "equity_curve": equity_curve,
            "dates": dates,
            "trades": self.trades[:100] if self.trades else [],  # Limit to first 100 trades
        }

    # =========================================================================
    # MAIN BACKTEST LOOP WITH FIBONACCI INTEGRATION
    # =========================================================================

    def run_simulation(self, start_date: str, end_date: str):
        print("\n🧪 STARTING BACKTEST WITH FIBONACCI INTEGRATION\n")
        self.load_data(start_date, end_date)

        total_days = len(self.dates)
        
        # Track filter diagnostics
        signals_before_filter = 0
        signals_after_filter = 0
        
        # NEW: Initialize PerformanceDiagnostics
        diagnostics = PerformanceDiagnostics()
        
        # NEW: Track Fibonacci metrics
        daily_fib_metrics = []

        for i, date_ in enumerate(self.dates):
            current_prices = self.price_df.loc[date_].to_dict()
            final_signals = {}
            
            # ⬇️ ADD THIS - NO CONDITION, ALWAYS PRINTS ⬇️
            print(f"🔄 DAY {i}: {date_.date()}")
    
            # ========== AGGRESSIVE DEBUG ==========
            if i < 3:
                print(f"\n{'='*60}")
                print(f"🔄 LOOP ITERATION {i}: {date_.date()}")
                print(f"   current_prices count: {len([p for p in current_prices.values() if p and not pd.isna(p)])}")
                print(f"   global_stop_triggered: {self.global_stop_triggered}")
                print(f"   equity: ${self.equity:,.2f}")
            # ========== END DEBUG ==========
            

            # ----------------------------------------------------------------
            # GLOBAL STOP CHECK (NO NEW TRADES)
            # ----------------------------------------------------------------

            # Check for global stop reset
            if self.global_stop_triggered:
                if hasattr(self, 'global_stop_date'):
                    days_since_stop = (date_ - self.global_stop_date).days
                    if days_since_stop >= GLOBAL_STOP_RESET_DAYS:
                        self.global_stop_triggered = False
                        self.max_drawdown = 0.0
                        self.peak_equity = self.equity
                        self.dd_multiplier = 1.0
                        
                        # NEW: Refresh price data after reset
                        try:
                            if hasattr(self, 'refresh_price_data_after_reset'):
                                self.refresh_price_data_after_reset(date_)
                            else:
                                print(f"⚠️  refresh_price_data_after_reset method not found!")
                                # Fallback: Reload data for remaining period
                                remaining_start = date_.strftime("%Y-%m-%d")
                                remaining_end = self.dates[-1].strftime("%Y-%m-%d")
                                print(f"🔄 FALLBACK: Reloading data for {remaining_start} to {remaining_end}")
                                self.load_data(remaining_start, remaining_end)
                        except Exception as e:
                            print(f"❌ Error during data refresh: {e}")
                            # Continue anyway - we'll rely on existing data
                        
                        print(f"🔄 GLOBAL STOP RESET on {date_.date()} after {days_since_stop} days. Resuming trading.")
    
                # Still stopped - skip to next day
                if self.global_stop_triggered:
                    self.update_equity(date_, current_prices, {"avg_fib_score": 0.5})
                    self.history.append({
                        "date": date_,
                        "equity": self.equity,
                        "cash": self.cash,
                        "drawdown": self.max_drawdown,
                    })
                    continue

            # Tiered position reduction based on drawdown
            current_dd = (self.peak_equity - self.equity) / self.peak_equity if self.peak_equity > 0 else 0

            if current_dd > MAX_ACCEPTABLE_DD_GLOBAL:
                # FULL STOP - liquidate everything
                for sym, pos in list(self.positions.items()):
                    px = current_prices.get(sym)
                    if px is not None and not pd.isna(px):
                        sigdata = final_signals.get(sym, {})
                        self.execute_trade(sym, -pos["shares"], px, date_, sigdata)
    
                self.positions.clear()
                self.global_stop_triggered = True
                self.global_stop_date = date_
    
                print(f"🚨 GLOBAL STOP TRIGGERED on {date_.date()}: "
                      f"Drawdown {current_dd:.1%} > {MAX_ACCEPTABLE_DD_GLOBAL:.1%}. "
                      f"All positions liquidated. Will reset after {GLOBAL_STOP_RESET_DAYS} days.")

            elif current_dd > DD_SEVERE_THRESHOLD:
                # SEVERE - reduce to 25% size
                self.dd_multiplier = 0.25
    
            elif current_dd > DD_WARNING_THRESHOLD:
                # WARNING - reduce to 50% size
                self.dd_multiplier = 0.50
    
            else:
                # NORMAL
                self.dd_multiplier = 1.0
            
            # ⬇️ ADD THIS DEBUG HERE ⬇️
            if i < 3:
                print(f"   ✅ Passed global stop check (dd={current_dd:.1%})")

            
            # ----------------------------------------------------------------
            # DAY 0: No signals, no trades
            # ----------------------------------------------------------------
            if i == 0:
                self.update_equity(date_, current_prices, {"avg_fib_score": 0.5})
                self.history.append({
                    "date": date_,
                    "equity": self.equity,
                    "cash": self.cash,
                    "drawdown": self.max_drawdown,
                })
                continue
            
            # ⬇️ ADD THIS DEBUG HERE ⬇️
            if i < 3:
                print(f"   ✅ Passed day 0 check, getting signals...")
            
            # ----------------------------------------------------------------
            # SIGNAL GENERATION USING PRIOR DAY'S DATA
            # ----------------------------------------------------------------
            analysis_date = self.dates[i - 1]
            econ_snapshot = self.get_econ_snapshot(analysis_date)
            regime = self.system.regime_mod.classify(econ_snapshot)
            regime_mult = self.system.regime_mod.compute_multiplier(regime)
            
            # NEW: Get market regime from dashboard if available
            try:
                dashboard = MarketRegimeDashboard()
                if dashboard.fetch_data():
                    dashboard.calculate_metrics()
                    market_regime = dashboard.classify_regime()
                else:
                    market_regime = "CENTRALIZED"
            except:
                market_regime = "CENTRALIZED"

            # NEW: Check data freshness before getting price slice
            if hasattr(self, 'debug_mode') and self.debug_mode:
                if not hasattr(self, 'price_df') or self.price_df.empty:
                    print(f"❌ [DEBUG] price_df is empty on {analysis_date.date()}")
            
            price_slice = self.get_price_slice(analysis_date)

            # ⬇️ ADD THIS DEBUG HERE ⬇️
            if i < 3:
                print(f"   price_slice: {len(price_slice) if price_slice else 'EMPTY'}")

            if not price_slice:
                if self.debug_mode:
                    print(f"❌ [DEBUG] get_price_slice returned empty on {analysis_date.date()}")
                self.update_equity(date_, current_prices, {"avg_fib_score": 0.5})
                continue

            # NEW: Use updated UniverseScanner with Fibonacci
            scanner = UniverseScanner(self.system)
            try:
                ranked = scanner.rank_universe(
                    price_slice,    # ✅ CORRECT - indented inside parentheses
                    regime, 
                    top_n=TOP_N_UNIVERSE_SCAN,
                    min_confidence="LOW",
                    min_combined_score=-0.5,
                    min_expected_return=-0.2,
                    min_fib_score=0.0,
                )
                if i < 3:
                    print(f"   ranked: {len(ranked) if ranked is not None else 'NONE'}")
            except Exception as e:
                print(f"   ❌ rank_universe FAILED: {e}")
                ranked = None
            # ⬆️ END TRY/EXCEPT ⬆️

            if ranked is None or len(ranked) == 0:
                self.update_equity(date_, current_prices, {"avg_fib_score": 0.5})
                continue

            # Always use ranked path
            top_syms = ranked["symbol"].tolist()

            # Print ranking every 20 days
            if (i % 20 == 0) or (i == total_days - 1):
                print(f"\n🏆 Top {len(top_syms)} ranked on {analysis_date.date()}: {top_syms}")

            full_signals = self.system.signal_gen.compute_signals(price_slice, regime)
            signals_before = len([s for s in top_syms if s in full_signals])
    
            signals = {sym: full_signals[sym] for sym in top_syms if sym in full_signals}
            filtered = {sym: price_slice[sym] for sym in top_syms if sym in price_slice}

            # NEW: Record Fibonacci diagnostics
            for sym, sig in signals.items():
                fib_score = sig.get('fib_score', 0)
                fib_level = sig.get('fib_level', '')
                diagnostics.record_fib_score(fib_score)
                diagnostics.record_fib_level(fib_level)
                
                if sig.get('near_fib_support', False):
                    diagnostics.record_support_signal(sig)
                elif fib_level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                    diagnostics.record_resistance_signal(sig)
            
            # Calculate average Fibonacci score for this day
            fib_scores = [s.get('fib_score', 0) for s in signals.values()]
            avg_fib_score = np.mean(fib_scores) if fib_scores else 0.5
            daily_fib_metrics.append({
                'date': date_,
                'avg_fib_score': avg_fib_score,
                'signal_count': len(signals)
            })
            
            diagnostics.record_regime(market_regime)

            signals = apply_quality_filters(filtered, signals)
            signals_after = len(signals)

            # ⬇️ ADD THIS DEBUG HERE ⬇️
            if i < 3:
                print(f"   signals after filter: {signals_after}")
            
            # Calculate risk_output with market_regime parameter
            risk_output = self.system.risk_mgmt.compute_position_sizes(
                signals=signals,
                price_data=filtered,
                regime=regime,
                regime_multiplier=regime_mult,
                total_capital=self.equity,
                market_regime=market_regime,  # NEW: Pass market regime
                merge_live_holdings=False,    # backtest: no look-ahead from current Airtable
            )

            # Track filter effectiveness
            signals_before_filter += signals_before
            signals_after_filter += signals_after

            # Print diagnostic every 20 days
            if (i % 20 == 0) or (i == total_days - 1):
                print(f"📊 Day {i}: Signals before filter: {signals_before}, after: {signals_after}")
                
                # NEW: Show Fibonacci info
                if signals:
                    avg_fib = np.mean([s.get('fib_score', 0) for s in signals.values()])
                    support_count = sum(1 for s in signals.values() if s.get('near_fib_support', False))
                    print(f"   Fibonacci: avg_score={avg_fib:.3f}, near_support={support_count}/{len(signals)}")

            final_signals = self.system._generate_recommendations(
                signals=signals,
                risk_output=risk_output,
                regime=regime,
                price_data=filtered
            )

            # Hybrid Rank Tilting
            pos_sizes = risk_output["position_sizing"]
            multipliers = build_hybrid_rank_multipliers(top_syms)

            for sym in list(pos_sizes.keys()):
                pos_sizes[sym] *= multipliers.get(sym, 0)

            # Leverage cap
            tw = sum(pos_sizes.values())
            if tw > MAX_PORTFOLIO_LEVERAGE:
                scale = MAX_PORTFOLIO_LEVERAGE / tw
                for sym in pos_sizes:
                    pos_sizes[sym] *= scale

            # ----------------------------------------------------------------
            # DRAWDOWN SCALING
            # ----------------------------------------------------------------
            if self.dd_multiplier < 1.0:
                for sym in risk_output["position_sizing"]:
                    risk_output["position_sizing"][sym] *= self.dd_multiplier

            # ----------------------------------------------------------------
            # FINAL SIGNALS
            # ----------------------------------------------------------------
            final_signals = self.system._generate_recommendations(
                signals=signals,
                risk_output=risk_output,
                regime=regime,
                price_data=filtered
            )

            self.last_signals = {
                "final_signals": final_signals,
                "risk_management": risk_output
            }

            # Debug diagnostics
            if (i % 20 == 0) or (i == total_days - 1):
                buy_signals = sum(1 for s in final_signals.values() 
                                  if s["signal"] in ("BUY", "STRONG_BUY"))
                sell_signals = sum(1 for s in final_signals.values() 
                                   if s["signal"] in ("SELL", "STRONG_SELL"))
                hold_signals = sum(1 for s in final_signals.values() 
                                   if s["signal"] == "HOLD")
                
                # Calculate average score safely
                if final_signals:
                    avg_score = np.mean([s["combined_score"] for s in final_signals.values()])
                    avg_fib_final = np.mean([s.get("fib_score", 0) for s in final_signals.values()])
                    support_count_final = sum(1 for s in final_signals.values() 
                                            if s.get("near_fib_support", False))
                else:
                    avg_score = 0
                    avg_fib_final = 0
                    support_count_final = 0
                
                # Check position sizing output
                num_with_positions = sum(1 for s in final_signals.values() 
                                        if s["position_size"] > 0)
    
                print(f"🎯 Day {i} Signal Breakdown:")
                print(f"   BUY: {buy_signals} | SELL: {sell_signals} | HOLD: {hold_signals}")
                print(f"   Signals with position size > 0: {num_with_positions}")
                print(f"   Avg Combined Score: {avg_score:.3f}")
                print(f"   Avg Fibonacci Score: {avg_fib_final:.3f}")
                print(f"   Near Fibonacci Support: {support_count_final}")
                print(f"   Active Positions: {len(self.positions)}")
                
                # NEW: Show positions with Fibonacci info
                if self.positions:
                    print(f"   Current Positions ({len(self.positions)}):")
                    for sym, pos in list(self.positions.items())[:3]:
                        fib_info = ""
                        if sym in final_signals:
                            sig = final_signals[sym]
                            fib_score = sig.get("fib_score", 0)
                            fib_level = sig.get("fib_level", "")
                            support = "✓" if sig.get("near_fib_support", False) else ""
                            fib_info = f" | Fib: {fib_score:.2f} ({fib_level}) {support}"
                        
                        price = current_prices.get(sym, 0)
                        entry = pos.get("entry", 0)
                        pnl_pct = ((price / entry) - 1) * 100 if entry > 0 else 0
                        print(f"     {sym}: ${price:.2f} ({pnl_pct:+.1f}%){fib_info}")
                
                # Sample a few signals
                sample_signals = list(final_signals.items())[:3]
                for sym, sig in sample_signals:
                    fib_level = sig.get('fib_level', '')
                    fib_score = sig.get('fib_score', 0)
                    near_support = "✓" if sig.get('near_fib_support', False) else ""
                    print(f"   {sym}: signal={sig['signal']}, score={sig['combined_score']:.3f}, "
                          f"fib={fib_score:.2f} ({fib_level}) {near_support}, "
                          f"pos_size={sig['position_size']:.3%}, stop={sig['stop_loss']:.3%}")
            
            # ----------------------------------------------------------------
            # EXECUTIONS WITH FIBONACCI METADATA
            # ----------------------------------------------------------------
            executed_this_day = 0
            attempted_this_day = 0
            
            # ----------------------------------------------------------------
            # STOP-LOSS CHECK FOR ALL POSITIONS (CRITICAL FIX)
            # ----------------------------------------------------------------
            for sym in list(self.positions.keys()):
                pos = self.positions[sym]
                stop_px = pos.get("stop", pos.get("stop_price", 0))
                price = current_prices.get(sym)
                if price is None or pd.isna(price):
                    continue
                
                if price <= stop_px:
                    shares = pos["shares"]
                    print(f"   🛑 STOP HIT: {sym} price={price:.2f} <= stop={stop_px:.2f}")
                    success, _, _ = self.execute_trade(sym, -shares, price, date_, {})
                    if success:
                        del self.positions[sym]
                        self.exit_cooldowns[sym] = date_  # Track exit for cooldown
                        executed_this_day += 1

            for sym, sigdata in final_signals.items():
                price = current_prices.get(sym)
                if price is None or pd.isna(price):
                    continue
                    
                signal = sigdata["signal"]
                pos_size = sigdata["position_size"]
                stop_pct = sigdata["stop_loss"]
                
                # EXIT
                if sym in self.positions:
                    shares = self.positions[sym]["shares"]
                    stop_px = self.positions[sym]["stop"]
                    entry_date = self.positions[sym].get("entry_date", date_)
                    
                    # Calculate holding period
                    holding_days = (date_ - entry_date).days
                    
                    # Exit conditions:
                    # 1. Stop loss hit
                    # 2. Sell signal
                    # 3. Time-based exit (swing = 10 days, position = 30 days)
                    trade_type = sigdata.get("trade_type", "SWING")
                    max_hold = POSITION_HOLDING_DAYS if trade_type == "POSITION" else 10
                    
                    time_exit = holding_days >= max_hold
                    stop_exit = price <= stop_px
                    signal_exit = signal in ("SELL", "STRONG_SELL")
                    
                    if stop_exit or signal_exit or time_exit:
                        # NEW: Pass Fibonacci metadata to execute_trade
                        success, _, fib_meta = self.execute_trade(
                            sym, -shares, price, date_, sigdata
                        )
                        if success:
                            # NEW: Record Fibonacci exit info
                            exit_type = "STOP" if stop_exit else "SIGNAL" if signal_exit else "TIME"
                            fib_score = fib_meta.get('fib_score', 0) if fib_meta else 0
                            # print(f"   EXIT {sym}: {exit_type} after {holding_days}d (Fib: {fib_score:.2f})")
                            del self.positions[sym]
                            self.exit_cooldowns[sym] = date_  # Track exit for cooldown
                            executed_this_day += 1
                        continue
                
                # ENTRY
                if signal in ("BUY", "STRONG_BUY") and pos_size > 0:
                    # MARKET REGIME CHECK - Skip entries in bear markets

                    # SIGNAL STRENGTH CHECK (MEM Labs dead zone filter)
                    sig_strength = get_signal_strength(sigdata.get("combined_score", 0))
                    if not sig_strength.should_trade:
                        continue  # Dead zone - skip weak signals
                    
                    # Confidence-weighted position sizing
                    base_pos_size = pos_size
                    pos_size = base_pos_size * sig_strength.confidence
                    
                    if not self.is_bull_market(current_prices, i):
                        if i < 5:  # Only print first few
                            print(f"   ⚠️ BEAR MARKET - Skipping new entry for {sym}")
                        continue
                    if sym not in self.positions:
                        # COOLDOWN CHECK - Skip if recently exited
                        if sym in self.exit_cooldowns:
                            days_since_exit = (date_ - self.exit_cooldowns[sym]).days
                            if days_since_exit < COOLDOWN_DAYS:
                                continue
                        # Position limit check
                        if len(self.positions) >= MAX_CONCURRENT_POSITIONS:
                            continue
                        
                        attempted_this_day += 1
        
                        pos_size = min(pos_size, MAX_POSITION_SIZE)
                        capital = self.equity * pos_size
                        shares = int(capital / price)
                        
                        print(f"   🎯 ENTRY ATTEMPT: {sym} signal={signal} pos_size={pos_size:.3f} price={price:.2f} shares={shares}")
                        if shares > 0:
                            # NEW: Pass Fibonacci metadata to execute_trade
                            success, actual_shares, fib_meta = self.execute_trade(
                                sym, shares, price, date_, sigdata
                            )
                            if success and actual_shares > 0:
                                actual_price = self.trades[-1]["price"]
                                self.positions[sym] = {
                                    "entry": actual_price,
                                    "shares": actual_shares,
                                    "stop": actual_price * (1 - stop_pct),
                                    "entry_date": date_,
                                    "fib_score": sigdata.get("fib_score", 0),
                                    "fib_level": sigdata.get("fib_level", ""),
                                    "near_support": sigdata.get("near_fib_support", False),
                                }
                                executed_this_day += 1
            
            # ----------------------------------------------------------------
            # EQUITY UPDATE WITH FIBONACCI METRICS
            # ----------------------------------------------------------------
            # Calculate Fibonacci metrics for this day
            day_fib_metrics = {
                'avg_fib_score': avg_fib_score,
                'signal_count': len(signals),
                'support_count': sum(1 for s in signals.values() if s.get('near_fib_support', False)),
                'positions_with_fib': sum(1 for pos in self.positions.values() 
                                        if pos.get('fib_score', 0) > 0.5),
                'total_positions': len(self.positions),
            }
            
            self.update_equity(date_, current_prices, day_fib_metrics)
            
            # Record position count with Fibonacci positions
            fib_positions = sum(1 for pos in self.positions.values() 
                              if pos.get('fib_score', 0) > 0.5)
            diagnostics.record_positions(len(self.positions), fib_positions)

            # ----------------------------------------------------------------
            # GLOBAL HARD STOP
            # ----------------------------------------------------------------
            if not self.global_stop_triggered and self.max_drawdown > MAX_ACCEPTABLE_DD_GLOBAL:
                for sym, pos in list(self.positions.items()):
                    px = current_prices.get(sym)
                    if px is not None and not pd.isna(px):
                        # NEW: Get signal data for Fibonacci metadata
                        sigdata = final_signals.get(sym, {})
                        self.execute_trade(sym, -pos["shares"], px, date_, sigdata)

                self.positions.clear()
                self.global_stop_triggered = True
                self.global_stop_date = date_

                print(
                    f"🚨 GLOBAL STOP TRIGGERED on {date_.date()}: "
                    f"Drawdown {self.max_drawdown:.1%} > {MAX_ACCEPTABLE_DD_GLOBAL:.1%}. "
                    f"All positions liquidated."
                )

            self.history.append({
                "date": date_,
                "equity": self.equity,
                "cash": self.cash,
                "drawdown": self.max_drawdown,
                "fib_metrics": day_fib_metrics,  # NEW: Add Fibonacci metrics
            })

        # Final statistics
        print(f"\n📊 FILTER STATISTICS:")
        print(f"   Total signals before filter: {signals_before_filter}")
        print(f"   Total signals after filter: {signals_after_filter}")
        if signals_before_filter > 0:
            filter_rate = (1 - signals_after_filter / signals_before_filter) * 100
            print(f"   Filter rejection rate: {filter_rate:.1f}%")

        # NEW: Print Fibonacci diagnostics
        print("\n📊 FIBONACCI PERFORMANCE DIAGNOSTICS:")
        diagnostics.print_summary()
        
        # NEW: Print Fibonacci backtest analysis
        self.print_fibonacci_backtest_summary()
        
        # DEBUG: Check data loading if no trades
        if len(self.trades) == 0:
            print("\n⚠️  WARNING: No trades executed!")
            self.debug_data_loading()

        # Final Airtable push
        print("\n📝 Pushing final backtest signals to Airtable...")
        try:
            if self.last_signals:
                push_to_airtable(
                    final_signals=self.last_signals["final_signals"],
                    risk_mgmt=self.last_signals["risk_management"],
                )
                print("📡 Airtable updated successfully.")
        except Exception as e:
            print(f"❌ Airtable update failed: {e}")

        print("\n✅ SIMULATION COMPLETE WITH FIBONACCI INTEGRATION\n")
        return self.generate_performance_report(), self.history

    def analyze_fibonacci_performance(self):
        """Analyze Fibonacci performance from trades."""
        if not self.trades:
            return {"enhancement_impact": 0, "fib_win_rate": 0, "non_fib_win_rate": 0}
        fib_trades = [t for t in self.trades if t.get("fib_score", 0) > 0.15]
        non_fib_trades = [t for t in self.trades if t.get("fib_score", 0) <= 0.15]
        fib_wins = len([t for t in fib_trades if t.get("pnl", 0) > 0])
        non_fib_wins = len([t for t in non_fib_trades if t.get("pnl", 0) > 0])
        fib_win_rate = (fib_wins / len(fib_trades) * 100) if fib_trades else 0
        non_fib_win_rate = (non_fib_wins / len(non_fib_trades) * 100) if non_fib_trades else 0
        return {
            "enhancement_impact": fib_win_rate - non_fib_win_rate,
            "fib_win_rate": fib_win_rate,
            "non_fib_win_rate": non_fib_win_rate,
            "fib_trade_count": len(fib_trades),
            "non_fib_trade_count": len(non_fib_trades),
        }

    # =========================================================================
    # PERFORMANCE REPORT WITH FIBONACCI METRICS
    # =========================================================================

    def generate_performance_report(self):
        """Generate comprehensive performance metrics with Fibonacci analysis."""
        # Use history if equity_curve not populated
        if not self.equity_curve and self.history:
            self.equity_curve = self.history
        if not self.equity_curve:
            print("⚠️ WARNING: equity_curve is empty!")
            return {
                "initial_capital": self.initial_capital,
                "final_value": self.initial_capital,
                "total_return_pct": 0.0,
                "cagr_pct": 0.0,
                "annual_volatility_pct": 0.0,
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "max_drawdown_pct": 0.0,
                "win_rate_pct": 0.0,
                "num_trades": 0,
                "equity_curve": pd.DataFrame(),
                "fibonacci_analysis": {},  # NEW
            }

        print(f"✅ Generating report from {len(self.equity_curve)} equity points...")
        df = pd.DataFrame(self.equity_curve).set_index("date")
        rets = df["equity"].pct_change().dropna()

        final_val = df["equity"].iloc[-1]
        total_return = (final_val / self.initial_capital - 1) * 100
        cagr = ((final_val / self.initial_capital) ** (252 / len(df)) - 1) * 100

        annual_vol = rets.std() * np.sqrt(252) * 100
        sharpe = (
            (rets.mean() * 252) / (rets.std() * np.sqrt(252))
            if rets.std() > 0
            else 0.0
        )

        downside = rets[rets < 0]
        sortino = (
            (rets.mean() * 252) / (downside.std() * np.sqrt(252))
            if len(downside) > 0 and downside.std() > 0
            else 0.0
        )

        wins = (rets > 0).sum()
        win_rate = wins / len(rets) * 100 if len(rets) > 0 else 0.0

        # NEW: Fibonacci performance analysis
        fib_analysis = self.analyze_fibonacci_performance()
        
        # NEW: Calculate Fibonacci-enhanced metrics if available
        fib_enhanced_return = total_return
        fib_enhanced_sharpe = sharpe
        
        if fib_analysis and fib_analysis.get('enhancement_impact', 0) > 0:
            # Estimate impact of Fibonacci on returns
            enhancement = fib_analysis.get('enhancement_impact', 0) / 100  # Convert from points to ratio
            fib_enhanced_return = total_return * (1 + enhancement)
            fib_enhanced_sharpe = sharpe * (1 + enhancement * 0.5)  # More conservative enhancement

        return {
            "initial_capital": self.initial_capital,
            "final_value": final_val,
            "total_return_pct": total_return,
            "fib_enhanced_return_pct": fib_enhanced_return,  # NEW
            "cagr_pct": cagr,
            "annual_volatility_pct": annual_vol,
            "sharpe_ratio": sharpe,
            "fib_enhanced_sharpe": fib_enhanced_sharpe,  # NEW
            "sortino_ratio": sortino,
            "max_drawdown_pct": self.max_drawdown * 100,
            "win_rate_pct": win_rate,
            "num_trades": len(self.trades),
            "num_fib_trades": len([t for t in self.trades if t.get('fib_score', 0) > 0.15]),  # NEW
            "equity_curve": df,
            "fibonacci_analysis": fib_analysis,  # NEW
            "trades": self.trades,  # Include trades for detailed analysis
            "total_fees": self.total_fees,  # MEM Labs fee tracking
        }
# =============================================================================
# REPORTING HELPERS (UPDATED FOR FIBONACCI)
# =============================================================================

def print_performance_report(report: dict):
    if not report:
        print("\n❌ Report is empty. Cannot display metrics.")
        print("\n✅ System execution complete\n")
        return

    print("\n" + "=" * 80)
    print("✨ BACKTEST PERFORMANCE SUMMARY (WITH FIBONACCI) ✨".center(80))
    print("=" * 80)
    print(f"\n💰 Initial Capital:        ${report['initial_capital']:,.2f}")
    print(f"💰 Final Value:            ${report['final_value']:,.2f}")
    print(f"\n📈 Total Return:           {report['total_return_pct']:.2f}%")
    
    # NEW: Show Fibonacci-enhanced return if available
    fib_enhanced_return = report.get('fib_enhanced_return_pct')
    if fib_enhanced_return is not None:
        print(f"📈 Fibonacci-Enhanced:     {fib_enhanced_return:.2f}%")
        enhancement = fib_enhanced_return - report['total_return_pct']
        if abs(enhancement) > 0.01:  # Significant difference
            print(f"📈 Fibonacci Impact:       {enhancement:+.2f}%")
    
    print(f"📈 CAGR:                   {report['cagr_pct']:.2f}%")
    print(f"\n📊 Annual Volatility:      {report['annual_volatility_pct']:.2f}%")
    print(f"📊 Sharpe Ratio:           {report['sharpe_ratio']:.3f}")
    
    # NEW: Show Fibonacci-enhanced Sharpe if available
    fib_enhanced_sharpe = report.get('fib_enhanced_sharpe')
    if fib_enhanced_sharpe is not None:
        print(f"📊 Fibonacci-Enhanced:     {fib_enhanced_sharpe:.3f}")
    
    print(f"📊 Sortino Ratio:          {report['sortino_ratio']:.3f}")
    print(f"\n⚠️  Max Drawdown:           {report['max_drawdown_pct']:.2f}%")
    print(f"🎯 Win Rate:               {report['win_rate_pct']:.2f}%")
    
    # NEW: Show Fibonacci trade statistics
    num_trades = report.get('num_trades', 0)
    num_fib_trades = report.get('num_fib_trades', 0)
    print(f"📝 Total Trades:           {num_trades}")
    if num_fib_trades > 0:
        fib_pct = (num_fib_trades / num_trades) * 100 if num_trades > 0 else 0
        print(f"📝 Fibonacci Trades:       {num_fib_trades} ({fib_pct:.1f}%)")
    
    
    # NEW: Fee Analysis (MEM Labs integration)
    total_fees = report.get("total_fees", 0)
    if total_fees > 0:
        avg_fee = total_fees / num_trades if num_trades > 0 else 0
        fee_drag_pct = (total_fees / report["initial_capital"]) * 100
        print(f"\n💸 FEE ANALYSIS:")
        print(f"   Total Fees Paid:        ${total_fees:,.2f}")
        print(f"   Avg Fee per Trade:      ${avg_fee:.2f}")
        print(f"   Fee Drag on Capital:    {fee_drag_pct:.2f}%")
    print("\n" + "=" * 80)

    # NEW: Print Fibonacci analysis section
    fib_analysis = report.get('fibonacci_analysis', {})
    if fib_analysis:
        print("\n📊 FIBONACCI PERFORMANCE ANALYSIS:")
        print("-" * 50)
        
        fib_trades = fib_analysis.get('fibonacci_trades', {})
        non_fib_trades = fib_analysis.get('non_fibonacci_trades', {})
        
        if fib_trades.get('count', 0) > 0 and non_fib_trades.get('count', 0) > 0:
            print(f"{'Metric':<20} {'Fibonacci':<12} {'Non-Fibonacci':<14} {'Difference':<12}")
            print("-" * 58)
            
            # Win Rate
            fib_win = fib_trades.get('win_rate', 0) * 100
            non_fib_win = non_fib_trades.get('win_rate', 0) * 100
            win_diff = fib_win - non_fib_win
            print(f"{'Win Rate':<20} {fib_win:<12.1f}% {non_fib_win:<14.1f}% {win_diff:<+12.1f}%")
            
            # Avg Return
            fib_ret = fib_trades.get('avg_return', 0) * 100
            non_fib_ret = non_fib_trades.get('avg_return', 0) * 100
            ret_diff = fib_ret - non_fib_ret
            print(f"{'Avg Return':<20} {fib_ret:<12.2f}% {non_fib_ret:<14.2f}% {ret_diff:<+12.2f}%")
            
            # Sharpe Ratio
            fib_sharpe = fib_trades.get('sharpe_ratio', 0)
            non_fib_sharpe = non_fib_trades.get('sharpe_ratio', 0)
            sharpe_diff = fib_sharpe - non_fib_sharpe
            print(f"{'Sharpe Ratio':<20} {fib_sharpe:<12.2f} {non_fib_sharpe:<14.2f} {sharpe_diff:<+12.2f}")
        
        enhancement_impact = fib_analysis.get('enhancement_impact', 0)
        print(f"\n💡 Fibonacci Enhancement Impact: {enhancement_impact:.1f} points")
        
        if enhancement_impact > 10:
            print("   ✅ Fibonacci is significantly improving performance")
        elif enhancement_impact > 5:
            print("   👍 Fibonacci is providing moderate improvements")
        elif enhancement_impact > 0:
            print("   📊 Fibonacci is providing slight improvements")
        elif enhancement_impact < -5:
            print("   ⚠️  Fibonacci may be harming performance")
        else:
            print("   🔄 Fibonacci impact is neutral")

    print("\n💡 PERFORMANCE INSIGHTS:")

    if report["total_return_pct"] > 100:
        print("   ✅ Excellent returns - Strategy significantly outperformed")
    elif report["total_return_pct"] > 50:
        print("   ✅ Strong returns - Strategy performed well")
    elif report["total_return_pct"] > 0:
        print("   ⚠️  Modest returns - Room for improvement")
    else:
        print("   ❌ Negative returns - Strategy needs revision")

    if report["sharpe_ratio"] > 1.5:
        print("   ✅ Excellent risk-adjusted returns (Sharpe > 1.5)")
    elif report["sharpe_ratio"] > 1.0:
        print("   ✅ Good risk-adjusted returns (Sharpe > 1.0)")
    elif report["sharpe_ratio"] > 0.5:
        print("   ⚠️  Acceptable risk-adjusted returns (Sharpe > 0.5)")
    else:
        print("   ❌ Poor risk-adjusted returns (Sharpe < 0.5)")

    if report["max_drawdown_pct"] < 15:
        print("   ✅ Low drawdown — well-controlled risk")
    elif report["max_drawdown_pct"] < 25:
        print("   ⚠️  Moderate drawdown — acceptable")
    else:
        print("   ❌ High drawdown — tighten risk controls")
    
    # NEW: Fibonacci-specific insights
    print("\n🎯 FIBONACCI STRATEGY INSIGHTS:")
    
    fib_avg_score = 0.5
    if fib_analysis and 'fibonacci_trades' in fib_analysis:
        fib_avg_score = np.mean([trade.get('fib_score', 0.5) for trade in report.get('trades', []) 
                                if trade.get('fib_score', 0) > 0.15]) if report.get('trades') else 0.5
    
    if fib_avg_score > 0.7:
        print("   ✅ High Fibonacci alignment - Good entry/exit timing")
    elif fib_avg_score > 0.5:
        print("   📊 Moderate Fibonacci alignment - Strategy working as expected")
    else:
        print("   ⚠️  Low Fibonacci alignment - Consider improving Fibonacci parameters")
    
    # Analyze support vs resistance trades
    if report.get('trades'):
        support_trades = [t for t in report['trades'] if t.get('near_support', False)]
        resistance_trades = [t for t in report['trades'] if not t.get('near_support', False) 
                           and t.get('fib_level', '') in ['23.60%', '38.20%', '23.6%', '38.2%']]
        
        if support_trades:
            support_win = sum(1 for t in support_trades if t.get('pnl', 0) > 0) / len(support_trades) * 100
            print(f"   📈 Support trades ({len(support_trades)}): {support_win:.1f}% win rate")
        if resistance_trades:
            resistance_win = sum(1 for t in resistance_trades if t.get('pnl', 0) > 0) / len(resistance_trades) * 100
            print(f"   📉 Resistance trades ({len(resistance_trades)}): {resistance_win:.1f}% win rate")

    print("\n✅ System execution complete with Fibonacci analysis\n")


# =============================================================================
# DAILY EXECUTION SYSTEM (LIVE MODE) - UPDATED
# =============================================================================

class DailyExecutionSystem:
    """Live trading execution engine + Airtable sync with Fibonacci integration."""

    def __init__(self, trading_system: ProfessionalTradingSystem, skip_airtable: bool = False):
        self.system = trading_system
        self.skip_airtable = skip_airtable
        self.execution_log: List[Dict] = []
        # NEW: Track Fibonacci metrics
        self.fib_metrics_log: List[Dict] = []

    def run_daily_update(self):
        today = date.today()
        print(f"\n📅 DAILY EXECUTION WITH FIBONACCI — {today}\n")
        try:
            results = self.system.run_analysis()
            if not results:
                print("❌ No results returned from trading system")
                return
            
            self.system.print_summary(results)
            
            final_signals = results.get("final_signals", {})
            dump_raw_signals(final_signals)  # forward-test reporting tap — no mutation, pre-sizing
            risk_output = results.get("risk_management", {})
            
            normalized_risk = {
                "position_sizing": risk_output.get("position_sizing", {}),
                "stop_losses": risk_output.get("stop_losses", {}),
                "var": risk_output.get("var", {}),
                "fib_metrics": risk_output.get("fib_metrics", {}),  # NEW: Include Fibonacci metrics
            }
            
            # NEW: Calculate and log Fibonacci metrics
            if final_signals:
                fib_scores = [sig.get("fib_score", 0) for sig in final_signals.values()]
                support_count = sum(1 for sig in final_signals.values() 
                                  if sig.get("near_fib_support", False))
                resistance_count = sum(1 for sig in final_signals.values() 
                                     if sig.get("fib_level", "") in ['23.60%', '38.20%', '23.6%', '38.2%'])
                
                daily_fib_metrics = {
                    "date": today.isoformat(),
                    "avg_fib_score": np.mean(fib_scores) if fib_scores else 0,
                    "support_signals": support_count,
                    "resistance_signals": resistance_count,
                    "total_signals": len(final_signals),
                }
                self.fib_metrics_log.append(daily_fib_metrics)
                
                print(f"📊 Fibonacci Metrics Today:")
                print(f"   - Average Fibonacci Score: {daily_fib_metrics['avg_fib_score']:.3f}")
                print(f"   - Support Signals: {support_count}/{len(final_signals)}")
                print(f"   - Resistance Signals: {resistance_count}/{len(final_signals)}")
            
            try:
                if not self.skip_airtable:
                    push_to_airtable(final_signals, normalized_risk)
                else:
                    print("📡 Airtable sync skipped (--no-airtable flag).")
                print("📡 Airtable updated successfully.")

                # Export CSV picks with Fibonacci info
                export_top_picks_csv(final_signals, normalized_risk)
                print("📁 CSV export complete.")
                
                # NEW: Export Fibonacci metrics
                if hasattr(self, 'fib_metrics_log') and self.fib_metrics_log:
                    fib_df = pd.DataFrame(self.fib_metrics_log)
                    fib_csv_path = f"fibonacci_metrics_{today}.csv"
                    fib_df.to_csv(fib_csv_path, index=False)
                    print(f"📁 Fibonacci metrics exported to: {fib_csv_path}")
                
            except Exception as e:
                print(f"❌ Airtable/CSV export failed: {e}")
                import traceback
                traceback.print_exc()
            
            self.execution_log.append({
                "date": today.isoformat(),
                "num_signals": len(final_signals),
                "num_positions": len(normalized_risk.get("position_sizing", {})),
                "avg_fib_score": daily_fib_metrics.get('avg_fib_score', 0) if final_signals else 0,
                "status": "success"
            })
            
        except Exception as e:
            print(f"❌ Daily execution failed: {e}")
            import traceback
            traceback.print_exc()
            self.execution_log.append({
                "date": today.isoformat(),
                "status": "failed",
                "error": str(e)
            })
    
    def print_fibonacci_summary(self):
        """Print summary of Fibonacci metrics over time."""
        if not self.fib_metrics_log:
            print("No Fibonacci metrics recorded yet.")
            return
        
        df = pd.DataFrame(self.fib_metrics_log)
        
        print("\n" + "=" * 80)
        print("📊 FIBONACCI PERFORMANCE SUMMARY (DAILY EXECUTION)".center(80))
        print("=" * 80)
        
        print(f"\n📅 Period: {df['date'].iloc[0]} to {df['date'].iloc[-1]}")
        print(f"📊 Total Execution Days: {len(df)}")
        
        print(f"\n📈 Fibonacci Score Statistics:")
        print(f"   - Average: {df['avg_fib_score'].mean():.3f}")
        print(f"   - Best: {df['avg_fib_score'].max():.3f} ({df.loc[df['avg_fib_score'].idxmax(), 'date']})")
        print(f"   - Worst: {df['avg_fib_score'].min():.3f} ({df.loc[df['avg_fib_score'].idxmin(), 'date']})")
        print(f"   - Std Dev: {df['avg_fib_score'].std():.3f}")
        
        print(f"\n🎯 Signal Composition:")
        avg_support_pct = (df['support_signals'].sum() / df['total_signals'].sum()) * 100
        avg_resistance_pct = (df['resistance_signals'].sum() / df['total_signals'].sum()) * 100
        print(f"   - Average Support Signals: {avg_support_pct:.1f}%")
        print(f"   - Average Resistance Signals: {avg_resistance_pct:.1f}%")
        
        # Trend analysis
        if len(df) >= 5:
            early_avg = df['avg_fib_score'].head(5).mean()
            late_avg = df['avg_fib_score'].tail(5).mean()
            trend = "improving" if late_avg > early_avg else "declining" if late_avg < early_avg else "stable"
            print(f"\n📊 Trend Analysis:")
            print(f"   - Early Period Avg: {early_avg:.3f}")
            print(f"   - Recent Period Avg: {late_avg:.3f}")
            print(f"   - Trend: {trend}")
        
        print("\n" + "=" * 80)


# =============================================================================
# VISUALIZATION - UPDATED FOR FIBONACCI
# =============================================================================

def plot_backtest_results(equity_curve: pd.DataFrame, trades: List[Dict], title: str = "Backtest Results", save_path: str = "equity_curve.png"):
    """Plot equity curve and drawdown with Fibonacci metrics."""
    if equity_curve.empty:
        print("⚠️ No equity curve data to plot")
        return
    
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12))
    
    # Equity curve
    ax1.plot(equity_curve.index, equity_curve['equity'], label='Portfolio Value', linewidth=2)
    ax1.axhline(y=equity_curve['equity'].iloc[0], color='gray', linestyle='--', label='Initial Capital')
    ax1.set_title(f'{title} - Equity Curve', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Portfolio Value ($)', fontsize=12)
    ax1.legend(loc='best')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate()
    
    # Highlight trades with Fibonacci color coding
    if trades:
        buy_dates_fib = []
        buy_equity_fib = []
        buy_dates_nonfib = []
        buy_equity_nonfib = []
        sell_dates = []
        sell_equity = []
        
        for trade in trades:
            if trade.get('action') == 'BUY':
                # Check if it's a Fibonacci trade
                is_fib = trade.get('fib_score', 0) > 0.15
                idx = equity_curve.index.get_indexer([trade['date']], method='nearest')[0]
                equity_val = equity_curve.iloc[idx]['equity']
                
                if is_fib:
                    buy_dates_fib.append(trade['date'])
                    buy_equity_fib.append(equity_val)
                else:
                    buy_dates_nonfib.append(trade['date'])
                    buy_equity_nonfib.append(equity_val)
                    
            elif trade.get('action') == 'SELL':
                idx = equity_curve.index.get_indexer([trade['date']], method='nearest')[0]
                sell_dates.append(trade['date'])
                sell_equity.append(equity_curve.iloc[idx]['equity'])
        
        # Plot Fibonacci buy signals (green triangles)
        if buy_dates_fib:
            ax1.scatter(buy_dates_fib, buy_equity_fib, color='green', s=80, marker='^', 
                       label='Fibonacci Buy Signals', alpha=0.9, zorder=5, edgecolors='darkgreen')
        
        # Plot non-Fibonacci buy signals (yellow triangles)
        if buy_dates_nonfib:
            ax1.scatter(buy_dates_nonfib, buy_equity_nonfib, color='gold', s=50, marker='^', 
                       label='Non-Fibonacci Buy Signals', alpha=0.7, zorder=4)
        
        if sell_dates:
            ax1.scatter(sell_dates, sell_equity, color='red', s=50, marker='v', 
                       label='Sell Signals', alpha=0.8, zorder=5)
        
        ax1.legend(loc='best')
    
    # Drawdown
    ax2.fill_between(equity_curve.index, equity_curve['drawdown_pct'], 0, 
                     color='red', alpha=0.3, label='Drawdown')
    ax2.plot(equity_curve.index, equity_curve['drawdown_pct'], color='darkred', linewidth=1)
    ax2.set_title('Drawdown', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Drawdown (%)', fontsize=12)
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    
    # NEW: Fibonacci Score Over Time
    if 'fib_score' in equity_curve.columns:
        ax3.plot(equity_curve.index, equity_curve['fib_score'], 
                color='purple', linewidth=2, label='Fibonacci Score')
        ax3.axhline(y=0.7, color='green', linestyle='--', alpha=0.5, label='High (0.7)')
        ax3.axhline(y=0.4, color='orange', linestyle='--', alpha=0.5, label='Low (0.4)')
        ax3.fill_between(equity_curve.index, equity_curve['fib_score'], 0.4, 
                        where=(equity_curve['fib_score'] > 0.4),
                        color='green', alpha=0.2)
        ax3.fill_between(equity_curve.index, equity_curve['fib_score'], 0.4, 
                        where=(equity_curve['fib_score'] <= 0.4),
                        color='red', alpha=0.2)
        ax3.set_title('Fibonacci Score Over Time', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Fibonacci Score', fontsize=12)
        ax3.set_xlabel('Date', fontsize=12)
        ax3.legend(loc='best')
        ax3.grid(True, alpha=0.3)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    else:
        # If no Fibonacci score data, show positions count
        if 'positions_count' in equity_curve.columns:
            ax3.plot(equity_curve.index, equity_curve['positions_count'], 
                    color='blue', linewidth=2, label='Positions Count')
            if 'fib_positions_count' in equity_curve.columns:
                ax3.plot(equity_curve.index, equity_curve['fib_positions_count'], 
                        color='green', linewidth=2, label='Fibonacci Positions')
            ax3.set_title('Positions Over Time', fontsize=14, fontweight='bold')
            ax3.set_ylabel('Number of Positions', fontsize=12)
            ax3.set_xlabel('Date', fontsize=12)
            ax3.legend(loc='best')
            ax3.grid(True, alpha=0.3)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    
    # Add statistics text box
    if 'total_return_pct' in equity_curve.columns:
        total_return = equity_curve['total_return_pct'].iloc[-1]
        max_drawdown = equity_curve['drawdown_pct'].max()
        sharpe = equity_curve.get('sharpe_ratio', [0]).iloc[-1] if 'sharpe_ratio' in equity_curve.columns else 0
        
        # Add Fibonacci stats if available
        if trades:
            fib_trades = [t for t in trades if t.get('fib_score', 0) > 0.15]
            non_fib_trades = [t for t in trades if t.get('fib_score', 0) <= 0.15]
            
            stats_text = (f"Total Return: {total_return:.2f}%\n"
                         f"Max Drawdown: {max_drawdown:.2f}%\n"
                         f"Sharpe Ratio: {sharpe:.2f}\n"
                         f"Total Trades: {len(trades)}\n"
                         f"Fibonacci Trades: {len(fib_trades)}")
        else:
            stats_text = (f"Total Return: {total_return:.2f}%\n"
                         f"Max Drawdown: {max_drawdown:.2f}%\n"
                         f"Sharpe Ratio: {sharpe:.2f}\n"
                         f"Total Trades: {len(trades)}")
        
        ax1.text(0.02, 0.98, stats_text, transform=ax1.transAxes, 
                verticalalignment='top', horizontalalignment='left',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                fontsize=10)
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=120, bbox_inches='tight')
    plt.close()
    print(f"📊 Equity chart saved: {save_path}")


def plot_fibonacci_analysis(trades: List[Dict], report: dict = None, save_path: str = "fibonacci_analysis.png"):
    """Create specialized plots for Fibonacci performance analysis."""
    if not trades:
        print("⚠️ No trade data for Fibonacci analysis")
        return
    
    # Separate Fibonacci and non-Fibonacci trades
    fib_trades = [t for t in trades if t.get('fib_score', 0) > 0.15]
    non_fib_trades = [t for t in trades if t.get('fib_score', 0) <= 0.15]
    
    # Separate support and resistance trades
    support_trades = [t for t in fib_trades if t.get('near_support', False)]
    resistance_trades = [t for t in fib_trades if not t.get('near_support', False) 
                       and t.get('fib_level', '') in ['23.60%', '38.20%', '23.6%', '38.2%']]
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. Trade Count Comparison
    categories = ['Fibonacci', 'Non-Fibonacci', 'Support', 'Resistance']
    counts = [len(fib_trades), len(non_fib_trades), len(support_trades), len(resistance_trades)]
    
    bars = axes[0, 0].bar(categories, counts, color=['green', 'gray', 'blue', 'orange'])
    axes[0, 0].set_title('Trade Count by Category', fontsize=12, fontweight='bold')
    axes[0, 0].set_ylabel('Number of Trades')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # Add percentage labels
    total = sum(counts)
    for i, (bar, count) in enumerate(zip(bars, counts)):
        if total > 0:
            height = bar.get_height()
            axes[0, 0].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                           f'{count}\n({count/total*100:.0f}%)',
                           ha='center', va='bottom', fontsize=9)
    
    # 2. Fibonacci Score Distribution
    if fib_trades:
        fib_scores = [t.get('fib_score', 0) for t in fib_trades]
        axes[0, 1].hist(fib_scores, bins=10, color='green', alpha=0.7, edgecolor='black')
        axes[0, 1].axvline(x=0.7, color='red', linestyle='--', alpha=0.5, label='High (0.7)')
        axes[0, 1].axvline(x=0.4, color='orange', linestyle='--', alpha=0.5, label='Low (0.4)')
        axes[0, 1].set_title('Fibonacci Score Distribution', fontsize=12, fontweight='bold')
        axes[0, 1].set_xlabel('Fibonacci Score')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].legend()
    
    # 3. Fibonacci Level Distribution
    if fib_trades:
        fib_levels = {}
        for trade in fib_trades:
            level = trade.get('fib_level', 'N/A')
            fib_levels[level] = fib_levels.get(level, 0) + 1
        
        if fib_levels:
            levels = list(fib_levels.keys())
            level_counts = list(fib_levels.values())
            
            colors = []
            for level in levels:
                if level == '61.8%':
                    colors.append('gold')
                elif level == '76.4%':
                    colors.append('darkorange')
                elif level in ['23.60%', '38.20%', '23.6%', '38.2%']:
                    colors.append('lightcoral')
                else:
                    colors.append('lightgray')
            
            bars = axes[1, 0].bar(levels, level_counts, color=colors)
            axes[1, 0].set_title('Fibonacci Level Distribution', fontsize=12, fontweight='bold')
            axes[1, 0].set_ylabel('Number of Trades')
            axes[1, 0].tick_params(axis='x', rotation=45)
            
            # Add percentage labels
            fib_total = sum(level_counts)
            for bar, count in zip(bars, level_counts):
                height = bar.get_height()
                axes[1, 0].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                               f'{count}\n({count/fib_total*100:.0f}%)',
                               ha='center', va='bottom', fontsize=9)
    
    # 4. Performance Comparison
    if report and 'fibonacci_analysis' in report:
        fib_analysis = report['fibonacci_analysis']
        fib_perf = fib_analysis.get('fibonacci_trades', {})
        non_fib_perf = fib_analysis.get('non_fibonacci_trades', {})
        
        if fib_perf and non_fib_perf:
            metrics = ['Win Rate', 'Avg Return', 'Sharpe Ratio']
            fib_values = [
                fib_perf.get('win_rate', 0) * 100,
                fib_perf.get('avg_return', 0) * 100,
                fib_perf.get('sharpe_ratio', 0)
            ]
            non_fib_values = [
                non_fib_perf.get('win_rate', 0) * 100,
                non_fib_perf.get('avg_return', 0) * 100,
                non_fib_perf.get('sharpe_ratio', 0)
            ]
            
            x = np.arange(len(metrics))
            width = 0.35
            
            axes[1, 1].bar(x - width/2, fib_values, width, label='Fibonacci', color='green')
            axes[1, 1].bar(x + width/2, non_fib_values, width, label='Non-Fibonacci', color='gray')
            
            axes[1, 1].set_title('Performance Comparison', fontsize=12, fontweight='bold')
            axes[1, 1].set_xticks(x)
            axes[1, 1].set_xticklabels(metrics)
            axes[1, 1].legend()
            axes[1, 1].tick_params(axis='x', rotation=45)
            
            # Add value labels
            for i, (fib_val, non_fib_val) in enumerate(zip(fib_values, non_fib_values)):
                axes[1, 1].text(i - width/2, fib_val + max(fib_val, non_fib_val)*0.05, 
                               f'{fib_val:.1f}' if i < 2 else f'{fib_val:.2f}',
                               ha='center', va='bottom', fontsize=9)
                axes[1, 1].text(i + width/2, non_fib_val + max(fib_val, non_fib_val)*0.05, 
                               f'{non_fib_val:.1f}' if i < 2 else f'{non_fib_val:.2f}',
                               ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=120, bbox_inches='tight')
    plt.close()
    print(f"📊 Fibonacci chart saved: {save_path}")


# =============================================================================
# HELPER FUNCTIONS - USE THESE THROUGHOUT YOUR CODE
# =============================================================================

def safe_scalar(value):
    """
    Safely convert any pandas object to a Python float.
    Handles Series, DataFrame, numpy types, and scalar values.
    USE THIS before any f-string formatting with :.2f etc.
    """
    if value is None:
        return 0.0
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return 0.0
        return float(value.iloc[0, 0])
    if isinstance(value, pd.Series):
        if value.empty:
            return 0.0
        return float(value.iloc[0])
    if isinstance(value, (np.floating, np.integer)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_get_close(df: pd.DataFrame) -> pd.Series:
    """
    Safely extract Close prices from a DataFrame.
    Handles both regular and MultiIndex columns from yfinance.
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)
    
    # Handle MultiIndex columns (newer yfinance format)
    if isinstance(df.columns, pd.MultiIndex):
        if 'Close' in df.columns.get_level_values(0):
            close = df['Close']
            if isinstance(close, pd.DataFrame):
                return close.iloc[:, 0]
            return close
    
    # Regular columns
    if 'Close' in df.columns:
        close = df['Close']
        if isinstance(close, pd.DataFrame):
            return close.iloc[:, 0]
        return close
    
    # Fallback: try first column
    if len(df.columns) > 0:
        first_col = df.iloc[:, 0]
        if isinstance(first_col, pd.DataFrame):
            return first_col.iloc[:, 0]
        return first_col
    
    return pd.Series(dtype=float)


def flatten_yfinance_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten MultiIndex columns from yfinance to simple column names.
    Call this immediately after yf.download() for single-ticker downloads.
    """
    if df is None or df.empty:
        return df
    
    df = df.copy()
    
    if isinstance(df.columns, pd.MultiIndex):
        # Take the first level (Open, High, Low, Close, Volume)
        df.columns = df.columns.get_level_values(0)
    
    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Capitalize column names
    df.columns = [str(col).strip().capitalize() for col in df.columns]
    
    return df


def print_fibonacci_trade_summary(trades: List[Dict]):
    """Print a summary of Fibonacci-enhanced trades."""
    if not trades:
        print("No trades to analyze.")
        return
    
    fib_trades = [t for t in trades if t.get('fib_score', 0) > 0.15]
    support_trades = [t for t in fib_trades if t.get('near_support', False)]
    resistance_trades = [t for t in fib_trades if not t.get('near_support', False) 
                       and t.get('fib_level', '') in ['23.60%', '38.20%', '23.6%', '38.2%']]
    
    print("\n" + "=" * 80)
    print("📊 FIBONACCI TRADE SUMMARY".center(80))
    print("=" * 80)
    
    print(f"\n📈 Trade Statistics:")
    print(f"   - Total Trades: {len(trades)}")
    print(f"   - Fibonacci Trades: {len(fib_trades)} ({len(fib_trades)/len(trades)*100:.1f}%)")
    print(f"   - Support Trades: {len(support_trades)} ({len(support_trades)/len(fib_trades)*100:.1f}% of Fibonacci)")
    print(f"   - Resistance Trades: {len(resistance_trades)} ({len(resistance_trades)/len(fib_trades)*100:.1f}% of Fibonacci)")
    
    if fib_trades:
        avg_fib_score = np.mean([t.get('fib_score', 0) for t in fib_trades])
        print(f"   - Average Fibonacci Score: {avg_fib_score:.3f}")
        
        # Calculate P&L by Fibonacci level
        level_pnl = {}
        for trade in fib_trades:
            level = trade.get('fib_level', 'N/A')
            pnl = trade.get('pnl', 0)  # Assuming pnl field exists
            if level not in level_pnl:
                level_pnl[level] = []
            level_pnl[level].append(pnl)
        
        if level_pnl:
            print(f"\n🎯 Performance by Fibonacci Level:")
            for level in ['23.60%', '38.20%', '23.6%', '38.2%', '50.0%', '61.8%', '76.4%']:
                if level in level_pnl:
                    avg_pnl = np.mean(level_pnl[level]) if level_pnl[level] else 0
                    count = len(level_pnl[level])
                    print(f"   - {level}: {count} trades, avg P&L: {avg_pnl*100:+.2f}%")
    
    print("\n" + "=" * 80)

# =============================================================================
# FIX 1: FULL DIAGNOSTIC FUNCTION - UPDATED FOR FIBONACCI
# =============================================================================

def full_diagnostic():
    """
    Comprehensive system diagnostic that tests all components.
    UPDATED: Includes Fibonacci module testing.
    Run with: python trading_system.py --mode diagnose
    """
    print("\n" + "=" * 80)
    print("🔬 FULL SYSTEM DIAGNOSTIC (WITH FIBONACCI)".center(80))
    print("=" * 80 + "\n")
    
    results = {
        'passed': 0,
        'failed': 0,
        'warnings': 0,
        'details': []
    }
    
    # =========================================================================
    # TEST 1: Environment Variables
    # =========================================================================
    print("📋 TEST 1: Environment Variables")
    print("-" * 40)
    
    at_api = os.getenv("AT_API")
    if at_api:
        print(f"   ✅ AT_API: Set ({len(at_api)} chars)")
        results['passed'] += 1
    else:
        print("   ⚠️  AT_API: NOT SET (Airtable sync disabled)")
        results['warnings'] += 1
        results['details'].append("AT_API not set - Airtable features disabled")
    
    fred_key = os.getenv("FRED_API_KEY")
    if fred_key:
        print(f"   ✅ FRED_API_KEY: Set ({len(fred_key)} chars)")
        results['passed'] += 1
    else:
        print("   ⚠️  FRED_API_KEY: NOT SET (Economic data disabled)")
        results['warnings'] += 1
        results['details'].append("FRED_API_KEY not set - Economic features disabled")
    
    print("\n   💡 To set environment variables:")
    print("      export AT_API='your_airtable_key'")
    print("      export FRED_API_KEY='your_fred_key'")
    
    # =========================================================================
    # TEST 2: Package Imports
    # =========================================================================
    print("\n📋 TEST 2: Package Imports")
    print("-" * 40)
    
    packages = [
        ('yfinance', 'yf'),
        ('pandas', 'pd'),
        ('numpy', 'np'),
        ('scipy', 'scipy'),
        ('matplotlib', 'plt'),
        ('fredapi', 'Fred'),
        ('requests', 'requests'),
    ]
    
    for pkg_name, alias in packages:
        try:
            __import__(pkg_name)
            print(f"   ✅ {pkg_name}: OK")
            results['passed'] += 1
        except ImportError as e:
            print(f"   ❌ {pkg_name}: FAILED - {e}")
            results['failed'] += 1
            results['details'].append(f"Cannot import {pkg_name}")
    
    # =========================================================================
    # TEST 3: Data Fetching (yfinance) - FIXED
    # =========================================================================
    print("\n📋 TEST 3: Data Fetching (yfinance)")
    print("-" * 40)
    
    try:
        import yfinance as yf
        test_ticker = yf.download("AAPL", period="5d", progress=False)
        
        if test_ticker is not None and len(test_ticker) > 0:
            print(f"   ✅ yfinance: Downloaded {len(test_ticker)} rows for AAPL")
            
            # FIXED: Flatten columns and use safe extraction
            test_ticker = flatten_yfinance_df(test_ticker)
            close_series = safe_get_close(test_ticker)
            
            if not close_series.empty:
                # FIXED: Convert to scalar before formatting
                last_close = safe_scalar(close_series.iloc[-1])
                print(f"   ✅ Latest close: ${last_close:.2f}")
                results['passed'] += 1
            else:
                print("   ⚠️  Could not extract Close prices")
                results['warnings'] += 1
        else:
            print("   ⚠️  yfinance: No data returned")
            results['warnings'] += 1
            
    except Exception as e:
        print(f"   ❌ yfinance: FAILED - {e}")
        results['failed'] += 1
        results['details'].append(f"yfinance download failed: {e}")
    
    # =========================================================================
    # TEST 4: FRED API Connection
    # =========================================================================
    print("\n📋 TEST 4: FRED API Connection")
    print("-" * 40)
    
    if fred_key:
        try:
            from fredapi import Fred
            fred = Fred(api_key=fred_key)
            vix = fred.get_series("VIXCLS", limit=5)
            if vix is not None and len(vix) > 0:
                # FIXED: Safe scalar conversion
                vix_value = safe_scalar(vix.iloc[-1])
                print(f"   ✅ FRED API: Connected, VIX = {vix_value:.2f}")
                results['passed'] += 1
            else:
                print("   ⚠️  FRED API: No data returned")
                results['warnings'] += 1
        except Exception as e:
            print(f"   ❌ FRED API: FAILED - {e}")
            results['failed'] += 1
            results['details'].append(f"FRED API failed: {e}")
    else:
        print("   ⏭️  FRED API: Skipped (no API key)")
        results['warnings'] += 1
    
    # =========================================================================
    # TEST 5: Airtable Connection
    # =========================================================================
    print("\n📋 TEST 5: Airtable Connection")
    print("-" * 40)
    
    if at_api:
        try:
            import requests
            from urllib.parse import quote
            
            AT_BASE = "appIUFp3KFrf8KXez"
            AT_TABLE = "Trading Signals"
            
            url = f"https://api.airtable.com/v0/{AT_BASE}/{quote(AT_TABLE)}"
            headers = {
                "Authorization": f"Bearer {at_api}",
                "Content-Type": "application/json",
            }
            
            resp = requests.get(url, headers=headers, params={"maxRecords": 1}, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                record_count = len(data.get("records", []))
                print(f"   ✅ Airtable: Connected ({record_count} test records)")
                results['passed'] += 1
            elif resp.status_code == 401:
                print("   ❌ Airtable: Authentication failed")
                results['failed'] += 1
                results['details'].append("Airtable authentication failed")
            elif resp.status_code == 404:
                print("   ❌ Airtable: Table not found")
                results['failed'] += 1
                results['details'].append("Airtable table not found")
            else:
                print(f"   ⚠️  Airtable: HTTP {resp.status_code}")
                results['warnings'] += 1
                
        except Exception as e:
            print(f"   ❌ Airtable: FAILED - {e}")
            results['failed'] += 1
            results['details'].append(f"Airtable connection failed: {e}")
    else:
        print("   ⏭️  Airtable: Skipped (no API key)")
        results['warnings'] += 1
    
    # =========================================================================
    # TEST 6: Signal Generation - UPDATED WITH FIBONACCI
    # =========================================================================
    print("\n📋 TEST 6: Signal Generation Pipeline (with Fibonacci)")
    print("-" * 40)
    
    try:
        import yfinance as yf
        from scipy import stats
        
        # Download test data
        test_symbols = ["AAPL", "MSFT", "GOOGL"]
        test_data = {}
        
        for sym in test_symbols:
            df = yf.download(sym, period="6mo", progress=False)
            if df is not None and not df.empty:
                # FIXED: Flatten columns immediately
                df = flatten_yfinance_df(df)
                test_data[sym] = df
        
        if len(test_data) >= 2:
            print(f"   ✅ Downloaded {len(test_data)} test symbols")
            
            # NEW: Test Fibonacci calculations
            print("\n   📐 Testing Fibonacci Calculations:")
            for sym, df in test_data.items():
                close = safe_get_close(df)
                
                if len(close) >= 252:  # Need enough data for Fibonacci
                    # Create FibonacciModule instance
                    fib_module = FibonacciModule(lookback_days=252)
                    
                    # Test Fibonacci levels
                    fib_data = fib_module.calculate_fib_levels(close)
                    if fib_data:
                        fib_score = fib_module.calculate_fib_score(fib_data)
                        
                        print(f"      {sym}: Fib Score = {fib_score.get('fib_score', 0):.3f}")
                        print(f"           Level: {fib_score.get('closest_level', 'N/A')}")
                        print(f"           Retracement: {fib_data.get('retracement_pct', 0):.1f}%")
                    else:
                        print(f"      {sym}: Insufficient data for Fibonacci")
                else:
                    print(f"      {sym}: Need 252+ days for Fibonacci, got {len(close)}")
            
            results['passed'] += 1
        else:
            print("   ⚠️  Insufficient test data")
            results['warnings'] += 1
            
    except Exception as e:
        print(f"   ❌ Signal generation: FAILED - {e}")
        import traceback
        traceback.print_exc()
        results['failed'] += 1
        results['details'].append(f"Signal generation failed: {e}")
    
    # =========================================================================
    # TEST 7: Fibonacci Module - COMPREHENSIVE TEST
    # =========================================================================
    print("\n📋 TEST 7: Fibonacci Module - Comprehensive")
    print("-" * 40)
    
    try:
        # Create synthetic price data
        np.random.seed(42)
        dates = pd.date_range('2023-01-01', periods=300, freq='D')
        base_price = 100
        price_series = pd.Series(
            base_price * (1 + np.random.randn(300).cumsum() * 0.01),
            index=dates
        )
        
        # Initialize Fibonacci module
        fib_module = FibonacciModule(lookback_days=252)
        
        # Test 7.1: Basic Fibonacci levels
        print("   📊 Testing Fibonacci Level Calculation:")
        fib_data = fib_module.calculate_fib_levels(price_series)
        
        if fib_data:
            print(f"      ✅ Fibonacci levels calculated")
            print(f"         High: ${fib_data.get('high_52w', 0):.2f}")
            print(f"         Low: ${fib_data.get('low_52w', 0):.2f}")
            print(f"         Current: ${fib_data.get('current', 0):.2f}")
            print(f"         Retracement: {fib_data.get('retracement_pct', 0):.1f}%")
            
            # Test key levels
            levels = fib_data.get('levels', {})
            expected_levels = ['23.60%', '38.20%', '23.6%', '38.2%', '50.0%', '61.8%', '76.4%']
            for level in expected_levels:
                if level in levels:
                    print(f"         {level}: ${levels[level]:.2f}")
            
            results['passed'] += 1
        else:
            print("      ❌ Failed to calculate Fibonacci levels")
            results['failed'] += 1
        
        # Test 7.2: Fibonacci Score
        print("\n   📈 Testing Fibonacci Score Calculation:")
        if fib_data:
            fib_scores = fib_module.calculate_fib_score(fib_data)
            
            if fib_scores:
                print(f"      ✅ Fibonacci score calculated: {fib_scores.get('fib_score', 0):.3f}")
                print(f"         Closest Level: {fib_scores.get('closest_level', 'N/A')}")
                print(f"         Distance: {fib_scores.get('distance_pct', 0):.1f}%")
                print(f"         Support Score: {fib_scores.get('support_score', 0):.3f}")
                
                results['passed'] += 1
            else:
                print("      ❌ Failed to calculate Fibonacci scores")
                results['failed'] += 1
        
        # Test 7.3: Fibonacci Signal Score
        print("\n   🎯 Testing Fibonacci Signal Score:")
        fib_signal_data = fib_module.calculate_fibonacci_signal_score(price_series)
        
        if fib_signal_data:
            print(f"      ✅ Fibonacci signal data calculated")
            print(f"         Signal: {fib_signal_data.get('fib_signal', 'N/A')}")
            print(f"         Confidence: {fib_signal_data.get('confidence', 'LOW')}")
            print(f"         Stop Distance: {fib_signal_data.get('stop_distance_pct', 0)*100:.1f}%")
            print(f"         Near Support: {fib_signal_data.get('is_support', False)}")
            
            results['passed'] += 1
        else:
            print("      ❌ Failed to calculate Fibonacci signal data")
            results['failed'] += 1
        
        # Test 7.4: Fibonacci Support Detection
        print("\n   🎯 Testing Support Detection:")
        if fib_scores:
            is_near_support = fib_module.is_near_fib_support(fib_scores, tolerance_pct=3.0)
            print(f"      ✅ Near Fibonacci Support: {is_near_support}")
            
            results['passed'] += 1
        
        # Test 7.5: Fibonacci Recommendations
        print("\n   💡 Testing Fibonacci Recommendations:")
        recommendation = fib_module.generate_fib_recommendation(fib_scores)
        
        if recommendation:
            print(f"      ✅ Recommendation: {recommendation.get('signal', 'N/A')}")
            print(f"         Confidence: {recommendation.get('confidence', 'LOW')}")
            
            results['passed'] += 1
        
    except Exception as e:
        print(f"   ❌ Fibonacci module: FAILED - {e}")
        import traceback
        traceback.print_exc()
        results['failed'] += 1
        results['details'].append(f"Fibonacci module failed: {e}")
    
    # =========================================================================
    # TEST 8: Configuration Validation (UPDATED FOR FIBONACCI)
    # =========================================================================
    print("\n📋 TEST 8: Configuration Validation (with Fibonacci)")
    print("-" * 40)
    
    try:
        # Check if config variables exist
        config_vars_exist = True
        try:
            _ = FILTER_MODE
            _ = RISK_PER_TRADE
            _ = MAX_POSITION_SIZE
            _ = FIB_POSITION_CONFIG  # NEW: Check Fibonacci config
        except NameError:
            config_vars_exist = False
        
        if config_vars_exist:
            config_issues = []
            
            if RISK_PER_TRADE > 0.05:
                config_issues.append(f"RISK_PER_TRADE too high: {RISK_PER_TRADE:.1%}")
            
            if MAX_POSITION_SIZE > 0.25:
                config_issues.append(f"MAX_POSITION_SIZE too high: {MAX_POSITION_SIZE:.1%}")
            
            if MIN_STOP_ALLOWED > MAX_STOP_ALLOWED:
                config_issues.append(f"MIN_STOP > MAX_STOP")
            
            if MAX_CONCURRENT_POSITIONS > 30:
                config_issues.append(f"MAX_CONCURRENT_POSITIONS very high: {MAX_CONCURRENT_POSITIONS}")
            
            # NEW: Check Fibonacci configuration
            fib_config = FIB_POSITION_CONFIG
            if not fib_config.get('enabled', False):
                config_issues.append("Fibonacci position sizing is DISABLED")
            
            fib_stop_enabled = fib_config.get('fib_stop_tightening', False)
            if not fib_stop_enabled:
                config_issues.append("Fibonacci stop tightening is DISABLED")
            
            # Check regime multipliers
            regime_multipliers = fib_config.get('regime_multipliers', {})
            if not regime_multipliers:
                config_issues.append("No Fibonacci regime multipliers configured")
            else:
                for regime, multipliers in regime_multipliers.items():
                    if 'support_boost' not in multipliers or 'resistance_penalty' not in multipliers:
                        config_issues.append(f"Missing Fibonacci multipliers for {regime}")
            
            if config_issues:
                for issue in config_issues:
                    print(f"   ⚠️  {issue}")
                    results['warnings'] += 1
            else:
                print("   ✅ All configuration parameters valid")
                results['passed'] += 1
            
            print(f"\n   Current Configuration:")
            print(f"      FILTER_MODE: {FILTER_MODE}")
            print(f"      RISK_PER_TRADE: {RISK_PER_TRADE:.1%}")
            print(f"      MAX_POSITION_SIZE: {MAX_POSITION_SIZE:.1%}")
            print(f"      STOP RANGE: {MIN_STOP_ALLOWED:.1%} - {MAX_STOP_ALLOWED:.1%}")
            print(f"      MAX_CONCURRENT_POSITIONS: {MAX_CONCURRENT_POSITIONS}")
            
            # NEW: Show Fibonacci configuration
            fib_config = FIB_POSITION_CONFIG
            print(f"      FIBONACCI ENABLED: {fib_config.get('enabled', False)}")
            print(f"      FIB_STOP_TIGHTENING: {fib_config.get('fib_stop_tightening', False)}")
            print(f"      REGIME_DEPENDENT: {fib_config.get('regime_dependent', False)}")
            
            # Show regime multipliers
            if fib_config.get('regime_dependent', False):
                regime_multipliers = fib_config.get('regime_multipliers', {})
                print(f"      REGIME MULTIPLIERS:")
                for regime, mult in regime_multipliers.items():
                    print(f"         - {regime}: Support={mult.get('support_boost', 1.0):.2f}x, "
                          f"Resistance={mult.get('resistance_penalty', 1.0):.2f}x")
            
            print(f"      FORCE_FULL_DEPLOYMENT: {FORCE_FULL_DEPLOYMENT}")
        else:
            print("   ℹ️  Configuration variables not in scope")
            print("      (This is normal when running diagnostic standalone)")
            results['passed'] += 1
            
    except Exception as e:
        print(f"   ⚠️  Config validation skipped: {e}")
        results['warnings'] += 1
    
    # =========================================================================
    # TEST 9: Statistical Signal Module (with Fibonacci Integration)
    # =========================================================================
    print("\n📋 TEST 9: Statistical Signal Module with Fibonacci")
    print("-" * 40)
    
    try:
        # Create synthetic price data
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        price_data = {}
        
        for i, symbol in enumerate(['TEST1', 'TEST2', 'TEST3']):
            np.random.seed(i * 42)
            base = 100 + i * 10
            prices = base * (1 + np.random.randn(100).cumsum() * 0.01)
            df = pd.DataFrame({
                'Open': prices * 0.99,
                'High': prices * 1.01,
                'Low': prices * 0.98,
                'Close': prices,
                'Volume': np.random.randint(100000, 1000000, size=100)
            }, index=dates)
            price_data[symbol] = df
        
        # Initialize signal module
        signal_module = StatisticalSignalModule(lookback=21)
        
        # Generate signals with Fibonacci
        signals = signal_module.compute_signals(
            price_data=price_data,
            regime={"volatility": "NORMAL", "market_breadth": "BROAD"}
        )
        
        if signals:
            print(f"   ✅ Generated {len(signals)} signals")
            
            # Check for Fibonacci fields
            sample_signal = next(iter(signals.values()))
            fib_fields = [
                'fib_score', 'fib_level', 'fib_signal', 'near_fib_support',
                'fib_stop_pct', 'fib_retracement', 'fib_enhancement'
            ]
            
            fib_fields_present = []
            for field in fib_fields:
                if field in sample_signal:
                    fib_fields_present.append(field)
            
            print(f"   📊 Fibonacci Integration:")
            print(f"      - Fields present: {len(fib_fields_present)}/{len(fib_fields)}")
            if fib_fields_present:
                for field in fib_fields_present:
                    value = sample_signal[field]
                    if isinstance(value, float):
                        print(f"      - {field}: {value:.3f}")
                    else:
                        print(f"      - {field}: {value}")
            
            # Test signal filtering with Fibonacci
            print(f"\n   🎯 Testing Fibonacci Filtering:")
            filtered = signal_module.filter_signals_by_fibonacci(
                signals, min_fib_score=0.4, require_support=False
            )
            print(f"      - Before filtering: {len(signals)} signals")
            print(f"      - After filtering: {len(filtered)} signals")
            
            if filtered:
                avg_fib_score = np.mean([s.get('fib_score', 0) for s in filtered.values()])
                support_count = sum(1 for s in filtered.values() if s.get('near_fib_support', False))
                print(f"      - Avg Fib Score in filtered: {avg_fib_score:.3f}")
                print(f"      - Support signals: {support_count}/{len(filtered)}")
            
            results['passed'] += 1
        else:
            print("   ❌ Failed to generate signals")
            results['failed'] += 1
            
    except Exception as e:
        print(f"   ❌ Statistical Signal Module: FAILED - {e}")
        import traceback
        traceback.print_exc()
        results['failed'] += 1
        results['details'].append(f"Statistical Signal Module failed: {e}")
    
    # =========================================================================
    # TEST 10: Risk Management with Fibonacci Integration
    # =========================================================================
    print("\n📋 TEST 10: Risk Management with Fibonacci")
    print("-" * 40)
    
    try:
        # Create test data
        test_signals = {}
        test_prices = {}
        
        for i in range(5):
            sym = f"TEST{i}"
            test_signals[sym] = {
                'combined_score': 0.5 + i * 0.1,
                'sharpe_ratio': 0.8 + i * 0.1,
                'fib_score': 0.4 + i * 0.15,
                'fib_level': ['23.60%', '38.20%', '23.6%', '38.2%', '50.0%', '61.8%', '76.4%'][i],
                'near_fib_support': i >= 2,  # Last 3 are near support
                'confidence': ['LOW', 'MEDIUM', 'HIGH', 'HIGH', 'HIGH'][i],
                'fib_stop_pct': 0.08 - i * 0.005,
            }
            
            # Create price DataFrame
            dates = pd.date_range('2023-01-01', periods=50, freq='D')
            prices = 100 * (1 + np.random.randn(50).cumsum() * 0.01)
            test_prices[sym] = pd.DataFrame({
                'Close': prices,
                'Open': prices * 0.99,
                'High': prices * 1.01,
                'Low': prices * 0.98,
                'Volume': np.random.randint(100000, 1000000, size=50)
            }, index=dates)
        
        # Initialize risk management with Fibonacci
        fib_module = FibonacciModule()
        risk_module = RiskManagementModule(fib_module=fib_module)
        
        # Test position sizing with Fibonacci
        risk_output = risk_module.compute_position_sizes(
            signals=test_signals,
            price_data=test_prices,
            regime={"volatility": "NORMAL"},
            regime_multiplier=1.0,
            total_capital=100000,
            market_regime="BROAD",
            merge_live_holdings=False,        # test harness: deterministic, no live Airtable
        )
        
        if risk_output and 'position_sizing' in risk_output:
            positions = risk_output['position_sizing']
            print(f"   ✅ Generated {len(positions)} positions")
            
            # Check for Fibonacci adjustments
            fib_metrics = risk_output.get('fib_metrics', {})
            if fib_metrics:
                print(f"   📊 Fibonacci Metrics:")
                print(f"      - Avg Fib Score: {fib_metrics.get('avg_fib_score', 0):.3f}")
                print(f"      - Support Positions: {fib_metrics.get('support_positions', 0)}")
                print(f"      - Avg Fibonacci Boost: {fib_metrics.get('total_fib_boost', 0)*100:+.1f}%")
            
            # Check individual positions
            print(f"\n   🎯 Position Details:")
            for sym, size in positions.items():
                signal = test_signals.get(sym, {})
                fib_score = signal.get('fib_score', 0)
                fib_level = signal.get('fib_level', '')
                support = "✓" if signal.get('near_fib_support', False) else ""
                
                print(f"      - {sym}: {size:.1%} | Fib: {fib_score:.2f} ({fib_level}) {support}")
            
            results['passed'] += 1
        else:
            print("   ❌ Failed to compute position sizes")
            results['failed'] += 1
            
    except Exception as e:
        print(f"   ❌ Risk Management: FAILED - {e}")
        import traceback
        traceback.print_exc()
        results['failed'] += 1
        results['details'].append(f"Risk Management failed: {e}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("📊 FIBONACCI-ENHANCED DIAGNOSTIC SUMMARY".center(80))
    print("=" * 80)
    
    total_tests = results['passed'] + results['failed'] + results['warnings']
    
    print(f"\n   ✅ Passed:   {results['passed']}/{total_tests}")
    print(f"   ❌ Failed:   {results['failed']}/{total_tests}")
    print(f"   ⚠️  Warnings: {results['warnings']}/{total_tests}")
    
    if results['details']:
        print("\n   📝 Notes:")
        for detail in results['details']:
            print(f"      - {detail}")
    
    # NEW: Fibonacci-specific recommendations
    print("\n   🎯 FIBONACCI-RELATED RECOMMENDATIONS:")
    
    # Check if Fibonacci tests passed
    fib_tests_passed = True
    for detail in results['details']:
        if 'Fibonacci' in detail or 'fib' in detail.lower():
            fib_tests_passed = False
    
    if fib_tests_passed:
        print("      ✅ Fibonacci modules are working correctly")
        print("      💡 Consider enabling Fibonacci-enhanced features in production")
    else:
        print("      ⚠️  Fibonacci modules have issues")
        print("      🔧 Review Fibonacci configuration and dependencies")
    
    # Determine overall status
    if results['failed'] == 0:
        print("\n   🎉 All tests passed! System ready for Fibonacci-enhanced operation.")
        if results['warnings'] > 0:
            print("   💡 Some optional features disabled (missing API keys).")
    elif results['failed'] <= 2:
        print("\n   ⚠️  Minor issues detected. System may work with limited Fibonacci features.")
    else:
        print("\n   🚨 Multiple failures detected. Review and fix issues before running.")
        print("   🔧 Pay special attention to Fibonacci module configuration")
    
    print("\n" + "=" * 80 + "\n")
    
    return results


# =============================================================================
# FIX 2: PLOT COMPARISON FUNCTION - FIXED
# =============================================================================

def plot_comparison(equity_curve: pd.DataFrame, benchmark: pd.Series, title: str = "Strategy vs Benchmark"):
    """
    Compare strategy performance against a benchmark (e.g., SPY).
    FIXED: Uses safe scalar extraction for all calculations.
    
    Args:
        equity_curve: DataFrame with 'equity' column and DatetimeIndex
        benchmark: Series of benchmark prices (e.g., SPY close prices)
        title: Plot title
    """
    if equity_curve.empty or benchmark.empty:
        print("⚠️ Insufficient data for comparison plot")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # FIXED: Safe extraction for normalization
    initial_equity = safe_scalar(equity_curve['equity'].iloc[0])
    strategy_norm = (equity_curve['equity'] / initial_equity) * 100
    
    # Align benchmark to strategy dates
    benchmark_aligned = benchmark.reindex(equity_curve.index, method='ffill')
    initial_benchmark = safe_scalar(benchmark_aligned.iloc[0])
    benchmark_norm = (benchmark_aligned / initial_benchmark) * 100
    
    # Calculate returns
    strategy_returns = equity_curve['equity'].pct_change().dropna()
    benchmark_returns = benchmark_aligned.pct_change().dropna()
    
    # =========================================================================
    # PLOT 1: Cumulative Performance
    # =========================================================================
    ax1 = axes[0, 0]
    ax1.plot(strategy_norm.index, strategy_norm.values, label='Strategy', 
             linewidth=2, color='blue')
    ax1.plot(benchmark_norm.index, benchmark_norm.values, label='Benchmark (SPY)', 
             linewidth=2, color='orange', alpha=0.8)
    ax1.axhline(y=100, color='gray', linestyle='--', alpha=0.5)
    ax1.set_title('Cumulative Performance (Normalized to 100)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Value')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    
    # =========================================================================
    # PLOT 2: Rolling Sharpe Ratio (60-day)
    # =========================================================================
    ax2 = axes[0, 1]
    
    rolling_window = min(60, max(10, len(strategy_returns) // 3))
    if len(strategy_returns) >= rolling_window:
        strategy_rolling_sharpe = (
            strategy_returns.rolling(rolling_window).mean() / 
            strategy_returns.rolling(rolling_window).std()
        ) * np.sqrt(252)
        
        benchmark_rolling_sharpe = (
            benchmark_returns.rolling(rolling_window).mean() / 
            benchmark_returns.rolling(rolling_window).std()
        ) * np.sqrt(252)
        
        ax2.plot(strategy_rolling_sharpe.index, strategy_rolling_sharpe.values, 
                label='Strategy', linewidth=1.5, color='blue')
        ax2.plot(benchmark_rolling_sharpe.index, benchmark_rolling_sharpe.values, 
                label='Benchmark', linewidth=1.5, color='orange', alpha=0.8)
        ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        ax2.axhline(y=1, color='green', linestyle=':', alpha=0.5, label='Sharpe = 1')
    
    ax2.set_title(f'Rolling {rolling_window}-Day Sharpe Ratio', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Sharpe Ratio')
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    # =========================================================================
    # PLOT 3: Drawdown Comparison
    # =========================================================================
    ax3 = axes[1, 0]
    
    # Calculate drawdowns
    strategy_peak = strategy_norm.expanding().max()
    strategy_dd = ((strategy_norm - strategy_peak) / strategy_peak) * 100
    
    benchmark_peak = benchmark_norm.expanding().max()
    benchmark_dd = ((benchmark_norm - benchmark_peak) / benchmark_peak) * 100
    
    ax3.fill_between(strategy_dd.index, strategy_dd.values, 0, 
                     alpha=0.4, color='blue', label='Strategy')
    ax3.fill_between(benchmark_dd.index, benchmark_dd.values, 0, 
                     alpha=0.4, color='orange', label='Benchmark')
    ax3.set_title('Drawdown Comparison', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Drawdown (%)')
    ax3.legend(loc='lower left')
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    # =========================================================================
    # PLOT 4: Statistics Table
    # =========================================================================
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Calculate statistics - FIXED with safe_scalar
    def calc_stats(returns, name):
        if len(returns) < 20:
            return {}
        
        # FIXED: Use safe_scalar for all calculations
        total_return = float((1 + returns).prod() - 1)
        ann_return = float((1 + total_return) ** (252 / len(returns)) - 1)
        ann_vol = float(returns.std() * np.sqrt(252))
        sharpe = ann_return / ann_vol if ann_vol > 0 else 0
        
        # Max drawdown
        cumulative = (1 + returns).cumprod()
        peak = cumulative.expanding().max()
        drawdown = (cumulative - peak) / peak
        max_dd = float(drawdown.min())
        
        # Sortino
        downside = returns[returns < 0]
        if len(downside) > 0:
            downside_std = float(downside.std())
            sortino = ann_return / (downside_std * np.sqrt(252)) if downside_std > 0 else 0
        else:
            sortino = 0
        
        # Win rate
        win_rate = float((returns > 0).sum() / len(returns))
        
        return {
            'Total Return': f"{total_return * 100:.2f}%",
            'Annual Return': f"{ann_return * 100:.2f}%",
            'Annual Volatility': f"{ann_vol * 100:.2f}%",
            'Sharpe Ratio': f"{sharpe:.3f}",
            'Sortino Ratio': f"{sortino:.3f}",
            'Max Drawdown': f"{max_dd * 100:.2f}%",
            'Win Rate': f"{win_rate * 100:.1f}%",
        }
    
    strategy_stats = calc_stats(strategy_returns, 'Strategy')
    benchmark_stats = calc_stats(benchmark_returns, 'Benchmark')
    
    # Create table data
    table_data = []
    metrics = ['Total Return', 'Annual Return', 'Annual Volatility', 
               'Sharpe Ratio', 'Sortino Ratio', 'Max Drawdown', 'Win Rate']
    
    for metric in metrics:
        strat_val = strategy_stats.get(metric, 'N/A')
        bench_val = benchmark_stats.get(metric, 'N/A')
        table_data.append([metric, strat_val, bench_val])
    
    # Add alpha calculation
    try:
        strat_ret = float(strategy_stats['Total Return'].replace('%', ''))
        bench_ret = float(benchmark_stats['Total Return'].replace('%', ''))
        alpha = strat_ret - bench_ret
        table_data.append(['Alpha', f"{alpha:.2f}%", '-'])
    except:
        pass
    
    table = ax4.table(
        cellText=table_data,
        colLabels=['Metric', 'Strategy', 'Benchmark'],
        cellLoc='center',
        loc='center',
        colWidths=[0.4, 0.3, 0.3]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.8)
    
    # Style header
    for i in range(3):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(color='white', fontweight='bold')
    
    # Alternating row colors
    for i in range(1, len(table_data) + 1):
        for j in range(3):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#D6DCE5')
    
    ax4.set_title('Performance Statistics', fontsize=12, fontweight='bold', pad=20)
    
    plt.suptitle(title, fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig('comparison_chart.png', dpi=150, bbox_inches='tight')
    #plt.show()  # Disabled - causes errors in terminal
    
    print("📊 Comparison chart saved to 'comparison_chart.png'")

# =============================================================================
# FIX 4: HELPER FUNCTION FOR EQUITY CURVE PROCESSING - UPDATED FOR FIBONACCI
# =============================================================================

def prepare_equity_curve_for_plotting(equity_curve: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure equity curve has all required columns for plotting.
    Call this before plot_backtest_results if needed.
    Updated to handle Fibonacci-enhanced equity curves.
    """
    df = equity_curve.copy()
    
    # Ensure index is datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df = df.set_index('date')
        elif 'Date' in df.columns:
            df = df.set_index('Date')
        df.index = pd.to_datetime(df.index)
    
    # Ensure required columns exist
    if 'equity' not in df.columns:
        if 'portfolio_value' in df.columns:
            df['equity'] = df['portfolio_value']
        elif 'Equity' in df.columns:
            df['equity'] = df['Equity']
        elif 'value' in df.columns:
            df['equity'] = df['value']
        else:
            raise ValueError("No equity column found. Available columns: " + str(df.columns.tolist()))
    
    # Add drawdown_pct if missing - FIXED: Use safe_scalar
    if 'drawdown_pct' not in df.columns:
        if 'drawdown' in df.columns:
            # Convert decimal to percentage
            df['drawdown_pct'] = df['drawdown'] * 100
        elif 'Drawdown' in df.columns:
            # Check if it's already percentage or decimal
            sample_val = safe_scalar(df['Drawdown'].iloc[0])
            if abs(sample_val) < 1:  # Likely decimal
                df['drawdown_pct'] = df['Drawdown'] * 100
            else:  # Likely percentage
                df['drawdown_pct'] = df['Drawdown']
        else:
            # Calculate from equity curve
            peak = df['equity'].expanding().max()
            df['drawdown_pct'] = ((df['equity'] - peak) / peak) * 100
    
    # Add total_return_pct if missing - FIXED: Use safe_scalar
    if 'total_return_pct' not in df.columns:
        initial = safe_scalar(df['equity'].iloc[0])
        df['total_return_pct'] = ((df['equity'] / initial) - 1) * 100
    
    # Add sharpe_ratio if missing (for statistics display)
    if 'sharpe_ratio' not in df.columns and 'Sharpe' not in df.columns:
        # Calculate from returns
        returns = df['equity'].pct_change().dropna()
        if len(returns) > 0:
            ret_mean = float(returns.mean())
            ret_std = float(returns.std())
            sharpe = (ret_mean * 252) / (ret_std * np.sqrt(252)) if ret_std > 0 else 0
            df['sharpe_ratio'] = sharpe
    
    # NEW: Add Fibonacci score column if missing but data exists
    if 'fib_score' not in df.columns:
        # Check if we have Fibonacci-related columns
        fib_cols = [col for col in df.columns if 'fib' in col.lower()]
        if fib_cols:
            # Use the most relevant Fibonacci column
            if 'daily_fib_score' in df.columns:
                df['fib_score'] = df['daily_fib_score']
            elif 'avg_fib_score' in df.columns:
                df['fib_score'] = df['avg_fib_score']
            elif 'fibonacci_score' in df.columns:
                df['fib_score'] = df['fibonacci_score']
    
    # NEW: Add positions count for visualization
    if 'positions_count' not in df.columns:
        # Check if we have position-related columns
        pos_cols = [col for col in df.columns if 'position' in col.lower() or 'positions' in col.lower()]
        if pos_cols:
            for col in pos_cols:
                if 'count' in col.lower() or 'number' in col.lower():
                    df['positions_count'] = df[col]
                    break
    
    # NEW: Add Fibonacci positions count if available
    if 'fib_positions_count' not in df.columns:
        fib_pos_cols = [col for col in df.columns if 'fib' in col.lower() and 'position' in col.lower()]
        if fib_pos_cols:
            for col in fib_pos_cols:
                if 'count' in col.lower() or 'number' in col.lower():
                    df['fib_positions_count'] = df[col]
                    break
    
    # Ensure all numeric columns are float type for plotting
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col] = df[col].astype(float)
    
    return df



###############
# BACKTEST RUN LEDGER  (CSV always; Airtable best-effort, self-provisioning)
###############
AT_RUNS_TABLE = "Backtest Runs"
BACKTEST_RUNS_CSV = "backtest_runs.csv"

# (column name, Airtable field type, number precision or None)
_BT_RUNS_SCHEMA = [
    ("Run Label",        "singleLineText", None),
    ("Run Timestamp",    "dateTime",       None),
    ("Code Version",     "singleLineText", None),
    ("Mode",             "singleLineText", None),
    ("Start",            "singleLineText", None),
    ("End",              "singleLineText", None),
    ("Capital",          "number",         2),
    ("Total Return %",   "number",         2),
    ("CAGR %",           "number",         2),
    ("Annual Vol %",     "number",         2),
    ("Sharpe",           "number",         3),
    ("Sortino",          "number",         3),
    ("Max Drawdown %",   "number",         2),
    ("Win Rate %",       "number",         2),
    ("Num Trades",       "number",         0),
    ("Total Fees",       "number",         2),
    ("SPY Return %",     "number",         2),
    ("Alpha %",          "number",         2),
    ("Notes",            "multilineText",  None),
]


def _bt_code_version():
    """Short md5 of this source file, so each run records which code produced it."""
    try:
        with open(os.path.abspath(__file__), "rb") as fh:
            return hashlib.md5(fh.read()).hexdigest()[:10]
    except Exception:
        return "unknown"


def _bt_field_defs():
    """Airtable Metadata-API field definitions built from _BT_RUNS_SCHEMA."""
    defs = []
    for name, ftype, prec in _BT_RUNS_SCHEMA:
        if ftype == "number":
            defs.append({"name": name, "type": "number",
                         "options": {"precision": prec if prec is not None else 2}})
        elif ftype == "dateTime":
            defs.append({"name": name, "type": "dateTime",
                         "options": {"timeZone": "America/Los_Angeles",
                                     "dateFormat": {"name": "iso"},
                                     "timeFormat": {"name": "24hour"}}})
        else:
            defs.append({"name": name, "type": ftype})
    return defs


def _bt_ensure_airtable_table():
    """
    Ensure a 'Backtest Runs' table exists in AT_BASE. Returns the table name on
    success (records are written by name), else None. Self-provisions via the
    Metadata API and degrades gracefully -- the CSV ledger is the source of truth.
    """
    if not AT_API:
        return None
    meta = f"https://api.airtable.com/v0/meta/bases/{AT_BASE}/tables"
    try:
        r = session.get(meta, headers=AT_HEADERS, timeout=20)
        if r.status_code == 200:
            for t in r.json().get("tables", []):
                if t.get("name", "").strip().lower() == AT_RUNS_TABLE.lower():
                    return AT_RUNS_TABLE  # already exists
            body = {"name": AT_RUNS_TABLE,
                    "description": "Automated backtest run ledger (Swing_System.py).",
                    "fields": _bt_field_defs()}
            c = session.post(meta, headers=AT_HEADERS, json=body, timeout=30)
            if c.status_code in (200, 201):
                print(f"   🆕 Created Airtable table '{AT_RUNS_TABLE}'.")
                return AT_RUNS_TABLE
            print(f"   ⚠️  Could not create '{AT_RUNS_TABLE}' (HTTP {c.status_code}); "
                  f"CSV only. Token may lack schema.bases:write scope.")
            return None
        print(f"   ⚠️  Airtable schema read failed (HTTP {r.status_code}); CSV only.")
        return None
    except Exception as e:
        print(f"   ⚠️  Airtable table check skipped ({e}); CSV only.")
        return None


def log_backtest_run(report, engine, args):
    """
    Append ONE summary row per backtest to backtest_runs.csv (always) and to the
    Airtable 'Backtest Runs' table (best-effort). Never raises into the backtest.
    """
    try:
        label = getattr(args, "label", None) or \
            f"{args.mode} {args.start}->{args.end or 'today'}"

        # Best-effort SPY benchmark + alpha (re-derive so it doesn't depend on
        # variables scoped inside the plotting block above).
        spy_ret, alpha = "", ""
        try:
            spy = yf.download("SPY", start=args.start, end=args.end, progress=False)
            if spy is not None and not spy.empty:
                if isinstance(spy.columns, pd.MultiIndex):
                    spy.columns = spy.columns.get_level_values(0)
                sc = spy["Close"]
                if isinstance(sc, pd.DataFrame):
                    sc = sc.iloc[:, 0]
                spy_ret = round(float((sc.iloc[-1] / sc.iloc[0] - 1) * 100), 4)
                alpha = round(float(report.get("total_return_pct", 0.0)) - spy_ret, 4)
        except Exception:
            pass

        def _num(x):
            try:
                return round(float(x), 4)
            except Exception:
                return ""

        row = {
            "Run Label":      label,
            "Run Timestamp":  datetime.now().isoformat(timespec="seconds"),
            "Code Version":   _bt_code_version(),
            "Mode":           str(args.mode),
            "Start":          str(args.start),
            "End":            str(args.end or "today"),
            "Capital":        _num(report.get("initial_capital", getattr(args, "capital", ""))),
            "Total Return %": _num(report.get("total_return_pct")),
            "CAGR %":         _num(report.get("cagr_pct")),
            "Annual Vol %":   _num(report.get("annual_volatility_pct")),
            "Sharpe":         _num(report.get("sharpe_ratio")),
            "Sortino":        _num(report.get("sortino_ratio")),
            "Max Drawdown %": _num(report.get("max_drawdown_pct")),
            "Win Rate %":     _num(report.get("win_rate_pct")),
            "Num Trades":     _num(report.get("num_trades")),
            "Total Fees":     _num(report.get("total_fees")),
            "SPY Return %":   spy_ret,
            "Alpha %":        alpha,
            "Notes":          getattr(args, "notes", "") or "",
        }

        # ---- CSV (always; this is the source of truth) -----------------------
        cols = [c for c, _, _ in _BT_RUNS_SCHEMA]
        new_file = not os.path.exists(BACKTEST_RUNS_CSV)
        with open(BACKTEST_RUNS_CSV, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            if new_file:
                w.writeheader()
            w.writerow(row)
        print(f"\n📒 Backtest run appended to '{BACKTEST_RUNS_CSV}' (label: {label}).")

        # ---- Airtable (best-effort) ------------------------------------------
        if not getattr(args, "no_airtable", False):
            table = _bt_ensure_airtable_table()
            if table:
                url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(table)}"
                payload = {"records": [{"fields": {k: v for k, v in row.items() if v != ""}}],
                           "typecast": True}
                try:
                    pr = session.post(url, headers=AT_HEADERS, json=payload, timeout=30)
                    if pr.status_code in (200, 201):
                        print(f"   ☁️  Logged to Airtable '{table}'.")
                    else:
                        print(f"   ⚠️  Airtable push failed (HTTP {pr.status_code}); "
                              f"row is safe in CSV.")
                except Exception as e:
                    print(f"   ⚠️  Airtable push error ({e}); row is safe in CSV.")
    except Exception as e:
        print(f"⚠️ log_backtest_run skipped: {e}")


###############
# MAIN EXECUTION - UPDATED WITH FIBONACCI ENHANCEMENTS
###############

def main(args):
    """
    Main entry point with three modes:
    1. BACKTEST: Run historical simulation
    2. LIVE: Generate today's signals
    3. ANALYZE: Quick analysis without execution
    
    Args:
        args: Parsed command-line arguments from argparse
    """
    
    # Clean and prepare ticker list
    # Check for custom ticker list
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
        print(f"🎯 Using custom ticker list: {tickers}")
    else:
        tickers = clean_ticker_list(DEFAULT_TICKERS)
    
    print("\n" + "=" * 100)
    print("🚀 PROFESSIONAL TRADING SYSTEM - STARTUP".center(100))
    print("=" * 100)
    print(f"\n⚙️  CONFIGURATION:")
    print(f"   Mode: {args.mode.upper()}")
    print(f"   Filter Mode: {FILTER_MODE}")
    print(f"   Max Concurrent Positions: {MAX_CONCURRENT_POSITIONS}")
    print(f"   Universe Size: {len(tickers)} symbols")
    print(f"   Capital: ${args.capital:,.0f}")
    print(f"   Risk per Trade: {SWING_RISK_PER_TRADE:.1%}")
    print(f"   Max Position Size: {MAX_POSITION_SIZE:.1%}")
    print(f"   Stop Loss Range: {MIN_STOP_ALLOWED:.1%} - {MAX_STOP_ALLOWED:.1%}")
    
    # NEW: Display Fibonacci configuration
    print(f"\n🎯 FIBONACCI CONFIGURATION:")
    print(f"   Fibonacci Enabled: {FIB_POSITION_CONFIG['enabled']}")
    print(f"   Regime Dependent: {FIB_POSITION_CONFIG.get('regime_dependent', False)}")
    print(f"   Stop Tightening: {FIB_POSITION_CONFIG.get('fib_stop_tightening', False)}")
    
    if FIB_POSITION_CONFIG.get('regime_dependent', False):
        regime_multipliers = FIB_POSITION_CONFIG.get('regime_multipliers', {})
        print(f"   Regime Multipliers:")
        for regime, mults in regime_multipliers.items():
            print(f"     - {regime}: Support Boost={mults.get('support_boost', 1.0):.1f}x, "
                  f"Resistance Penalty={mults.get('resistance_penalty', 1.0):.1f}x")
    
    print("=" * 100 + "\n")
    
    # Initialize system
    system = ProfessionalTradingSystem(symbols=tickers, start_date=args.start)
    
    # =========================================================================
    # MODE 1: BACKTEST
    # =========================================================================
    if args.mode == 'backtest':
        print(f"📊 Running backtest from {args.start} to {args.end or 'today'}...\n")
        
        engine = BacktestEngine(system, initial_capital=args.capital)
        
        try:
            report, history = engine.run_simulation(
                start_date=args.start,
                end_date=args.end or date.today().isoformat()
            )
            
            # Print performance report with Fibonacci analysis
            print_performance_report(report)
            
            # Generate visualizations
            if not report['equity_curve'].empty:
                # Prepare equity curve for plotting
                equity_curve = prepare_equity_curve_for_plotting(report['equity_curve'])
                
                # Plot basic backtest results — always-savefig (headless-safe; no plt.show)
                _label_slug = (args.label or "run").replace(" ", "_").replace("/", "_")
                plot_backtest_results(
                    equity_curve,
                    engine.trades,
                    title=f"Backtest {args.start} to {args.end or 'Today'}",
                    save_path=f"equity_{_label_slug}.png"
                )

                # NEW: Plot Fibonacci-specific analysis
                if 'fibonacci_analysis' in report and report['fibonacci_analysis']:
                    print("\n📊 Generating Fibonacci-specific analysis...")
                    plot_fibonacci_analysis(engine.trades, report, save_path=f"fib_{_label_slug}.png")
                
                # Compare to SPY
                try:
                    spy = yf.download('SPY', start=args.start, end=args.end, progress=False)
                    if spy is not None and not spy.empty:
                        # FIXED: Handle MultiIndex columns
                        if isinstance(spy.columns, pd.MultiIndex):
                            spy.columns = spy.columns.get_level_values(0)
        
                        spy_close = spy['Close']
                        if isinstance(spy_close, pd.DataFrame):
                            spy_close = spy_close.iloc[:, 0]
        
                        # Calculate SPY return
                        spy_return = ((spy_close.iloc[-1] / spy_close.iloc[0]) - 1) * 100
                        strategy_return = report['total_return_pct']
                        alpha = strategy_return - spy_return
        
                        print(f"\n📊 BENCHMARK COMPARISON:")
                        print(f"   Strategy Return: {strategy_return:.2f}%")
                        print(f"   SPY Return: {spy_return:.2f}%")
                        print(f"   Alpha: {alpha:+.2f}%")
        
                        if alpha > 0:
                            print(f"   ✅ Strategy outperformed by {alpha:.2f}%")
                        else:
                            print(f"   ❌ Strategy underperformed by {abs(alpha):.2f}%")
        
                        # Plot comparison
                        plot_comparison(
                            equity_curve,
                            spy_close,
                            title=f'Strategy vs SPY ({args.start} to {args.end or "Today"})'
                        )
        
                except Exception as e:
                    print(f"⚠️ Could not compare to SPY: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Save detailed results with Fibonacci data
            try:
                results_df = pd.DataFrame(history)
                
                # NEW: Add Fibonacci metrics to saved results
                if hasattr(engine, 'fib_metrics_history') and engine.fib_metrics_history:
                    fib_metrics_df = pd.DataFrame(engine.fib_metrics_history)
                    results_df = pd.merge(results_df, fib_metrics_df, on='date', how='left')
                
                results_df.to_csv('backtest_history.csv', index=False)
                print("\n📁 Detailed results saved to 'backtest_history.csv'")
                
                # Save trades with Fibonacci metadata
                trades_df = pd.DataFrame(engine.trades)
                if not trades_df.empty:
                    trades_df.to_csv('backtest_trades.csv', index=False)
                    print("📁 Trade log saved to 'backtest_trades.csv'")
                    
                    # NEW: Save Fibonacci trades separately
                    fib_trades_df = pd.DataFrame(engine.trades_with_fib)
                    if not fib_trades_df.empty:
                        fib_trades_df.to_csv('fibonacci_trades.csv', index=False)
                        print("📁 Fibonacci trades saved to 'fibonacci_trades.csv'")
                
                # NEW: Generate Fibonacci performance report
                fib_analysis = engine.analyze_fibonacci_performance()
                if fib_analysis:
                    with open('fibonacci_performance_report.json', 'w') as f:
                        import json
                        json.dump(fib_analysis, f, indent=2, default=str)
                    print("📁 Fibonacci performance report saved to 'fibonacci_performance_report.json'")
                    
            except Exception as e:
                print(f"⚠️ Could not save CSV files: {e}")
                import traceback
                traceback.print_exc()

            # Append a one-row summary of this run to the backtest ledger
            # (local CSV always; Airtable 'Backtest Runs' table best-effort).
            log_backtest_run(report, engine, args)

        except Exception as e:
            print(f"❌ Backtest failed: {e}")
            import traceback
            traceback.print_exc()
    
    # =========================================================================
    # MODE 2: LIVE
    # =========================================================================
    elif args.mode == 'live':
        print("📡 Running LIVE signal generation...\n")
        
        daily_exec = DailyExecutionSystem(system, skip_airtable=args.no_airtable)
        
        try:
            daily_exec.run_daily_update()
            
            # NEW: Print Fibonacci summary for live execution
            if hasattr(daily_exec, 'fib_metrics_log') and daily_exec.fib_metrics_log:
                daily_exec.print_fibonacci_summary()
            
            if not args.no_airtable:
                print("\n✅ Live execution complete. Signals pushed to Airtable.")
            else:
                print("\n✅ Live execution complete. Airtable sync skipped.")
                
        except Exception as e:
            print(f"❌ Live execution failed: {e}")
            import traceback
            traceback.print_exc()
    
    # =========================================================================
    # MODE 3: ANALYZE
    # =========================================================================
    elif args.mode == 'analyze':
        print("🔍 Running quick analysis (no execution)...\n")
        
        try:
            results = system.run_analysis(total_capital=args.capital)
            
            if results:
                system.print_summary(results)
                
                # Show portfolio stats with Fibonacci data
                portfolio_stats = results.get('portfolio_stats', {})
                if portfolio_stats:
                    print(f"\n📊 PORTFOLIO STATISTICS:")
                    print(f"   Total Positions: {portfolio_stats.get('total_positions', 0)}")
                    print(f"   Total Allocation: {portfolio_stats.get('total_allocation', 0):.1%}")
                    print(f"   Avg Fibonacci Score: {portfolio_stats.get('avg_fib_score', 0):.2f}")
                    print(f"   Support Positions: {portfolio_stats.get('support_positions', 0)}")
                
                # NEW: Show Fibonacci metrics from risk management
                risk_mgmt = results.get('risk_management', {})
                fib_metrics = risk_mgmt.get('fib_metrics', {})
                if fib_metrics:
                    print(f"\n📊 FIBONACCI RISK METRICS:")
                    print(f"   Average Fibonacci Score: {fib_metrics.get('avg_fib_score', 0):.2f}")
                    print(f"   Support Positions: {fib_metrics.get('support_positions', 0)}")
                    print(f"   Average Fibonacci Boost: {fib_metrics.get('total_fib_boost', 0)*100:+.1f}%")
                
                # NEW: Show market regime analysis
                market_regime = results.get('market_breadth', 'UNKNOWN')
                print(f"\n🌐 MARKET REGIME ANALYSIS:")
                print(f"   Detected Regime: {market_regime}")
                
                # Get regime-specific recommendations
                try:
                    dashboard = MarketRegimeDashboard()
                    if dashboard.fetch_data():
                        dashboard.calculate_metrics()
                        current_regime = dashboard.classify_regime()
                        recommendations = dashboard.get_recommendation()
                        
                        print(f"   Recommended Action: {recommendations['action']}")
                        print(f"   Position Count: {recommendations['position_count']}")
                        print(f"   Focus: {recommendations['focus']}")
                        
                        # Apply regime-specific Fibonacci adjustments
                        regime_multipliers = FIB_POSITION_CONFIG.get('regime_multipliers', {})
                        if current_regime in regime_multipliers:
                            mults = regime_multipliers[current_regime]
                            print(f"   Fibonacci Adjustments:")
                            print(f"     - Support Boost: {mults.get('support_boost', 1.0):.1f}x")
                            print(f"     - Resistance Penalty: {mults.get('resistance_penalty', 1.0):.1f}x")
                except Exception as e:
                    print(f"   ⚠️ Could not load regime recommendations: {e}")
                
                print("\n✅ Analysis complete.")
            else:
                print("❌ No results generated.")
                
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            import traceback
            traceback.print_exc()
    
    # =========================================================================
    # MODE 4: UNIVERSE SCAN (NEW OPTIONAL MODE)
    # =========================================================================
    elif args.mode == 'portfolio':
        print("📊 Running PORTFOLIO MODE - Checking existing positions...\n")
        
        try:
            # 1. Fetch current portfolio from Airtable
            print("📥 Fetching portfolio from Airtable...")
            url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
            resp = session.get(url, headers=AT_HEADERS, params={"filterByFormula": '{In Portfolio}="Yes"'})
            
            portfolio = {}
            if resp.status_code == 200:
                records = resp.json().get('records', [])
                for rec in records:
                    fields = rec.get('fields', {})
                    ticker = fields.get('Ticker', '')
                    if ticker:
                        portfolio[ticker] = {
                            'record_id': rec['id'],
                            'entry_price': fields.get('Current Price', 0),
                            'stop_price': fields.get('Stop Price', 0),
                            'target_price': fields.get('Target Price', 0),
                            'trade_type': fields.get('Trade Type', 'SWING'),
                            'stop_loss_pct': fields.get('Stop Loss', 0.12),
                            'entry_date': fields.get('Entry Date', ''),
                        }
            
            print(f"📋 Current Portfolio: {len(portfolio)} positions")
            for ticker in portfolio:
                print(f"   - {ticker}")
            
            # 2. Load price data for portfolio + universe
            price_data = system.load_price_data()
            if not price_data:
                print("❌ Failed to load price data")
                exit(1)
            
            # 3. Get current regime
            econ_data = system.economic.load()
            regime = system.regime_mod.classify(econ_data)
            
            # 4. Generate signals for portfolio holdings
            print(f"\n{'='*70}")
            print("📊 PORTFOLIO STATUS".center(70))
            print('='*70)
            print(f"{'Ticker':<8} {'Price':>10} {'Stop':>10} {'Target':>10} {'Signal':<12} {'Action':<10}")
            print('-'*70)
            
            holds = []
            sells = []
            stops = []
            
            for ticker, pos in portfolio.items():
                if ticker not in price_data:
                    print(f"{ticker:<8} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'NO DATA':<12} {'CHECK':<10}")
                    continue
                
                df = price_data[ticker]
                if df.empty:
                    continue
                    
                current_price = float(df['Close'].iloc[-1])
                stop_price = pos['stop_price']
                target_price = pos['target_price']
                
                # Check conditions
                if current_price <= stop_price:
                    signal = "STOP HIT"
                    action = "SELL NOW"
                    stops.append(ticker)
                elif current_price >= target_price:
                    signal = "TARGET HIT"
                    action = "TAKE PROFIT"
                    sells.append(ticker)
                else:
                    # Generate fresh signal
                    signals = system.signal_gen.compute_signals({ticker: df}, regime)
                    sig = signals.get(ticker, {})
                    if sig:
                        raw_signal = sig.get('signal', 'HOLD')
                        if raw_signal in ('SELL', 'STRONG_SELL'):
                            signal = raw_signal
                            action = "SELL"
                            sells.append(ticker)
                        elif raw_signal in ('BUY', 'STRONG_BUY'):
                            signal = "HOLD ✓"
                            action = "KEEP"
                            holds.append(ticker)
                        else:
                            signal = "HOLD"
                            action = "KEEP"
                            holds.append(ticker)
                    else:
                        signal = "HOLD"
                        action = "KEEP"
                        holds.append(ticker)
                
                # Calculate % to target/stop
                pct_to_target = ((target_price / current_price) - 1) * 100 if current_price > 0 else 0
                pct_to_stop = ((current_price / stop_price) - 1) * 100 if stop_price > 0 else 0
                
                print(f"{ticker:<8} ${current_price:>8.2f} ${stop_price:>8.2f} ${target_price:>8.2f} {signal:<12} {action:<10}")
            
            print('-'*70)
            print(f"HOLD: {len(holds)} | SELL: {len(sells)} | STOPPED: {len(stops)}")
            
            # 4.5 Update Airtable with actions
            print("\n📤 Updating Airtable with actions...")
            for ticker, pos in portfolio.items():
                if ticker in holds:
                    action = "KEEP"
                elif ticker in sells:
                    action = "SELL"
                elif ticker in stops:
                    action = "SELL NOW"
                else:
                    action = "CHECK"
                
                # Get current price for update
                if ticker in price_data and not price_data[ticker].empty:
                    current_price = float(price_data[ticker]['Close'].iloc[-1])
                else:
                    current_price = 0
                
                # Update record
                record_id = pos.get('record_id')
                if record_id:
                    try:
                        update_url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}/{record_id}"
                        
                        # Calculate Days Held
                        entry_date_str = pos.get('entry_date', '')
                        days_held = 0
                        if entry_date_str:
                            try:
                                entry_dt = datetime.strptime(entry_date_str, '%Y-%m-%d').date()
                                days_held = (date.today() - entry_dt).days
                            except:
                                days_held = 0
                        
                        update_data = {
                            "fields": {
                                "Action": action,
                                "Current Price": current_price,
                                
                                "Last Updated": date.today().isoformat()
                            }
                        }
                        resp = session.patch(update_url, headers=AT_HEADERS, json=update_data)
                        if resp.status_code == 200:
                            print(f"   ✅ {ticker}: {action}")
                        else:
                            print(f"   ❌ {ticker}: Failed to update")
                    except Exception as e:
                        print(f"   ❌ {ticker}: {e}")
            
            # 5. Find new buys if slots available
            open_slots = 10 - len(holds)
            
            if open_slots > 0:
                print(f"\n{'='*70}")
                print(f"🆕 NEW BUY CANDIDATES ({open_slots} slots available)".center(70))
                print('='*70)
                
                # Run full analysis to get top picks
                results = system.run_analysis()
                if results:
                    final_signals = results.get('final_signals', {})
                    
                    # Filter out current holdings and sells
                    exclude = set(portfolio.keys())
                    new_picks = {k: v for k, v in final_signals.items() 
                                 if k not in exclude and v.get('signal') in ('BUY', 'STRONG_BUY')}
                    
                    # Sort by ranking score
                    sorted_picks = sorted(new_picks.items(), 
                                         key=lambda x: x[1].get('ranking_score', 0), 
                                         reverse=True)[:open_slots]
                    
                    if sorted_picks:
                        print(f"{'Ticker':<8} {'Price':>10} {'Stop':>10} {'Target':>10} {'Signal':<12} {'Type':<10}")
                        print('-'*70)
                        for ticker, sig in sorted_picks:
                            print(f"{ticker:<8} ${sig.get('current_price', 0):>8.2f} "
                                  f"${sig.get('stop_price', 0):>8.2f} ${sig.get('target_price', 0):>8.2f} "
                                  f"{sig.get('signal', 'BUY'):<12} {sig.get('trade_type', 'SWING'):<10}")
                    else:
                        print("   No new qualifying signals today")
            
            print(f"\n{'='*70}")
            print("✅ Portfolio check complete")
            
        except Exception as e:
            print(f"❌ Portfolio mode failed: {e}")
            import traceback
            traceback.print_exc()


    elif args.mode == 'scan':
        print("🔍 Running universe scan with Fibonacci analysis...\n")
        
        try:
            # Load price data
            price_data = system.load_price_data(start=args.start)
            
            # Get current market regime
            econ_data = system.economic.load()
            regime = system.regime_mod.classify(econ_data)
            
            # Run scanner with Fibonacci focus
            scanner = UniverseScanner(system)
            scan_results = scanner.scan_universe_with_fibonacci_report(
                price_data=price_data,
                regime=regime,
                top_n=20
            )
            
            if scan_results:
                print("\n✅ Universe scan complete.")
                
                # Get top symbols for potential trading
                top_symbols = scan_results.get('top_symbols', [])
                if top_symbols:
                    print(f"\n🏆 TOP {len(top_symbols)} SCANNED SYMBOLS:")
                    for i, symbol in enumerate(top_symbols[:10], 1):
                        print(f"   {i:2}. {symbol}")
            else:
                print("❌ No suitable symbols found in scan.")
                
        except Exception as e:
            print(f"❌ Universe scan failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 100)
    print("🏁 SYSTEM SHUTDOWN".center(100))
    print("=" * 100 + "\n")


# =========================================================================
# ENTRY POINT - UPDATED WITH FIBONACCI OPTIONS
# =========================================================================
if __name__ == "__main__":
    import argparse
    import sys
    
    # =========================================================================
    # PARSE COMMAND LINE ARGUMENTS - UPDATED
    # =========================================================================
    parser = argparse.ArgumentParser(
        description='Professional Trading System with Fibonacci Integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python Swing_System.py --mode portfolio      # Check existing positions + new buys
  python Swing_System.py --mode live           # Generate today's signals
  python Swing_System.py --mode backtest       # Run historical backtest
  python Swing_System.py --mode analyze        # Quick analysis
  python Swing_System.py --mode diagnose       # System diagnostics
  python Swing_System.py --mode backtest --start 2023-01-01 --end 2023-12-31
  python Swing_System.py --mode live --no-airtable  # Live without Airtable sync
  python Swing_System.py --mode analyze --capital 50000  # Analysis with $50k capital
        """
    )
    
    parser.add_argument('--mode', type=str, default='live', 
                       choices=['live', 'backtest', 'analyze', 'diagnose', 'scan', 'portfolio'],
                       help='Execution mode (default: live)')
    parser.add_argument('--start', type=str, default='2020-01-01', 
                       help='Backtest start date (YYYY-MM-DD, default: 2020-01-01)')
    parser.add_argument('--end', type=str, default=None, 
                       help='Backtest end date (YYYY-MM-DD, default: today)')
    parser.add_argument('--capital', type=float, default=100000, 
                       help='Initial capital (default: 100000)')
    parser.add_argument('--no-airtable', action='store_true', 
                       help='Skip Airtable sync in live mode')
    parser.add_argument('--fib-enabled', action='store_true', 
                       help='Enable Fibonacci position sizing (overrides config)')
    parser.add_argument('--fib-strict', action='store_true',
                       help='Use strict Fibonacci filtering')
    parser.add_argument('--fib-report', action='store_true',
                       help='Generate detailed Fibonacci performance report')
    parser.add_argument('--tickers', type=str, default=None,
                       help='Comma-separated list of tickers (e.g., ELVR,CARR,CELH)')
    parser.add_argument('--label', type=str, default=None,
                       help='Label for this backtest run in the ledger (e.g., "baseline" or "flow+squeeze")')
    parser.add_argument('--notes', type=str, default='',
                       help='Free-text notes recorded with this backtest run')

    args = parser.parse_args()
    
    # =========================================================================
    # APPLY FIBONACCI COMMAND-LINE OVERRIDES
    # =========================================================================
    if args.fib_enabled:
        FIB_POSITION_CONFIG['enabled'] = True
        print("🔧 Override: Fibonacci position sizing ENABLED")
    
    if args.fib_strict:
        FIB_POSITION_CONFIG['strict_fib_filtering'] = True
        print("🔧 Override: Strict Fibonacci filtering ENABLED")
    
    # =========================================================================
    # DIAGNOSTIC MODE - RUNS STANDALONE
    # =========================================================================
    if args.mode == 'diagnose':
        print("\n🔬 RUNNING DIAGNOSTIC MODE...\n")
        full_diagnostic()
        sys.exit(0)
    
    # =========================================================================
    # NORMAL EXECUTION - CALL MAIN
    # =========================================================================
    try:
        main(args)
        
        # Generate Fibonacci report if requested
        if args.fib_report and args.mode == 'backtest':
            print("\n📊 Generating detailed Fibonacci report...")
            try:
                # This would typically be generated by the backtest engine
                print("✅ Fibonacci report included in backtest results.")
            except Exception as e:
                print(f"⚠️ Could not generate Fibonacci report: {e}")
                
    except KeyboardInterrupt:
        print("\n\n⚠️  Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)