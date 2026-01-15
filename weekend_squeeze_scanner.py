#!/usr/bin/env python3
"""
Weekend Squeeze Scanner v2.2
============================
Scans for stocks where the weekly TTM Squeeze fired on Friday's close.
Run this over the weekend to find potential breakout candidates for Monday.

Changes in v2.2:
  - Airtable now only syncs GREEN fires (bullish squeeze breakouts)

Changes in v2.1:
  - Added Airtable integration (writes to "Squeeze Signals" table)
  - Added --no-airtable flag to skip Airtable sync
  - Added --tickers flag for custom ticker scans

Changes in v2.0:
  - Added -s flag for individual stock analysis
  - Fixed bars_in_squeeze counting
  - Uses daily data resampled to weekly (more current than native weekly)
  - KC multiplier = 1.2 (matches Mid squeeze threshold)
  - Added --debug flag for troubleshooting

Airtable Fields:
  - Ticker, Sector, Final Signal, Signal Strength, Current Price, Last Updated
  - Squeeze Status (FIRED_GREEN, FIRED_RED, READY, IN_SQUEEZE)
  - Momentum, Momentum Accel, Bars in Squeeze, Weekly Change Pct
  - Momentum Rising, Momentum Positive, Fire Direction

Usage:
    # Individual stock analysis
    python weekend_squeeze_scanner.py -s AAPL
    python weekend_squeeze_scanner.py -s AAPL NVDA TSLA
    python weekend_squeeze_scanner.py -s AAPL --debug

    # Universe scans
    python weekend_squeeze_scanner.py
    python weekend_squeeze_scanner.py --universe sp500
    python weekend_squeeze_scanner.py --universe custom --symbols AAPL NVDA TSLA
    python weekend_squeeze_scanner.py --tickers AAPL NVDA TSLA

    # Sunday rankings
    python weekend_squeeze_scanner.py --sunday --save

    # With/without Airtable
    python weekend_squeeze_scanner.py --no-airtable  # Skip Airtable sync
"""

import argparse
import pandas as pd
import numpy as np
import yfinance as yf
import os
import re
import urllib.parse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# AIRTABLE CONFIGURATION
# ============================================================================

AT_BASE = os.getenv("AT_BASE", "appIUFp3KFrf8KXez")
AT_API = os.getenv("AT_API", "")  # Set via environment variable
AT_TABLE = "Squeeze Signals"

# HTTP session with retry strategy
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

AIRTABLE_BATCH_SIZE = 10


def sanitize_number(val):
    """Sanitize numeric values for Airtable."""
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
    except Exception:
        return 'Unknown'


# ============================================================================
# AIRTABLE INTEGRATION
# ============================================================================

def fetch_airtable_records() -> Dict[str, dict]:
    """Return {TICKER: {record_id, fields}} from Airtable Squeeze Signals table."""
    if not AT_API:
        return {}

    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    params = {"pageSize": 100}
    records_map: Dict[str, dict] = {}

    try:
        while True:
            resp = session.get(url, headers=AT_HEADERS, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"❌ Airtable fetch failed: {resp.status_code}")
                break

            data = resp.json()
            for rec in data.get("records", []):
                ticker = rec.get("fields", {}).get("Ticker", "").upper()
                if ticker:
                    records_map[ticker] = {
                        "id": rec["id"],
                        "fields": rec.get("fields", {})
                    }

            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset

    except Exception as e:
        print(f"❌ Airtable fetch failed: {e}")

    return records_map


def push_squeeze_signals_to_airtable(
    fired_green: List[Dict],
    fired_red: List[Dict],
    ready_to_fire: List[Dict],
    in_squeeze: List[Dict]
):
    """
    Push squeeze signals to Airtable 'Squeeze Signals' table.
    Uses same field structure as Swing System where applicable.
    """
    if not AT_API:
        print("⚠️  AT_API not set - Airtable sync skipped")
        return

    print("\n📤 Pushing squeeze signals to Airtable...")

    # Fetch existing records
    existing = fetch_airtable_records()
    print(f"   Found {len(existing)} existing records in Airtable")

    # Only push GREEN fires to Airtable (bullish squeeze breakouts)
    all_signals = []

    for stock in fired_green:
        stock['squeeze_status'] = 'FIRED_GREEN'
        stock['signal'] = 'BUY'
        all_signals.append(stock)

    # Skip FIRED_RED, READY, and IN_SQUEEZE - only GREEN fires are actionable

    update_batch = []
    create_batch = []
    update_count = 0
    create_count = 0

    for stock in all_signals:
        sym = stock['symbol'].upper()

        # Calculate sunday_score if not already set
        if 'sunday_score' not in stock:
            stock['sunday_score'] = calculate_sunday_score(stock)

        # Use sector from scan phase (avoids extra API calls)
        sector = stock.get('sector', 'Unknown')

        # Build fields matching Swing System structure
        fields = {
            "Ticker": sym,
            "Sector": sector,
            "Final Signal": stock['signal'],
            "Signal Strength": sanitize_number(stock.get('sunday_score', 0)),
            "Current Price": sanitize_number(stock.get('current_price', 0)),
            "Last Updated": date.today().isoformat(),

            # Squeeze-specific fields
            "Squeeze Status": stock['squeeze_status'],
            "Momentum": sanitize_number(stock.get('momentum', 0)),
            "Momentum Accel": sanitize_number(stock.get('momentum_accel', 0)),
            "Bars in Squeeze": sanitize_number(stock.get('bars_in_squeeze', 0)),
            "Weekly Change Pct": sanitize_number(stock.get('weekly_change_pct', 0)) / 100,  # Convert to decimal for Airtable percent field
            "Momentum Rising": bool(stock.get('momentum_rising', False)),
            "Momentum Positive\t": bool(stock.get('momentum_positive', False)),

            # Fire direction for squeeze fires
            "Fire Direction": stock.get('fire_direction', '') or '',
        }

        # Add to appropriate batch
        if sym in existing:
            update_batch.append({
                "id": existing[sym]["id"],
                "fields": fields
            })
        else:
            create_batch.append({"fields": fields})

        # Process batches
        if len(update_batch) >= AIRTABLE_BATCH_SIZE:
            _process_airtable_batch(update_batch, "PATCH")
            update_count += len(update_batch)
            update_batch = []

        if len(create_batch) >= AIRTABLE_BATCH_SIZE:
            _process_airtable_batch(create_batch, "POST")
            create_count += len(create_batch)
            create_batch = []

    # Process remaining batches
    if update_batch:
        _process_airtable_batch(update_batch, "PATCH")
        update_count += len(update_batch)

    if create_batch:
        _process_airtable_batch(create_batch, "POST")
        create_count += len(create_batch)

    print(f"✅ Airtable sync complete: {update_count} updates, {create_count} creates")


def _process_airtable_batch(batch: List[Dict], method: str):
    """Process a batch of Airtable records."""
    if not batch:
        return

    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"

    try:
        if method == "PATCH":
            # Update existing records
            payload = {"records": batch}
            resp = session.patch(url, headers=AT_HEADERS, json=payload, timeout=30)
        else:
            # Create new records
            payload = {"records": batch}
            resp = session.post(url, headers=AT_HEADERS, json=payload, timeout=30)

        if resp.status_code not in [200, 201]:
            print(f"   ⚠️  Airtable {method} failed: {resp.status_code} - {resp.text[:200]}")
    except Exception as e:
        print(f"   ⚠️  Airtable {method} error: {e}")


# ============================================================================
# UNIVERSE DEFINITIONS
# ============================================================================

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


# =========================================================================
# MERGED TRADING UNIVERSE
# Includes: Core watchlist + Russell 2000 + S&P 600 + Speculative picks
# Total: ~1600 unique tickers after deduplication
# =========================================================================

DEFAULT_TICKERS = [
    # =========================================================================
    # RUSSELL 2000 SCREENER - $1-$20, 500k+ volume (317 stocks)
    # =========================================================================
    'FATE', 'HUMA', 'IPSC', 'NRDY', 'FCUV', 'HAIN', 'CDLX', 'BRCC', 'LXRX', 'QSI',
    'HRTX', 'OPK', 'ZNTL', 'AMTX', 'SLQT', 'GERN', 'ALLO', 'URG', 'SABR', 'OCGN',
    'ALEC', 'GPRO', 'SPWR', 'WTI', 'CRBU', 'SVC', 'PACB', 'DDD', 'OTLK', 'PLBY',
    'CNDT', 'ALIT', 'HYLN', 'EGHT', 'CERS', 'AIRS', 'GEVO', 'CCCC', 'IBRX', 'EDIT',
    'CLNE', 'SAVA', 'LIDR', 'RC', 'CCO', 'INO', 'TMCI', 'JELD', 'XRX', 'CLOV',
    'FUBO', 'HNST', 'OMI', 'IOVA', 'NNOX', 'BDN', 'MVST', 'ASPN', 'RBBN', 'ALTO',
    'BLND', 'EVGO', 'SPCE', 'IRWD', 'CYH', 'GOSS', 'LWLG', 'AGEN', 'OM', 'VUZI',
    'NAT', 'EGY', 'RCKT', 'ERAS', 'FNKO', 'BMBL', 'ABSI', 'ABCL', 'TOI', 'AVXL',
    'TTSH', 'RPAY', 'INDI', 'STKL', 'FOSL', 'BFLY', 'BORR', 'ESPR', 'EVH', 'ZIP',
    'PGEN', 'RXRX', 'HPK', 'DNUT', 'TROX', 'SANA', 'SLDP', 'AMPY', 'BGS', 'GOGO',
    'PTLO', 'ULCC', 'ABUS', 'MQ', 'NRGV', 'CODI', 'GTN', 'STGW', 'VERI', 'TLS',
    'SFIX', 'APPS', 'GAMB', 'BW', 'ORGO', 'NEXT', 'RES', 'RWT', 'PAYO', 'MNKD',
    'PTEN', 'UDMY', 'VIR', 'AIV', 'SENS', 'THRY', 'CRSR', 'DENN', 'DC', 'TDUP',
    'HLX', 'AXL', 'ESRT', 'MYGN', 'SITC', 'AHH', 'EVLV', 'JBI', 'BLMN', 'EOLS',
    'MDXG', 'SG', 'CFFN', 'UNIT', 'VNDA', 'LFST', 'NEOG', 'RDW', 'WEAV', 'SXC',
    'ORC', 'XERS', 'NABL', 'COUR', 'LILAK', 'BCRX', 'ENVX', 'RLJ', 'SPIR', 'DBI',
    'SFL', 'ACHR', 'FDMT', 'ABR', 'YEXT', 'CRGY', 'ACVA', 'FCEL', 'ONDS', 'KREF',
    'SNBR', 'RLAY', 'GNL', 'AVAH', 'IVR', 'TNGX', 'GT', 'KODK', 'TRTX', 'PRCH',
    'PCT', 'HLMN', 'SHO', 'GNW', 'DRH', 'TK', 'SHLS', 'PUMP', 'TTI', 'NUVB',
    'EXPI', 'NTLA', 'DAWN', 'MARA', 'MFA', 'GPRE', 'ARI', 'HLIT', 'REPL', 'ECVT',
    'LZ', 'CWH', 'NG', 'ARRY', 'FSLY', 'AHCO', 'CGEM', 'FBRT', 'UTZ', 'BKKT',
    'COMP', 'KURA', 'PBI', 'TWO', 'BKD', 'TALO', 'CLSK', 'RXST', 'BHVN', 'HOPE',
    'PEB', 'WULF', 'FIGS', 'AMPL', 'VLY', 'APLE', 'UEC', 'DHT', 'NEO', 'AMLX',
    'EBS', 'PMT', 'HTBK', 'OCUL', 'AMRX', 'PD', 'AEVA', 'HLF', 'RIOT', 'DNOW',
    'AVPT', 'ASAN', 'EFC', 'DX', 'JOBY', 'DBRG', 'AI', 'HOUS', 'ARLO', 'RELY',
    'JANX', 'FOLD', 'FLYW', 'UUUU', 'SEM', 'STNE', 'SMR', 'OSCR', 'OI', 'SBH',
    'VSH', 'KALV', 'CIFR', 'AXTI', 'DVAX', 'SGRY', 'FWRG', 'AMN', 'DOC', 'REAL',
    'GEO', 'PLAY', 'MGNI', 'CWK', 'ADPT', 'DNLI', 'EXTR', 'HR', 'PHR', 'NN',
    'IRDM', 'GLUE', 'BNL', 'IRT', 'ARR', 'NTST', 'MXL', 'METC', 'WWW', 'GRPN',
    'SCVL', 'SONO', 'SKYT', 'VIAV', 'COMM', 'EYPT', 'LBRT', 'SM', 'ALHC', 'SBRA',
    'EBC', 'MAC', 'CDE', 'CXW', 'SFNC', 'ADMA', 'NRIX', 'UE', 'PL', 'TGNA',
    'RSI', 'ADNT', 'CVBF', 'LC', 'BANC', 'BXMT', 'SMPL',

    # =========================================================================
    # S&P 600 SCREENER - $1-$20, 500k+ volume (116 stocks)
    # =========================================================================
    'SABR', 'RC', 'BDN', 'NWL', 'GOGO', 'JBLU', 'MPW', 'HTZ', 'RES', 'RWT',
    'PAYO', 'MCW', 'PTEN', 'VIR', 'THRY', 'CRSR', 'HLX', 'AXL', 'HBI', 'MYGN',
    'SITC', 'VSTS', 'AHH', 'BLMN', 'CFFN', 'UNIT', 'NEOG', 'OGN', 'SXC', 'NABL',
    'LUMN', 'CXM', 'ABR', 'CRGY', 'WEN', 'KREF', 'GNL', 'CERT', 'BGC', 'SHO',
    'GNW', 'DRH', 'WU', 'AESI', 'EXPI', 'MARA', 'ARI', 'HLIT', 'AHCO', 'GO',
    'FBRT', 'VYX', 'PBI', 'TWO', 'TALO', 'CLSK', 'LEG', 'DEI', 'HOPE', 'MBC',
    'PEB', 'DV', 'MODG', 'APLE', 'CC', 'NEO', 'WT', 'PMT', 'IART', 'RXO',
    'FMC', 'DNOW', 'EFC', 'TRIP', 'ARLO', 'ACHC', 'SEM', 'FUN', 'OI', 'SBH',
    'VSH', 'DXC', 'PENN', 'DVAX', 'HAYW', 'AMN', 'GEO', 'PLAY', 'CWK', 'EXTR',
    'ADEA', 'FTRE', 'ARR', 'ELME', 'MXL', 'VTLE', 'NVRI', 'WWW', 'SCVL', 'SONO',
    'VIAV', 'LBRT', 'SM', 'MAC', 'CXW', 'SFNC', 'ADMA', 'UE', 'TGNA', 'ADNT',
    'CVBF', 'MDU', 'BANC', 'WSC', 'BXMT', 'SMPL',

    # =========================================================================
    # SPECULATIVE PICKS - Drones, AI, Batteries, Mining, Space (21 stocks)
    # =========================================================================

    # Drones/Defense
    'ONDS',   # Ondas Holdings - Counter-UAS, Iron Drone Raider
    'RCAT',   # Red Cat Holdings - Black Widow drones, Palantir partnership
    'DPRO',   # Draganfly - Heavy-lift drones, emergency response
    'SPAI',   # Safe Pro Group - AI demining, threat detection

    # Space
    'RDW',    # Redwire - Space infrastructure, solar arrays

    # Mining/Critical Minerals
    'TMC',    # The Metals Company - Deep-sea mining nickel/cobalt
    'NAK',    # Northern Dynasty - Pebble Mine copper/gold

    # Real Estate Tech
    'OPEN',   # Opendoor - AI-powered iBuying platform

    # Crypto/Energy
    'WULF',   # TeraWulf - Nuclear-powered Bitcoin mining

    # Batteries/EV
    'ENVX',   # Enovix - Next-gen silicon batteries
    'EOSE',   # Eos Energy - Grid-scale battery storage
    'QS',     # QuantumScape - Solid-state EV batteries
    'ABAT',   # American Battery Tech - Lithium recycling

    # eVTOL/Aviation
    'ACHR',   # Archer Aviation - Air taxis, LA Olympics 2028

    # AI Security
    'EVLV',   # Evolv Technologies - AI weapons detection

    # IoT/Fleet Tech
    'AIOT',   # PowerFleet - Connected vehicle management
    'LTRX',   # Lantronix - Edge computing, industrial IoT

    # AI Software/Semiconductors
    'POET',   # POET Technologies - Optical interposers for AI
    'VERI',   # Veritone - AI-powered media platform

    # AI/Defense
    'BBAI',   # BigBear.ai - Government AI analytics

    # Energy Infrastructure
    'WTTR',   # Select Water Solutions - Permian Basin water mgmt

    # =========================================================================
    # CORE UNIVERSE - Major caps and ETFs
    # =========================================================================
    'A', 'AAPL', 'AAUC', 'AAUKF', 'ABBRF', 'ABBV', 'ABCB', 'ABEV', 'ABNB', 'ABT',
    'ABZPY', 'ACAD', 'ACCMF', 'ACGL', 'ACN', 'ACTU', 'ADBE', 'ADI', 'ADM',
    'ADP', 'ADSK', 'ADTX', 'AEE', 'AEG', 'AEHL', 'AEM', 'AEP', 'AES',
    'AFBOF', 'AFL', 'AG', 'AGI', 'AGMH', 'AGRI', 'AGRZ', 'AIG',
    'AIR', 'AIRO', 'AIZ', 'AJG', 'AKAM', 'AL', 'ALAB', 'ALB', 'ALE', 'ALEX',
    'ALGN', 'ALL', 'ALLE', 'ALLR', 'ALLY', 'ALM', 'ALV', 'AM', 'AMAT',
    'AMBP', 'AMC', 'AMCR', 'AMD', 'AME', 'AMGN', 'AMKR', 'AMP', 'AMRK',
    'AMRRY', 'AMT', 'AMTM', 'AMVMF', 'AMZN', 'ANET', 'ANF', 'AON', 'AOS', 'APA',
    'APD', 'APH', 'APO', 'APP', 'APTV', 'APUS', 'APVO', 'AQB', 'AQMS', 'ARE',
    'ARM', 'ARMN', 'ARTV', 'ARWR', 'ASH', 'ASLE', 'ASM', 'ASML', 'ASND',
    'ASNS', 'ASTL', 'ASTS', 'ASX', 'ATAT', 'ATEC', 'ATI', 'ATMU', 'ATMV',
    'ATO', 'ATRC', 'ATRO', 'ATROB', 'ATUSF', 'AU', 'AUPH', 'AUR', 'AUTL', 'AVA',
    'AVAV', 'AVB', 'AVGO', 'AVNBF', 'AVVOF', 'AVVSY', 'AVY', 'AWK', 'AXON', 'AXP',
    'AXS', 'AXSM', 'AZ', 'AZN', 'AZO', 'B', 'BA', 'BABA', 'BAC', 'BAESY',
    'BALL', 'BAX', 'BBIO', 'BBVA', 'BBWI', 'BBY', 'BCCC', 'BCKIF',
    'BCKIY', 'BCS', 'BDNNY', 'BDRAF', 'BDRBF', 'BDRPF', 'BDRXF', 'BDSX', 'BDX', 'BEN',
    'BEP', 'BEPC', 'BETA', 'BF-B', 'BG', 'BHC', 'BHP', 'BHPLF', 'BIIB', 'BIOA',
    'BIPC', 'BITF', 'BITO', 'BK', 'BKCH', 'BKE', 'BKH', 'BKNG', 'BKR', 'BKU',
    'BLBX', 'BLDR', 'BLIDF', 'BLK', 'BLNE', 'BLSH', 'BMNR', 'BMO', 'BMY', 'BN',
    'BNPQY', 'BNS', 'BOLD', 'BOMBF', 'BP', 'BPOP', 'BR', 'BRAG', 'BRELY',
    'BRK-B', 'BRN', 'BRO', 'BRSGF', 'BRSL', 'BSX', 'BTAI', 'BTBD', 'BTC', 'BTDR',
    'BTG', 'BTI', 'BTM', 'BURU', 'BVN', 'BWA', 'BWAY', 'BWXT', 'BX',
    'BXP', 'BYD', 'BYRN', 'C', 'CADE', 'CAE', 'CAG', 'CAH', 'CAI', 'CAL',
    'CAN', 'CAPR', 'CAPS', 'CARR', 'CAT', 'CAVA', 'CB', 'CBOE', 'CBRE',
    'CCCX', 'CCEP', 'CCHH', 'CCI', 'CCJ', 'CCL', 'CDNS', 'CDRE', 'CDW',
    'CEG', 'CELH', 'CEPU', 'CETX', 'CF', 'CFG', 'CFRUY', 'CG', 'CHA', 'CHAI',
    'CHD', 'CHRS', 'CHRW', 'CHTR', 'CHWY', 'CHYM', 'CI', 'CINF', 'CL',
    'CLF', 'CLFD', 'CLRO', 'CLS', 'CLVT', 'CLX', 'CM', 'CMA', 'CMBT',
    'CMCLY', 'CMCSA', 'CMCT', 'CME', 'CMGMF', 'CMGMY', 'CMP', 'CMPX', 'CMS', 'CNC',
    'CNH', 'CNL', 'CNO', 'CNP', 'CNTY', 'COCO', 'COEP', 'COF', 'COHR', 'COHTF',
    'COIN', 'COKE', 'COLL', 'CONL', 'COO', 'COOT', 'COP', 'COR', 'COST', 'CPAY',
    'CPB', 'CPRT', 'CPT', 'CPWPF', 'CPXWF', 'CRBG', 'CRCL', 'CRDO', 'CRIS', 'CRL',
    'CRM', 'CRML', 'CRPJY', 'CRRSF', 'CRWD', 'CRWV', 'CSCO', 'CSGP', 'CSGS',
    'CSIQ', 'CSX', 'CTAS', 'CTGO', 'CTRA', 'CTRE', 'CTSH', 'CTVA', 'CUK', 'CVE',
    'CVI', 'CVS', 'CVX', 'CWEN', 'CWEN-A', 'D', 'DAL', 'DASH', 'DAY', 'DB',
    'DBOEY', 'DCO', 'DD', 'DDOG', 'DE', 'DECK', 'DEFT', 'DELL', 'DEO', 'DEVS',
    'DG', 'DGX', 'DHI', 'DHR', 'DINO', 'DIS', 'DJT', 'DLR', 'DLTR', 'DLXY',
    'DNN', 'DOV', 'DOW', 'DPZ', 'DRI', 'DRS', 'DRUG', 'DTE',
    'DTIL', 'DTM', 'DUAVF', 'DUK', 'DVA', 'DVN', 'DVS', 'DWMNF', 'DXCM',
    'DXF', 'EA', 'EADSF', 'EADSY', 'EBAY', 'ECL', 'ECX', 'ED', 'EDRWY', 'EFX',
    'EG', 'EGO', 'EH', 'EHMEF', 'EIX', 'EJH', 'EKSO', 'EL', 'ELF',
    'ELV', 'ELVR', 'EMBJ', 'EME', 'EMMS', 'EMR', 'ENB', 'ENSG', 'ENTO', 'EPAM',
    'EPD', 'EPWDF', 'EQIX', 'EQNR', 'EQR', 'EQT', 'ERDCF', 'ERIE', 'ERMAF', 'ERMAY',
    'ERO', 'ES', 'ESNT', 'ESS', 'ETHM', 'ETN', 'ETR', 'EVAX', 'EVEX', 'EVRG',
    'EVTL', 'EW', 'EWBC', 'EXALF', 'EXAS', 'EXC', 'EXE', 'EXEL', 'EXK', 'EXPD',
    'EXPE', 'EXR', 'EYE', 'EYUBY', 'F', 'FANG', 'FAST', 'FBYD', 'FCX', 'FDS',
    'FDX', 'FE', 'FFIV', 'FGI', 'FHI', 'FHN', 'FIEE', 'FINMF', 'FINMY',
    'FIS', 'FITB', 'FIX', 'FLNC', 'FLR', 'FLY', 'FMCC', 'FMX', 'FNB', 'FNV',
    'FOFO', 'FORD', 'FOUR', 'FOX', 'FOXA', 'FRMI', 'FRO', 'FRT', 'FSLR', 'FSM',
    'FTNT', 'FTS', 'FTV', 'FWRD', 'GAUZ', 'GCTK', 'GD', 'GDDY', 'GDIV', 'GE',
    'GEHC', 'GEN', 'GENI', 'GEOS', 'GEV', 'GFAI', 'GFI', 'GFS', 'GIBO',
    'GIL', 'GILD', 'GIS', 'GL', 'GLD', 'GLNCY', 'GLW', 'GM', 'GMAB', 'GME',
    'GNRC', 'GOLD', 'GOOG', 'GOOGL', 'GOTU', 'GPC', 'GPK', 'GPN', 'GRMN', 'GRRR',
    'GS', 'GSAT', 'GSK', 'GTX', 'GVA', 'GWAV', 'GWW', 'HAGHY', 'HAL', 'HAO',
    'HAS', 'HASI', 'HBAN', 'HBM', 'HCA', 'HCWB', 'HD', 'HEI', 'HELE', 'HG',
    'HHS', 'HIG', 'HII', 'HIMS', 'HL', 'HLT', 'HMY', 'HNRG', 'HNSDF',
    'HOLO', 'HOLX', 'HON', 'HOOD', 'HOVR', 'HP', 'HPE', 'HPIFY', 'HPQ', 'HRL',
    'HSIC', 'HST', 'HSY', 'HUBB', 'HUM', 'HUN', 'HUT', 'HWM', 'HXL', 'HYFT',
    'HYMC', 'HYPD', 'IAG', 'IBG', 'IBM', 'ICE', 'IDA', 'IDEXY', 'IDR',
    'IDXX', 'IE', 'IEX', 'IFF', 'ILKAY', 'IMNN', 'IMO', 'IMSR', 'INCY', 'INDV',
    'ING', 'INMB', 'INSM', 'INSP', 'INSW', 'INTC', 'INTR', 'INTT', 'INTU', 'INUV',
    'INVH', 'IONQ', 'IONS', 'IOT', 'IOTR', 'IP', 'IPG', 'IPX', 'IQV', 'IR',
    'IRBT', 'IREN', 'IRM', 'IRTC', 'ISRG', 'ISSC', 'IT', 'ITRG', 'ITUB', 'ITW',
    'IVDA', 'IVF', 'IVPAF', 'IVVD', 'IVZ', 'IWM', 'J', 'JAGX', 'JBDI', 'JBHT',
    'JBL', 'JCI', 'JD', 'JDZG', 'JFBR', 'JKHY', 'JMIA', 'JNJ', 'JPM', 'JSPR',
    'JXG', 'JXN', 'K', 'KALA', 'KAPA', 'KAR', 'KBGGY', 'KD', 'KDP', 'KEN',
    'KEP', 'KEWL', 'KEY', 'KEYS', 'KGC', 'KHC', 'KIM', 'KKR', 'KLAC', 'KMB',
    'KMI', 'KMT', 'KNSA', 'KO', 'KOSS', 'KPRX', 'KR', 'KRMD', 'KRMN', 'KSS',
    'KTOS', 'KTTA', 'KVUE', 'L', 'LAB', 'LAC', 'LAUR', 'LBRX', 'LCUT', 'LDOS',
    'LEA', 'LEN', 'LEU', 'LEVI', 'LGHL', 'LH', 'LHX', 'LII', 'LIMN',
    'LIN', 'LITM', 'LIVN', 'LKQ', 'LLY', 'LMT', 'LNC', 'LNG', 'LNT', 'LNZA',
    'LOAR', 'LOMA', 'LOW', 'LRCX', 'LSANF', 'LSF', 'LSIIF', 'LULU', 'LUNR', 'LUV',
    'LVS', 'LW', 'LXP', 'LYB', 'LYG', 'LYRA', 'LYSCF', 'LYV', 'MA', 'MAA',
    'MAGH', 'MAIN', 'MALJF', 'MALRF', 'MALRY', 'MAR', 'MAS', 'MBIO', 'MBND',
    'MCD', 'MCHB', 'MCHP', 'MCK', 'MCO', 'MCRB', 'MD', 'MDALF', 'MDCX', 'MDLZ',
    'MDT', 'MEHCQ', 'MELI', 'MET', 'META', 'MFC', 'MGA', 'MGM', 'MHK', 'MIMTF',
    'MIND', 'MIR', 'MJDLF', 'MKC', 'MLHKF', 'MLM', 'MLSPF', 'MMC', 'MMM', 'MMSMY',
    'MNDR', 'MNST', 'MO', 'MOB', 'MOBQ', 'MOG-A', 'MOH', 'MOS', 'MP',
    'MPC', 'MPLX', 'MPWR', 'MRCY', 'MRK', 'MRNA', 'MRVL', 'MS', 'MSCI', 'MSFT',
    'MSI', 'MSILF', 'MSTR', 'MTB', 'MTC', 'MTCH', 'MTD', 'MTG', 'MTRN', 'MTUAY',
    'MU', 'MUFG', 'MUX', 'MWYN', 'MYSZ', 'NATL', 'NB', 'NBIS', 'NBIX',
    'NCLH', 'NCNA', 'NDAQ', 'NDSN', 'NEE', 'NEM', 'NERV', 'NEWP', 'NEXA', 'NFE',
    'NFGC', 'NFLX', 'NGD', 'NGG', 'NGLOY', 'NGXXF', 'NHTC', 'NI', 'NIO',
    'NKE', 'NLY', 'NMRA', 'NOC', 'NOMA', 'NOV', 'NOW', 'NPK', 'NPWR',
    'NRG', 'NSC', 'NSKFF', 'NTAP', 'NTCT', 'NTES', 'NTRA', 'NTRP', 'NTRS', 'NU',
    'NUAI', 'NUE', 'NUKK', 'NUVL', 'NVA', 'NVDA', 'NVMI', 'NVO', 'NVR',
    'NVS', 'NVST', 'NWE', 'NWG', 'NWGL', 'NWN', 'NWS', 'NWSA', 'NXPI',
    'NXTT', 'O', 'OBLG', 'ODFL', 'OFAL', 'OGS', 'OKE', 'OKLO', 'OKUR', 'OMC',
    'OMF', 'OMH', 'ON', 'ONCO', 'ONEG', 'ONFO', 'ONMD', 'ONON',
    'OPXS', 'ORA', 'ORCL', 'ORI', 'ORLY', 'OSTTF', 'OTIS', 'OUT', 'OXY',
    'PAA', 'PAAS', 'PANW', 'PAPL', 'PARR', 'PATH', 'PAX', 'PAYC', 'PAYX', 'PCAR',
    'PCG', 'PDD', 'PEG', 'PEP', 'PFE', 'PFG', 'PG', 'PGNY', 'PGR', 'PH',
    'PHM', 'PHYS', 'PILL', 'PKE', 'PKG', 'PLD', 'PLRZ', 'PLTK', 'PLTR',
    'PM', 'PMAX', 'PNC', 'PNR', 'PNRG', 'PNW', 'PODD', 'POOL', 'POR', 'POWI',
    'PPCB', 'PPG', 'PPL', 'PRGO', 'PRMB', 'PRU', 'PRVA', 'PSA', 'PSHG', 'PSKY',
    'PSX', 'PTC', 'PTN', 'PTNDY', 'PTY', 'PUK', 'PWR', 'PYPL', 'Q', 'QBTS',
    'QCOM', 'QNTM', 'QNTQY', 'QQQ', 'QTTB', 'RBLX', 'RBOT', 'RCL', 'RDDT',
    'RDHL', 'RDN', 'REE', 'REG', 'REGN', 'REKR', 'RF', 'RGTI', 'RHHBY',
    'RIGL', 'RIO', 'RIVN', 'RJF', 'RKLB', 'RL', 'RMBS', 'RMD', 'RNMBY',
    'ROIV', 'ROK', 'ROL', 'ROP', 'ROST', 'RPRX', 'RR', 'RRR', 'RSG',
    'RTNTF', 'RTPPF', 'RTX', 'RVMD', 'RVTY', 'RWEOY', 'RWNFF', 'RY', 'RYCEY', 'RYOJ',
    'RYTM', 'RZLV', 'SA', 'SAABF', 'SAABY', 'SAFRY', 'SAN', 'SANM', 'SARO', 'SBAC',
    'SBCF', 'SBET', 'SBS', 'SBSW', 'SBUX', 'SCGLY', 'SCHW', 'SCNI', 'SCZMF', 'SEAT',
    'SER', 'SF', 'SGBX', 'SGGKF', 'SGGKY', 'SGI', 'SGML', 'SGN', 'SHEL', 'SHOP',
    'SHPH', 'SHW', 'SIF', 'SINT', 'SJM', 'SLB', 'SLE', 'SLNH', 'SLSR', 'SMCI',
    'SMFG', 'SMLR', 'SMMYY', 'SMTK', 'SMX', 'SNA', 'SNEX', 'SNGX', 'SNOW',
    'SNPS', 'SNT', 'SNTI', 'SO', 'SOBO', 'SOFI', 'SOGP', 'SOLS', 'SOLV', 'SONM',
    'SOUHY', 'SOUN', 'SOXX', 'SPG', 'SPGI', 'SPNT', 'SPR', 'SR', 'SRE',
    'SRL', 'SSKN', 'SSRM', 'STE', 'STEM', 'STKE', 'STKH', 'STLA', 'STLD',
    'STMNF', 'STT', 'STTSY', 'STX', 'STZ', 'SU', 'SUPN', 'SVM', 'SW', 'SWK',
    'SWKS', 'SYF', 'SYK', 'SYM', 'SYNX', 'SYY', 'T', 'TAC', 'TANH', 'TAP',
    'TARS', 'TATT', 'TBBB', 'TCBI', 'TCKRF', 'TD', 'TDG', 'TDS', 'TDY', 'TE',
    'TEAM', 'TECH', 'TECK', 'TEL', 'TEM', 'TER', 'TEVA', 'TFC', 'TFPM', 'TGEN',
    'TGT', 'THBRF', 'THC', 'THLEF', 'THLLY', 'TIL', 'TIRX', 'TJX', 'TKO', 'TLN',
    'TMO', 'TMQ', 'TMUS', 'TNIPF', 'TNK', 'TNL', 'TNXP', 'TPL', 'TPR',
    'TRAW', 'TREX', 'TRGP', 'TRI', 'TRIB', 'TRMB', 'TROW', 'TRP', 'TRUG', 'TRV',
    'TSCO', 'TSLA', 'TSM', 'TSN', 'TSSI', 'TT', 'TTD', 'TTMI', 'TTNNF', 'TTWO',
    'TWG', 'TXN', 'TXNM', 'TXT', 'TYL', 'U', 'UAL', 'UAMY', 'UBER', 'UBS',
    'UDR', 'UGI', 'UHS', 'UK', 'ULTA', 'UNH', 'UNP', 'UNPRF', 'UPS',
    'UPST', 'URBN', 'URI', 'USAR', 'USAS', 'USAU', 'USB', 'USFD', 'USGO', 'UTHR',
    'UUU', 'V', 'VEEE', 'VET', 'VGZ', 'VICI', 'VIPS', 'VIVK',
    'VLO', 'VLTO', 'VMC', 'VOO', 'VOYG', 'VRCA', 'VRMTF', 'VRSK', 'VRSN', 'VRT',
    'VRTX', 'VSEC', 'VST', 'VT', 'VTI', 'VTR', 'VTSI', 'VVX', 'VWAV',
    'VZ', 'VZLA', 'WAB', 'WAI', 'WAT', 'WBA', 'WBD', 'WBUY', 'WBX',
    'WDAY', 'WDC', 'WEC', 'WELL', 'WES', 'WFC', 'WIMI', 'WIT', 'WKSP', 'WM',
    'WMB', 'WMT', 'WOK', 'WOLF', 'WPM', 'WRB', 'WSM', 'WST', 'WTRG', 'WTW',
    'WWD', 'WY', 'WYNN', 'WYY', 'XBI', 'XEL', 'XLE', 'XLF',
    'XLP', 'XLRE', 'XLU', 'XLV', 'XOM', 'XYL', 'XYZ', 'XZJCF', 'YOU', 'YUM',
    'ZBH', 'ZBRA', 'ZTS', 'ZWS'
]


def get_default_tickers() -> List[str]:
    """Return the default trading universe."""
    return clean_ticker_list(DEFAULT_TICKERS)


# ============================================================================
# TTM SQUEEZE CALCULATION
# ============================================================================

def calculate_weekly_squeeze(df: pd.DataFrame, 
                             bb_length: int = 20, bb_mult: float = 2.0,
                             kc_length: int = 20, kc_mult: float = 1.2,  # Changed from 1.5 to 1.2
                             mom_length: int = 12) -> Dict:
    """
    Calculate TTM Squeeze on weekly data.
    
    KC mult = 1.2 matches "Mid Squeeze" threshold (ignores weak 1.5x squeezes).
    
    Returns dict with:
        - squeeze_on: Current bar in squeeze
        - squeeze_fired: Squeeze just released (was on, now off)
        - fire_direction: GREEN (bullish) or RED (bearish)
        - momentum: Current momentum value
        - momentum_accel: Momentum acceleration
        - bars_in_squeeze: How many bars squeeze was on
        - ready: 6+ bars in squeeze (ready to fire)
    """
    if df is None or len(df) < max(bb_length, kc_length, mom_length) + 5:
        return None
    
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
    atr = tr.ewm(span=kc_length, adjust=False).mean()
    
    kc_mid = close.rolling(kc_length).mean()
    kc_upper = kc_mid + (kc_mult * atr)
    kc_lower = kc_mid - (kc_mult * atr)
    
    # --- Squeeze Detection ---
    squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)
    
    # --- TRUE TTM SQUEEZE MOMENTUM ---
    # Midline = average of (Donchian midline + SMA)
    highest_high = high.rolling(mom_length).max()
    lowest_low = low.rolling(mom_length).min()
    donchian_mid = (highest_high + lowest_low) / 2
    sma_close = close.rolling(mom_length).mean()
    
    # TTM Midline = average of Donchian mid and SMA
    ttm_midline = (donchian_mid + sma_close) / 2
    
    # Momentum = Linear regression of (close - ttm_midline)
    deviation = close - ttm_midline
    
    def linreg_value(x):
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
    
    # Squeeze fired = was on, now off
    squeeze_fired = prev_squeeze and not current_squeeze
    
    # Count bars in squeeze (looking back)
    # If currently in squeeze, start from current bar
    # If squeeze just fired, start from previous bar
    bars_in_squeeze = 0
    start_idx = 2 if squeeze_fired else 1
    
    for i in range(start_idx, min(50, len(squeeze_on))):
        if squeeze_on.iloc[-i]:
            bars_in_squeeze += 1
        else:
            break
    
    # Fire direction
    fire_direction = None
    if squeeze_fired:
        fire_direction = 'GREEN' if current_mom > 0 else 'RED'
    
    # Momentum acceleration
    mom_accel = current_mom - prev_mom if not pd.isna(prev_mom) else 0
    
    # Ready = currently in squeeze with 6+ bars
    ready = current_squeeze and bars_in_squeeze >= 6
    
    # Get last 10 bars of squeeze status for debugging
    squeeze_history = squeeze_on.tail(10).tolist()
    bar_dates = [str(d.date()) for d in df.index[-10:]]
    
    # Check if data is stale (most recent bar is more than 10 days old)
    last_bar_date = df.index[-1].date()
    days_old = (datetime.now().date() - last_bar_date).days
    data_stale = days_old > 10
    
    return {
        'squeeze_on': bool(current_squeeze),
        'prev_squeeze': bool(prev_squeeze),
        'squeeze_fired': bool(squeeze_fired),
        'fire_direction': fire_direction,
        'momentum': float(current_mom) if not pd.isna(current_mom) else 0,
        'momentum_accel': float(mom_accel) if not pd.isna(mom_accel) else 0,
        'bars_in_squeeze': bars_in_squeeze,
        'ready': ready,
        'momentum_rising': current_mom > prev_mom if not pd.isna(prev_mom) else False,
        'momentum_positive': current_mom > 0,
        'current_price': float(close.iloc[-1]),
        'prev_close': float(close.iloc[-2]) if len(close) > 1 else 0,
        'squeeze_history': squeeze_history,
        'bar_dates': bar_dates,
        'data_stale': data_stale,
        'last_bar_date': str(last_bar_date)
    }


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_weekly_data(symbol: str, weeks: int = 52) -> Optional[Tuple[pd.DataFrame, str]]:
    """
    Fetch daily data and resample to weekly for most current data.
    Also returns sector to avoid extra API calls later.

    Native yfinance weekly data often lags by a week. By fetching daily
    and resampling to Friday close, we get the most recent complete week.

    Returns: (DataFrame, sector) tuple or None
    """
    try:
        ticker = yf.Ticker(symbol)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=weeks * 7 + 30)

        # Get sector from info (same API call)
        try:
            info = ticker.info
            sector = info.get('sector') or info.get('category') or 'Unknown'
        except Exception:
            sector = 'Unknown'

        # Fetch daily data
        df = ticker.history(start=start_date, end=end_date, interval='1d')

        if df is None or df.empty:
            return None

        # Resample to weekly (Friday close)
        df = df.resample('W-FRI').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()

        if len(df) < 20:
            return None

        return (df, sector)
    except Exception as e:
        return None


def analyze_symbol(symbol: str) -> Optional[Dict]:
    """Analyze a single symbol for weekly squeeze status."""
    import time
    import random

    # Small random delay to avoid Yahoo rate limiting (0.05-0.15 seconds)
    time.sleep(random.uniform(0.05, 0.15))

    try:
        result = fetch_weekly_data(symbol)
        if result is None:
            return None

        df, sector = result

        squeeze_data = calculate_weekly_squeeze(df)
        if squeeze_data is None:
            return None

        # Add symbol, sector, and price info
        squeeze_data['symbol'] = symbol
        squeeze_data['sector'] = sector
        squeeze_data['weekly_change_pct'] = (
            (squeeze_data['current_price'] - squeeze_data['prev_close']) /
            squeeze_data['prev_close'] * 100
        ) if squeeze_data['prev_close'] > 0 else 0

        return squeeze_data
    except Exception as e:
        return None


# ============================================================================
# SCANNER
# ============================================================================

def scan_for_squeeze_fires(symbols: List[str], max_workers: int = 10) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Scan symbols for weekly squeeze fires.
    
    Returns:
        fired_green: List of stocks where squeeze fired GREEN (bullish)
        fired_red: List of stocks where squeeze fired RED (bearish)
        ready_to_fire: List of stocks ready to fire (6+ bars in squeeze)
        in_squeeze: List of stocks currently in squeeze
    """
    fired_green = []
    fired_red = []
    ready_to_fire = []
    in_squeeze = []
    
    total = len(symbols)
    completed = 0
    
    print(f"\n🔍 Scanning {total} symbols for weekly squeeze fires...\n")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_symbol, sym): sym for sym in symbols}
        
        for future in as_completed(futures):
            completed += 1
            symbol = futures[future]
            
            # Progress indicator
            if completed % 50 == 0 or completed == total:
                print(f"   Progress: {completed}/{total} ({100*completed//total}%)")
            
            try:
                result = future.result()
                if result is None:
                    continue
                
                if result['squeeze_fired']:
                    if result['fire_direction'] == 'GREEN':
                        fired_green.append(result)
                    else:
                        fired_red.append(result)
                elif result['ready']:
                    ready_to_fire.append(result)
                elif result['squeeze_on']:
                    in_squeeze.append(result)
                    
            except Exception as e:
                continue
    
    # Sort by momentum
    fired_green.sort(key=lambda x: x['momentum'], reverse=True)
    fired_red.sort(key=lambda x: x['momentum'])
    ready_to_fire.sort(key=lambda x: x['bars_in_squeeze'], reverse=True)
    in_squeeze.sort(key=lambda x: x['momentum'], reverse=True)
    
    return fired_green, fired_red, ready_to_fire, in_squeeze


# ============================================================================
# OUTPUT FORMATTING
# ============================================================================

def print_results(fired_green: List[Dict], fired_red: List[Dict], 
                  ready_to_fire: List[Dict], in_squeeze: List[Dict]):
    """Print scan results in a formatted table."""
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    print(f"\n{'='*70}")
    print(f"  WEEKEND SQUEEZE SCANNER RESULTS - {now}")
    print(f"{'='*70}")
    
    # GREEN fires
    print(f"\n🟢 SQUEEZE FIRED GREEN (Bullish Breakouts) - {len(fired_green)} found")
    print("-" * 70)
    if fired_green:
        print(f"{'Symbol':<10} {'Price':>10} {'Wk Chg':>8} {'Momentum':>10} {'Mom Accel':>10} {'Bars':>6}")
        print("-" * 70)
        for stock in fired_green[:20]:
            print(f"{stock['symbol']:<10} ${stock['current_price']:>8.2f} {stock['weekly_change_pct']:>+7.1f}% "
                  f"{stock['momentum']:>10.2f} {stock['momentum_accel']:>+10.2f} {stock['bars_in_squeeze']:>6}")
        if len(fired_green) > 20:
            print(f"  ... and {len(fired_green) - 20} more")
    
    # RED fires
    print(f"\n🔴 SQUEEZE FIRED RED (Bearish Breakdowns) - {len(fired_red)} found")
    print("-" * 70)
    if fired_red:
        print(f"{'Symbol':<10} {'Price':>10} {'Wk Chg':>8} {'Momentum':>10} {'Mom Accel':>10} {'Bars':>6}")
        print("-" * 70)
        for stock in fired_red[:10]:
            print(f"{stock['symbol']:<10} ${stock['current_price']:>8.2f} {stock['weekly_change_pct']:>+7.1f}% "
                  f"{stock['momentum']:>10.2f} {stock['momentum_accel']:>+10.2f} {stock['bars_in_squeeze']:>6}")
        if len(fired_red) > 10:
            print(f"  ... and {len(fired_red) - 10} more")
    
    # Ready to fire
    print(f"\n⏳ READY TO FIRE (6+ bars in squeeze) - {len(ready_to_fire)} found")
    print("-" * 70)
    if ready_to_fire:
        print(f"{'Symbol':<10} {'Price':>10} {'Momentum':>10} {'Rising':>8} {'Bars':>6}")
        print("-" * 70)
        for stock in ready_to_fire[:15]:
            rising = "✅" if stock['momentum_rising'] else "❌"
            print(f"{stock['symbol']:<10} ${stock['current_price']:>8.2f} {stock['momentum']:>10.2f} "
                  f"{rising:>8} {stock['bars_in_squeeze']:>6}")
        if len(ready_to_fire) > 15:
            print(f"  ... and {len(ready_to_fire) - 15} more")
    
    # In squeeze
    print(f"\n🔵 CURRENTLY IN SQUEEZE (building) - {len(in_squeeze)} found")
    print("-" * 70)
    if in_squeeze:
        print(f"{'Symbol':<10} {'Price':>10} {'Momentum':>10} {'Positive':>10}")
        print("-" * 70)
        for stock in in_squeeze[:10]:
            pos = "✅" if stock['momentum_positive'] else "❌"
            print(f"{stock['symbol']:<10} ${stock['current_price']:>8.2f} {stock['momentum']:>10.2f} {pos:>10}")
        if len(in_squeeze) > 10:
            print(f"  ... and {len(in_squeeze) - 10} more")
    
    # Summary
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"  🟢 Fired GREEN (bullish):  {len(fired_green)}")
    print(f"  🔴 Fired RED (bearish):    {len(fired_red)}")
    print(f"  ⏳ Ready to fire:          {len(ready_to_fire)}")
    print(f"  🔵 In squeeze:             {len(in_squeeze)}")
    print(f"{'='*70}")


def save_results_to_csv(fired_green: List[Dict], fired_red: List[Dict],
                        ready_to_fire: List[Dict], in_squeeze: List[Dict]):
    """Save scan results to CSV files."""
    date_str = datetime.now().strftime("%Y%m%d")
    
    if fired_green:
        df = pd.DataFrame(fired_green)
        filename = f"squeeze_fired_green_{date_str}.csv"
        df.to_csv(filename, index=False)
        print(f"📁 Saved {len(fired_green)} GREEN fires to {filename}")
    
    if fired_red:
        df = pd.DataFrame(fired_red)
        filename = f"squeeze_fired_red_{date_str}.csv"
        df.to_csv(filename, index=False)
        print(f"📁 Saved {len(fired_red)} RED fires to {filename}")
    
    if ready_to_fire:
        df = pd.DataFrame(ready_to_fire)
        filename = f"squeeze_ready_{date_str}.csv"
        df.to_csv(filename, index=False)
        print(f"📁 Saved {len(ready_to_fire)} ready-to-fire to {filename}")


# ============================================================================
# SUNDAY RANKINGS
# ============================================================================

def calculate_sunday_score(stock: Dict) -> float:
    """
    Calculate a composite score for Sunday night ranking.
    Higher = better setup for Monday entry.
    """
    score = 0
    
    # Momentum strength (0-40 points)
    mom_score = min(abs(stock['momentum']) / 30 * 40, 40)
    score += mom_score
    
    # Momentum acceleration (0-20 points)
    accel_score = min(max(stock['momentum_accel'], 0) / 5 * 20, 20)
    score += accel_score
    
    # Squeeze duration (0-20 points)
    bars_score = min(stock['bars_in_squeeze'] / 12 * 20, 20)
    score += bars_score
    
    # Weekly confirmation (0-20 points)
    weekly_score = min(abs(stock['weekly_change_pct']) / 10 * 20, 20)
    score += weekly_score
    
    return score


def print_sunday_rankings(fired_green: List[Dict], top_n: int = 15) -> List[Dict]:
    """Print Sunday night rankings for GREEN fires."""
    
    # Calculate scores
    for stock in fired_green:
        stock['sunday_score'] = calculate_sunday_score(stock)
    
    # Sort by score
    ranked = sorted(fired_green, key=lambda x: x['sunday_score'], reverse=True)
    
    print(f"\n{'='*80}")
    print(f"  🌙 SUNDAY NIGHT RANKINGS — Top {min(top_n, len(ranked))} Monday Setups")
    print(f"{'='*80}")
    print(f"\n  Scoring: Momentum + Acceleration + Squeeze Duration + Weekly Confirmation")
    print(f"  Higher score = Better setup for Monday entry\n")
    
    print(f"{'Rank':<6} {'Symbol':<8} {'Score':>8} {'Price':>10} {'Mom':>10} {'Accel':>8} {'Bars':>6} {'Wk%':>8}")
    print("-" * 80)
    
    for i, stock in enumerate(ranked[:top_n], 1):
        # Color coding hint based on score
        if stock['sunday_score'] >= 70:
            tier = "🔥"  # Hot
        elif stock['sunday_score'] >= 50:
            tier = "✅"  # Good
        elif stock['sunday_score'] >= 30:
            tier = "⚠️"   # Okay
        else:
            tier = "❄️"   # Cold
        
        print(f"{tier} {i:<4} {stock['symbol']:<8} {stock['sunday_score']:>8.1f} "
              f"${stock['current_price']:>8.2f} {stock['momentum']:>10.2f} "
              f"{stock['momentum_accel']:>+8.2f} {stock['bars_in_squeeze']:>6} "
              f"{stock['weekly_change_pct']:>+7.1f}%")
    
    if len(ranked) > top_n:
        print(f"\n  ... and {len(ranked) - top_n} more")
    
    # Summary stats
    print(f"\n{'-'*80}")
    print(f"  📊 Score Distribution:")
    
    hot = len([s for s in ranked if s['sunday_score'] >= 70])
    good = len([s for s in ranked if 50 <= s['sunday_score'] < 70])
    okay = len([s for s in ranked if 30 <= s['sunday_score'] < 50])
    cold = len([s for s in ranked if s['sunday_score'] < 30])
    
    print(f"     🔥 Hot (70+):    {hot}")
    print(f"     ✅ Good (50-69): {good}")
    print(f"     ⚠️  Okay (30-49): {okay}")
    print(f"     ❄️  Cold (<30):   {cold}")
    
    # Top 3 recommendation
    print(f"\n{'='*80}")
    print(f"  🎯 TOP 3 MONDAY CANDIDATES")
    print(f"{'='*80}")
    
    for i, stock in enumerate(ranked[:3], 1):
        print(f"\n  {i}. {stock['symbol']} — Score: {stock['sunday_score']:.1f}")
        print(f"     Price: ${stock['current_price']:.2f} | Week: {stock['weekly_change_pct']:+.1f}%")
        print(f"     Momentum: {stock['momentum']:.2f} | Accel: {stock['momentum_accel']:+.2f}")
        print(f"     Squeeze Duration: {stock['bars_in_squeeze']} bars")
        
        # Quick analysis
        strengths = []
        if stock['momentum'] > 20:
            strengths.append("Strong momentum")
        if stock['momentum_accel'] > 3:
            strengths.append("Accelerating")
        if stock['bars_in_squeeze'] >= 10:
            strengths.append("Long squeeze")
        if stock['weekly_change_pct'] > 5:
            strengths.append("Big weekly move")
        if stock['momentum_rising']:
            strengths.append("Rising")
        
        if strengths:
            print(f"     ✨ {', '.join(strengths)}")
    
    print(f"\n{'='*80}\n")
    
    return ranked


def save_sunday_rankings(ranked: List[Dict], filename: str = None):
    """Save Sunday rankings to CSV."""
    if filename is None:
        filename = f"sunday_rankings_{datetime.now().strftime('%Y%m%d')}.csv"
    
    if ranked:
        df = pd.DataFrame(ranked)
        cols = ['symbol', 'sunday_score', 'current_price', 'weekly_change_pct',
                'momentum', 'momentum_accel', 'bars_in_squeeze', 
                'momentum_rising', 'momentum_positive']
        df = df[[c for c in cols if c in df.columns]]
        df = df.sort_values('sunday_score', ascending=False)
        df.to_csv(filename, index=False)
        print(f"📁 Sunday rankings saved to {filename}")
    
    return filename


# ============================================================================
# INDIVIDUAL STOCK ANALYSIS
# ============================================================================

def print_single_stock_analysis(symbol: str, result: Dict, debug: bool = False):
    """Print detailed analysis for a single stock."""
    print(f"\n{'='*60}")
    print(f"  📊 {symbol} — SQUEEZE ANALYSIS")
    print(f"{'='*60}")
    
    if result is None:
        print(f"  ❌ Could not fetch data for {symbol}")
        return
    
    # Debug output
    if debug:
        print(f"\n  [DEBUG]")
        print(f"  squeeze_on (current bar): {result.get('squeeze_on')}")
        print(f"  squeeze_fired: {result.get('squeeze_fired')}")
        print(f"  prev_squeeze: {result.get('prev_squeeze', 'N/A')}")
        print(f"  bars_in_squeeze: {result.get('bars_in_squeeze')}")
        if 'squeeze_history' in result:
            print(f"  Last 10 bars squeeze (oldest->newest): {result['squeeze_history']}")
            print(f"  Bar dates: {result['bar_dates']}")
        if result.get('data_stale'):
            print(f"  ⚠️  DATA STALE! Last bar: {result.get('last_bar_date')} - Results may be outdated!")
    
    print(f"\n  Price:  ${result['current_price']:.2f}")
    print(f"  Week:   {result['weekly_change_pct']:+.1f}%")
    
    # Squeeze status
    print(f"\n  {'─'*40}")
    print(f"  SQUEEZE STATUS")
    print(f"  {'─'*40}")
    
    if result['squeeze_fired']:
        direction = "🟢 GREEN (Bullish)" if result['momentum_positive'] else "🔴 RED (Bearish)"
        print(f"  🔥 SQUEEZE FIRED! — {direction}")
    elif result['squeeze_on']:
        print(f"  🟡 IN SQUEEZE — Compression active")
        print(f"     Bars in squeeze: {result['bars_in_squeeze']}")
        if result['ready']:
            print(f"     ⚠️  READY TO FIRE — 6+ bars, watch for breakout!")
    else:
        print(f"  ⚪ NO SQUEEZE — Volatility expanded")
    
    # Momentum details
    print(f"\n  {'─'*40}")
    print(f"  MOMENTUM")
    print(f"  {'─'*40}")
    print(f"  Current:      {result['momentum']:.2f}")
    print(f"  Acceleration: {result['momentum_accel']:+.2f}")
    print(f"  Direction:    {'📈 Rising' if result['momentum_rising'] else '📉 Falling'}")
    print(f"  Polarity:     {'Positive' if result['momentum_positive'] else 'Negative'}")
    
    # Score (if squeeze fired)
    if result['squeeze_fired']:
        # Calculate sunday_score inline
        mom_score = min(abs(result['momentum']) / 30 * 40, 40)
        accel_score = min(max(result['momentum_accel'], 0) / 5 * 20, 20)
        bars_score = min(result['bars_in_squeeze'] / 12 * 20, 20)
        weekly_score = min(abs(result['weekly_change_pct']) / 10 * 20, 20)
        sunday_score = mom_score + accel_score + bars_score + weekly_score
        
        print(f"\n  {'─'*40}")
        print(f"  SUNDAY SCORE: {sunday_score:.1f}/100")
        print(f"  {'─'*40}")
        
        if sunday_score >= 70:
            print(f"  🔥 HOT — Strong candidate for Monday")
        elif sunday_score >= 50:
            print(f"  ✅ GOOD — Worth watching")
        elif sunday_score >= 30:
            print(f"  ⚠️  OKAY — Proceed with caution")
        else:
            print(f"  ❄️  COLD — Weak setup")
        
        # Strengths
        strengths = []
        if result['momentum'] > 20:
            strengths.append("Strong momentum")
        if result['momentum_accel'] > 3:
            strengths.append("Accelerating")
        if result['bars_in_squeeze'] >= 10:
            strengths.append("Long squeeze duration")
        if result['weekly_change_pct'] > 5:
            strengths.append("Big weekly move")
        if result['momentum_rising']:
            strengths.append("Rising momentum")
        
        if strengths:
            print(f"\n  ✨ Strengths: {', '.join(strengths)}")
        
        # Concerns
        concerns = []
        if result['momentum'] < 5:
            concerns.append("Weak momentum")
        if result['momentum_accel'] < 0:
            concerns.append("Decelerating")
        if result['bars_in_squeeze'] < 4:
            concerns.append("Short squeeze")
        if not result['momentum_rising']:
            concerns.append("Momentum fading")
        
        if concerns:
            print(f"  ⚠️  Concerns: {', '.join(concerns)}")
    
    print(f"\n{'='*60}\n")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Weekend Squeeze Scanner v2.0')
    parser.add_argument('-s', '--stock', nargs='+', type=str,
                        help='Individual stock(s) to analyze (e.g., -s AAPL or -s AAPL NVDA TSLA)')
    parser.add_argument('--universe', type=str, default='swing',
                        choices=['swing', 'sp500', 'nasdaq100', 'custom'],
                        help='Stock universe to scan')
    parser.add_argument('--symbols', nargs='+', type=str,
                        help='Custom symbols to scan (use with --universe custom)')
    parser.add_argument('--workers', type=int, default=10,
                        help='Number of parallel workers')
    parser.add_argument('--save', action='store_true',
                        help='Save results to CSV')
    parser.add_argument('--sunday', action='store_true',
                        help='Run Sunday night ranking analysis on GREEN fires')
    parser.add_argument('--top', type=int, default=15,
                        help='Number of top stocks to show in Sunday rankings')
    parser.add_argument('--debug', action='store_true',
                        help='Show debug info for squeeze detection')
    parser.add_argument('--no-airtable', action='store_true',
                        help='Skip Airtable sync')
    parser.add_argument('--tickers', nargs='+', type=str,
                        help='Specific tickers to scan (overrides universe)')

    args = parser.parse_args()
    
    # Individual stock mode
    if args.stock:
        symbols = [s.upper() for s in args.stock]
        print(f"\n{'='*60}")
        print(f"  INDIVIDUAL STOCK ANALYSIS")
        print(f"  Symbols: {', '.join(symbols)}")
        print(f"{'='*60}")
        
        for symbol in symbols:
            result = analyze_symbol(symbol)
            print_single_stock_analysis(symbol, result, debug=args.debug)
        
        return
    
    # Get symbols based on universe selection or --tickers flag
    if args.tickers:
        # --tickers flag overrides universe
        symbols = [t.upper() for t in args.tickers]
        print(f"📊 Using custom ticker list: {len(symbols)} symbols")
    elif args.universe == 'sp500':
        symbols = get_sp500_symbols()
    elif args.universe == 'nasdaq100':
        symbols = get_nasdaq100_symbols()
    elif args.universe == 'custom':
        if not args.symbols:
            print("❌ --symbols required when using --universe custom")
            return
        symbols = args.symbols
    else:  # swing (default)
        symbols = get_swing_universe()
    
    # Remove duplicates
    symbols = list(set(symbols))
    
    print(f"\n{'='*70}")
    print(f"  WEEKEND SQUEEZE SCANNER v2.0")
    print(f"  Universe: {args.universe.upper()} ({len(symbols)} symbols)")
    print(f"{'='*70}")
    
    # Run scanner
    fired_green, fired_red, ready_to_fire, in_squeeze = scan_for_squeeze_fires(
        symbols, max_workers=args.workers
    )
    
    # Print results
    print_results(fired_green, fired_red, ready_to_fire, in_squeeze)
    
    # Sunday night rankings
    if args.sunday and fired_green:
        ranked = print_sunday_rankings(fired_green, top_n=args.top)
        
        if args.save:
            save_sunday_rankings(ranked)
    elif args.sunday and not fired_green:
        print("\n⚠️  No GREEN fires found — nothing to rank for Sunday")
    
    # Save to CSV if requested (original format)
    if args.save:
        save_results_to_csv(fired_green, fired_red, ready_to_fire, in_squeeze)

    # Push to Airtable (unless --no-airtable flag)
    if not args.no_airtable:
        push_squeeze_signals_to_airtable(fired_green, fired_red, ready_to_fire, in_squeeze)
    else:
        print("\n📡 Airtable sync skipped (--no-airtable flag)")


if __name__ == "__main__":
    main()
