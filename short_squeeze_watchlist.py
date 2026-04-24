#!/usr/bin/env python3
"""
Short Squeeze Watchlist — Phase 1
==================================
Identifies short-squeeze candidates BEFORE activation using three free data sources:
  1. yfinance — SI% of float, days to cover, float size, SI month-over-month change
  2. Finnhub  — Insider sentiment (MSPR)
  3. FINRA    — Daily short volume trend (5d/20d ratio)

Scores each candidate 0-100 and grades A/B/C.
Pushes top 100 to Airtable "Short_Squeeze_Watchlist" table.

Airtable Schema (create manually):
  - Symbol          (primary, single line text)
  - Price           (currency)
  - Score           (number, 0-100)
  - Grade           (single select: A, B, C)
  - SI % Float      (percent)
  - Days to Cover   (number)
  - Short Vol Trend (number)
  - MSPR            (number, -100 to 100)
  - Float Shares    (number)
  - SI Change MoM   (percent)
  - Last Updated    (date)
  - Notes           (long text)

Usage:
  python short_squeeze_watchlist.py                     # Full universe scan
  python short_squeeze_watchlist.py --limit 50          # Quick test with 50 symbols
  python short_squeeze_watchlist.py --symbols CAR GME   # Specific tickers
  python short_squeeze_watchlist.py --no-airtable       # Skip Airtable push
"""

import argparse
import os
import time
import urllib.parse
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
warnings.filterwarnings('ignore')

# Reuse symbol loader
from weekend_squeeze_scanner import get_swing_universe, sanitize_number


# ============================================================================
# CONFIG
# ============================================================================

AT_BASE = "appIUFp3KFrf8KXez"
AT_API = os.getenv("AT_API", "")
AT_TABLE = "Short_Squeeze_Watchlist"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
AIRTABLE_BATCH_SIZE = 10

# HTTP session with retry
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PATCH", "DELETE"],
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

AT_HEADERS = {
    "Authorization": f"Bearer {AT_API}",
    "Content-Type": "application/json",
}


# ============================================================================
# DATA FETCHING — yfinance
# ============================================================================

def fetch_yfinance_short_data(symbol: str) -> Optional[Dict]:
    """
    Fetch short interest data from yfinance.
    Returns dict with si_pct, days_to_cover, shares_short, float_shares, si_change_mom, price.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        shares_short = info.get('sharesShort', 0) or 0
        shares_short_prior = info.get('sharesShortPriorMonth', 0) or 0
        float_shares = info.get('floatShares', 0) or 0
        si_pct = info.get('shortPercentOfFloat', 0) or 0
        short_ratio = info.get('shortRatio', 0) or 0  # days to cover
        price = info.get('currentPrice') or info.get('regularMarketPrice', 0) or 0

        # SI month-over-month change
        si_change_mom = 0.0
        if shares_short_prior and shares_short_prior > 0:
            si_change_mom = (shares_short - shares_short_prior) / shares_short_prior

        return {
            'si_pct': float(si_pct),
            'days_to_cover': float(short_ratio),
            'shares_short': int(shares_short),
            'float_shares': int(float_shares),
            'si_change_mom': float(si_change_mom),
            'price': float(price),
        }
    except Exception:
        return None


# ============================================================================
# DATA FETCHING — Finnhub MSPR
# ============================================================================

def fetch_finnhub_mspr(symbol: str) -> Optional[float]:
    """
    Fetch most recent Monthly Share Purchase Ratio from Finnhub.
    MSPR ranges from -100 to 100. Positive = insiders buying.
    Respects 60 req/min rate limit via caller-side throttle.
    """
    if not FINNHUB_API_KEY:
        return None

    try:
        url = "https://finnhub.io/api/v1/stock/insider-sentiment"
        params = {
            'symbol': symbol,
            'from': (date.today() - timedelta(days=90)).isoformat(),
            'to': date.today().isoformat(),
            'token': FINNHUB_API_KEY,
        }
        resp = session.get(url, params=params, timeout=15)
        if resp.status_code == 429:
            time.sleep(2)
            resp = session.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        entries = data.get('data', [])
        if not entries:
            return None

        # Return most recent month's MSPR
        latest = entries[-1]
        return float(latest.get('mspr', 0))
    except Exception:
        return None


# ============================================================================
# DATA FETCHING — FINRA Short Volume
# ============================================================================

def _parse_finra_file(text: str, check_date: date) -> Dict[str, Dict]:
    """Parse a FINRA short volume text file into {SYMBOL: {short_volume, total_volume, short_ratio}}."""
    result = {}
    for line in text.strip().split('\n'):
        parts = line.split('|')
        if len(parts) < 6 or parts[0] == 'Date':
            continue
        symbol = parts[1].strip()
        try:
            short_vol = int(float(parts[2]))
            total_vol = int(float(parts[4]))
            short_ratio = short_vol / total_vol if total_vol > 0 else 0
            # Merge: if symbol already seen from another file, sum volumes
            if symbol in result:
                result[symbol]['short_volume'] += short_vol
                result[symbol]['total_volume'] += total_vol
                tv = result[symbol]['total_volume']
                result[symbol]['short_ratio'] = result[symbol]['short_volume'] / tv if tv > 0 else 0
            else:
                result[symbol] = {
                    'short_volume': short_vol,
                    'total_volume': total_vol,
                    'short_ratio': short_ratio,
                    'date': check_date.isoformat(),
                }
        except (ValueError, ZeroDivisionError):
            continue
    return result


def fetch_finra_short_volume(target_date: date = None, max_lookback: int = 5) -> Dict[str, Dict]:
    """
    Download FINRA daily short volume reports (CNMS + FNAQ).
    Merges both files to cover NYSE and NASDAQ-listed symbols.
    Returns {SYMBOL: {short_volume, total_volume, short_ratio}}.
    Falls back up to max_lookback days if target date 404s.
    """
    if target_date is None:
        target_date = date.today()

    # CNMS = Combined NMS (mostly NYSE/Arca), FNSQshvol = NASDAQ
    file_prefixes = ['CNMSshvol', 'FNSQshvol']

    for days_back in range(max_lookback):
        check_date = target_date - timedelta(days=days_back)
        if check_date.weekday() >= 5:
            continue

        date_str = check_date.strftime('%Y%m%d')
        merged = {}

        for prefix in file_prefixes:
            url = f"https://cdn.finra.org/equity/regsho/daily/{prefix}{date_str}.txt"
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code not in [200]:
                    continue
                parsed = _parse_finra_file(resp.text, check_date)
                # Merge into combined result
                for sym, data in parsed.items():
                    if sym in merged:
                        merged[sym]['short_volume'] += data['short_volume']
                        merged[sym]['total_volume'] += data['total_volume']
                        tv = merged[sym]['total_volume']
                        merged[sym]['short_ratio'] = merged[sym]['short_volume'] / tv if tv > 0 else 0
                    else:
                        merged[sym] = data
            except Exception:
                continue

        if merged:
            print(f"   FINRA data loaded for {check_date.isoformat()} ({len(merged)} symbols)")
            return merged

    print("   ⚠️  No FINRA short volume data available in lookback window")
    return {}


def fetch_finra_history(days: int = 20) -> List[Dict[str, Dict]]:
    """
    Fetch multiple days of FINRA short volume data for trend calculation.
    Returns list of daily dicts, most recent first.
    """
    history = []
    check_date = date.today()
    attempts = 0
    max_attempts = days + 15  # account for weekends/holidays

    while len(history) < days and attempts < max_attempts:
        check_date -= timedelta(days=1)
        attempts += 1

        if check_date.weekday() >= 5:
            continue

        daily = fetch_finra_short_volume(check_date, max_lookback=1)
        if daily:
            history.append(daily)

    print(f"   FINRA history: {len(history)} trading days loaded")
    return history


def compute_finra_trend(symbol: str, finra_history: List[Dict[str, Dict]]) -> Optional[float]:
    """
    Compute (5-day avg short ratio) / (20-day avg short ratio).
    > 1.0 = shorts piling in; < 1.0 = shorts covering.
    """
    ratios = []
    for daily in finra_history:
        if symbol in daily:
            ratios.append(daily[symbol]['short_ratio'])

    if len(ratios) < 5:
        return None

    avg_5d = np.mean(ratios[:5])
    avg_20d = np.mean(ratios[:20])

    if avg_20d <= 0:
        return None

    return float(avg_5d / avg_20d)


# ============================================================================
# SCORING
# ============================================================================

def score_candidate(data: Dict) -> Dict:
    """
    Score a short squeeze candidate 0-100.

    Weights:
      - SI % of Float:        30%  (0% = 0, 50%+ = max)
      - Days to Cover:        20%  (0 = 0, 10+ = max)
      - FINRA short vol trend: 20%  (reward > 1.0, penalize < 0.8)
      - Insider MSPR:         15%  (reward > 50, penalize < -50)
      - Float size (inverse): 15%  (<50M = max, >500M = 0)

    Returns {score, components, grade}.
    """
    components = {}

    # 1. SI % of Float (30%)
    si_pct = data.get('si_pct', 0) * 100  # convert from decimal (0.15 → 15)
    si_score = min(si_pct / 50.0, 1.0) * 100
    components['si_pct'] = round(si_score, 1)

    # 2. Days to Cover (20%)
    dtc = data.get('days_to_cover', 0)
    dtc_score = min(dtc / 10.0, 1.0) * 100
    components['days_to_cover'] = round(dtc_score, 1)

    # 3. FINRA short volume trend (20%)
    trend = data.get('finra_trend')
    if trend is not None:
        # Center around 1.0: trend=1.0 → 50, trend=1.5 → 100, trend=0.5 → 0
        trend_score = max(0, min((trend - 0.5) * 100, 100))
    else:
        trend_score = 50  # neutral if no data
    components['finra_trend'] = round(trend_score, 1)

    # 4. Insider MSPR (15%)
    mspr = data.get('mspr')
    if mspr is not None:
        # MSPR: -100 to 100. Map to 0-100 score.
        mspr_score = max(0, min((mspr + 100) / 2, 100))
    else:
        mspr_score = 50  # neutral if no data
    components['mspr'] = round(mspr_score, 1)

    # 5. Float size — inverse (15%)
    float_shares = data.get('float_shares', 0)
    float_m = float_shares / 1_000_000 if float_shares > 0 else 500
    if float_m <= 50:
        float_score = 100
    elif float_m >= 500:
        float_score = 0
    else:
        float_score = max(0, (500 - float_m) / 450 * 100)
    components['float_size'] = round(float_score, 1)

    # Weighted total
    total = (
        si_score * 0.30 +
        dtc_score * 0.20 +
        trend_score * 0.20 +
        mspr_score * 0.15 +
        float_score * 0.15
    )
    score = round(total, 1)

    # Grade
    if score >= 70:
        grade = 'A'
    elif score >= 50:
        grade = 'B'
    elif score >= 30:
        grade = 'C'
    else:
        grade = None  # skip

    return {
        'score': score,
        'components': components,
        'grade': grade,
    }


def generate_notes(data: Dict) -> str:
    """Auto-generate note flags for a candidate."""
    flags = []
    si_pct = data.get('si_pct', 0)
    si_change = data.get('si_change_mom', 0)
    mspr = data.get('mspr')
    trend = data.get('finra_trend')
    dtc = data.get('days_to_cover', 0)

    if si_pct > 0.20:
        flags.append(f"HIGH SI ({si_pct*100:.1f}%)")
    if si_change > 0.10:
        flags.append(f"SI rising MoM (+{si_change*100:.0f}%)")
    elif si_change < -0.10:
        flags.append(f"SI falling MoM ({si_change*100:.0f}%)")
    if mspr is not None and mspr > 50:
        flags.append("Insiders buying")
    elif mspr is not None and mspr < -50:
        flags.append("Insiders selling")
    if trend is not None and trend > 1.2:
        flags.append(f"Shorts piling in (trend {trend:.2f})")
    elif trend is not None and trend < 0.8:
        flags.append(f"Shorts covering (trend {trend:.2f})")
    if dtc > 8:
        flags.append(f"High DTC ({dtc:.1f} days)")

    return "; ".join(flags) if flags else ""


# ============================================================================
# SINGLE SYMBOL ANALYSIS
# ============================================================================

def analyze_short_candidate(symbol: str, finra_history: List[Dict],
                            finnhub_throttle: bool = True) -> Optional[Dict]:
    """
    Fetch all data sources and score a single symbol.
    Returns scored candidate dict or None.
    """
    # yfinance data (includes volume/price/float filters)
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        avg_volume = info.get('averageVolume', 0) or 0
        if avg_volume < 500_000:
            return None
        # Filter out micro-caps: lottery tickets, not squeeze trades
        price = info.get('currentPrice') or info.get('regularMarketPrice', 0) or 0
        if price < 2.0:
            return None
        float_shares = info.get('floatShares', 0) or 0
        if 0 < float_shares < 5_000_000:
            return None
    except Exception:
        return None

    yf_data = fetch_yfinance_short_data(symbol)
    if yf_data is None:
        return None

    # Skip if no meaningful short interest
    if yf_data['si_pct'] < 0.01:  # < 1%
        return None

    # FINRA trend
    finra_trend = compute_finra_trend(symbol, finra_history)

    # Finnhub MSPR (throttled)
    mspr = None
    if FINNHUB_API_KEY and finnhub_throttle:
        time.sleep(1.2)  # respect 60/min limit (~50/min to be safe)
        mspr = fetch_finnhub_mspr(symbol)

    # Combine data
    candidate = {
        'symbol': symbol,
        'price': yf_data['price'],
        'si_pct': yf_data['si_pct'],
        'days_to_cover': yf_data['days_to_cover'],
        'shares_short': yf_data['shares_short'],
        'float_shares': yf_data['float_shares'],
        'si_change_mom': yf_data['si_change_mom'],
        'finra_trend': finra_trend,
        'mspr': mspr,
    }

    # Score
    scoring = score_candidate(candidate)
    candidate['score'] = scoring['score']
    candidate['grade'] = scoring['grade']
    candidate['components'] = scoring['components']
    candidate['notes'] = generate_notes(candidate)

    return candidate


# ============================================================================
# SCANNER
# ============================================================================

def scan_short_squeeze_candidates(
    symbols: List[str],
    finra_history: List[Dict],
    max_workers: int = 8,
    use_finnhub: bool = True,
) -> Tuple[List[Dict], Dict]:
    """
    Scan all symbols for short squeeze candidates.
    Returns (candidates sorted by score, scan_stats).

    Finnhub calls are sequential (rate limit) so we do yfinance in parallel
    first, then Finnhub for top candidates only.
    """
    print(f"\n🔍 Scanning {len(symbols)} symbols for short squeeze candidates...\n")

    # Phase 1: yfinance + FINRA (parallelizable)
    raw_candidates = []
    analyzed_ok = 0
    total = len(symbols)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for sym in symbols:
            future = executor.submit(
                analyze_short_candidate, sym, finra_history, False  # no Finnhub yet
            )
            futures[future] = sym

        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 or completed == total:
                print(f"   Progress: {completed}/{total} ({100*completed//total}%)")

            try:
                result = future.result()
                if result is not None:
                    analyzed_ok += 1
                    raw_candidates.append(result)
            except Exception:
                continue

    print(f"\n   Phase 1 complete: {analyzed_ok} candidates with SI > 1%")

    # Phase 2: Finnhub MSPR for top 100 candidates only (sequential, rate limited)
    raw_candidates.sort(key=lambda x: x['score'], reverse=True)
    top_for_finnhub = raw_candidates[:100]

    if use_finnhub and FINNHUB_API_KEY and top_for_finnhub:
        print(f"\n📊 Fetching Finnhub MSPR for top {len(top_for_finnhub)} candidates...")
        for i, candidate in enumerate(top_for_finnhub):
            if (i + 1) % 10 == 0:
                print(f"   Finnhub progress: {i+1}/{len(top_for_finnhub)}")
            time.sleep(1.2)
            mspr = fetch_finnhub_mspr(candidate['symbol'])
            candidate['mspr'] = mspr

            # Re-score with MSPR data
            scoring = score_candidate(candidate)
            candidate['score'] = scoring['score']
            candidate['grade'] = scoring['grade']
            candidate['components'] = scoring['components']
            candidate['notes'] = generate_notes(candidate)
    elif not FINNHUB_API_KEY:
        print("   ⚠️  FINNHUB_API_KEY not set — MSPR data unavailable")

    # Final sort and filter
    raw_candidates.sort(key=lambda x: x['score'], reverse=True)
    graded = [c for c in raw_candidates if c.get('grade') is not None]

    scan_stats = {
        'total': total,
        'scanned': analyzed_ok,
        'a_grade': sum(1 for c in graded if c['grade'] == 'A'),
        'b_grade': sum(1 for c in graded if c['grade'] == 'B'),
        'c_grade': sum(1 for c in graded if c['grade'] == 'C'),
    }

    return graded, scan_stats


# ============================================================================
# AIRTABLE PUSH
# ============================================================================

def fetch_watchlist_records() -> Dict[str, dict]:
    """Return {TICKER: {id, fields}} from Airtable Short_Squeeze_Watchlist table."""
    if not AT_API:
        return {}

    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    params = {"pageSize": 100}
    records_map = {}

    try:
        while True:
            resp = session.get(url, headers=AT_HEADERS, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"❌ Airtable fetch failed: {resp.status_code}")
                break
            data = resp.json()
            for rec in data.get("records", []):
                ticker = rec.get("fields", {}).get("Symbol", "").upper()
                if ticker:
                    records_map[ticker] = {"id": rec["id"], "fields": rec.get("fields", {})}
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset
    except Exception as e:
        print(f"❌ Airtable fetch failed: {e}")

    return records_map


def _at_batch(batch: List[Dict], method: str) -> int:
    """Process an Airtable batch request. Returns count of records confirmed created/updated."""
    if not batch:
        return 0
    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    payload = {"records": batch}
    try:
        if method == "PATCH":
            resp = session.patch(url, headers=AT_HEADERS, json=payload, timeout=30)
        else:
            resp = session.post(url, headers=AT_HEADERS, json=payload, timeout=30)
        if resp.status_code not in [200, 201]:
            print(f"   ⚠️  Airtable {method} failed: {resp.status_code} - {resp.text[:300]}")
            return 0
        # Confirm actual records in response
        result = resp.json()
        confirmed = len(result.get('records', []))
        if confirmed != len(batch):
            print(f"   ⚠️  Airtable {method}: sent {len(batch)} but only {confirmed} confirmed")
        return confirmed
    except Exception as e:
        print(f"   ⚠️  Airtable {method} error: {e}")
        return 0


def push_to_airtable(candidates: List[Dict], top_n: int = 100):
    """Push top N candidates to Airtable Short_Squeeze_Watchlist."""
    if not AT_API:
        print("⚠️  AT_API not set — Airtable sync skipped")
        return

    print(f"\n📤 Pushing top {min(top_n, len(candidates))} to Airtable...")

    existing = fetch_watchlist_records()
    print(f"   Found {len(existing)} existing records")
    print(f"   Base: {AT_BASE}, Table: {AT_TABLE}")
    print(f"   API key present: {'yes' if AT_API else 'NO'}")

    top = candidates[:top_n]
    update_batch = []
    create_batch = []
    update_count = 0
    create_count = 0

    for c in top:
        sym = c['symbol'].upper()
        fields = {
            "Symbol": sym,
            "Price": sanitize_number(c.get('price', 0)),
            "Score": sanitize_number(c.get('score', 0)),
            "Grade": c.get('grade', 'C'),
            "SI % Float": sanitize_number(c.get('si_pct', 0)),
            "Days to Cover": sanitize_number(c.get('days_to_cover', 0)),
            "Short Volume Trend": sanitize_number(c.get('finra_trend', 0)),
            "MSPR": sanitize_number(c.get('mspr', 0)),
            "Float Shares": sanitize_number(c.get('float_shares', 0)),
            "SI Change MoM": sanitize_number(c.get('si_change_mom', 0)),
            "Last Updated": date.today().isoformat(),
            "Notes": c.get('notes', ''),
        }

        if sym in existing:
            update_batch.append({"id": existing[sym]["id"], "fields": fields})
        else:
            create_batch.append({"fields": fields})

        if len(update_batch) >= AIRTABLE_BATCH_SIZE:
            update_count += _at_batch(update_batch, "PATCH")
            update_batch = []
        if len(create_batch) >= AIRTABLE_BATCH_SIZE:
            create_count += _at_batch(create_batch, "POST")
            create_batch = []

    if update_batch:
        update_count += _at_batch(update_batch, "PATCH")
    if create_batch:
        create_count += _at_batch(create_batch, "POST")

    # Delete records no longer in top N
    current_tickers = {c['symbol'].upper() for c in top}
    stale = [rec for ticker, rec in existing.items() if ticker not in current_tickers]
    delete_count = 0
    for i in range(0, len(stale), AIRTABLE_BATCH_SIZE):
        batch_ids = [rec["id"] for rec in stale[i:i + AIRTABLE_BATCH_SIZE]]
        try:
            url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
            params = [("records[]", rid) for rid in batch_ids]
            resp = session.delete(url, headers=AT_HEADERS, params=params, timeout=30)
            if resp.status_code == 200:
                delete_count += len(batch_ids)
        except Exception:
            pass

    print(f"✅ Airtable sync: {update_count} updates, {create_count} creates, {delete_count} stale deleted")


# ============================================================================
# OUTPUT
# ============================================================================

def print_results(candidates: List[Dict], scan_stats: Dict, top_n: int = 30):
    """Print formatted results to console."""
    print(f"\n{'='*80}")
    print(f"  SHORT SQUEEZE WATCHLIST — Phase 1")
    print(f"  {date.today().strftime('%B %d, %Y')}")
    print(f"{'='*80}")
    print(f"  Scanned {scan_stats['scanned']} of {scan_stats['total']} symbols")
    print(f"  A-grade: {scan_stats['a_grade']}  B-grade: {scan_stats['b_grade']}  C-grade: {scan_stats['c_grade']}")
    print(f"{'='*80}\n")

    if not candidates:
        print("  No candidates found above C-grade threshold.\n")
        return

    display = candidates[:top_n]
    print(f"  {'Rank':<5} {'Symbol':<8} {'Score':>6} {'Grd':>4} {'SI%':>7} {'DTC':>6} {'Trend':>7} {'MSPR':>7} {'Float(M)':>10} {'Price':>8}  Notes")
    print(f"  {'-'*5} {'-'*8} {'-'*6} {'-'*4} {'-'*7} {'-'*6} {'-'*7} {'-'*7} {'-'*10} {'-'*8}  {'-'*30}")

    for i, c in enumerate(display, 1):
        si = c.get('si_pct', 0) * 100
        dtc = c.get('days_to_cover', 0)
        trend = c.get('finra_trend')
        trend_str = f"{trend:.2f}" if trend else "N/A"
        mspr = c.get('mspr')
        mspr_str = f"{mspr:.1f}" if mspr is not None else "N/A"
        float_m = c.get('float_shares', 0) / 1_000_000
        notes = c.get('notes', '')[:40]

        print(f"  {i:<5} {c['symbol']:<8} {c['score']:>6.1f} {c['grade']:>4} {si:>6.1f}% {dtc:>6.1f} {trend_str:>7} {mspr_str:>7} {float_m:>9.1f}M ${c.get('price',0):>7.2f}  {notes}")

    if len(candidates) > top_n:
        print(f"\n  ... +{len(candidates) - top_n} more below top {top_n}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Short Squeeze Watchlist Scanner — Phase 1')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of symbols to scan (0 = all)')
    parser.add_argument('--symbols', nargs='+', type=str,
                        help='Specific symbols to scan')
    parser.add_argument('--no-airtable', action='store_true',
                        help='Skip Airtable push')
    parser.add_argument('--no-finnhub', action='store_true',
                        help='Skip Finnhub MSPR fetch')
    parser.add_argument('--no-finra-history', action='store_true',
                        help='Skip 20-day FINRA history (use single day only)')
    parser.add_argument('--workers', type=int, default=8,
                        help='ThreadPool workers for yfinance')

    args = parser.parse_args()

    print("=" * 60)
    print("Short Squeeze Watchlist Scanner — Phase 1")
    print("=" * 60)
    print(f"Date: {date.today().strftime('%A, %B %d, %Y')}")
    print()

    # Load symbols
    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
        print(f"📊 Custom tickers: {len(symbols)} symbols")
    else:
        symbols = get_swing_universe()
        if args.limit > 0:
            symbols = symbols[:args.limit]
        print(f"📊 Universe: {len(symbols)} symbols")

    # Fetch FINRA history
    if args.no_finra_history:
        print("\n📁 Fetching FINRA short volume (single day)...")
        single = fetch_finra_short_volume()
        finra_history = [single] if single else []
    else:
        print("\n📁 Fetching FINRA short volume history (20 trading days)...")
        finra_history = fetch_finra_history(20)

    # Run scanner
    candidates, scan_stats = scan_short_squeeze_candidates(
        symbols, finra_history,
        max_workers=args.workers,
        use_finnhub=not args.no_finnhub,
    )

    # Print results
    print_results(candidates, scan_stats)

    # Push to Airtable
    if not args.no_airtable:
        push_to_airtable(candidates, top_n=100)
    else:
        print("\n📡 Airtable sync skipped (--no-airtable)")

    print(f"\n✅ Done.")


if __name__ == '__main__':
    main()
