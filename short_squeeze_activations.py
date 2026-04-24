#!/usr/bin/env python3
"""
Short Squeeze Activation Tracker — Phase 2
============================================
DEPRECATED as standalone script. Now invoked as a module via short_squeeze_daily.py.
The compute_activation_signals() function is the primary export.

Previously ran at 13:00 UTC pre-market; now runs post-close via short_squeeze_daily.py.

Airtable Schema Additions (add these to Short_Squeeze_Watchlist before first run):
  - Status            (single select: WATCHING, ACTIVATING, ACTIVATED, PARABOLIC, COOLED, EARNINGS_HOLD)
  - Activation Score  (number, integer)
  - Activation Signals (long text)
  - Activation Date   (date, include time)
  - Breakout Level    (currency, USD)
  - Pre-market %      (percent, 2 decimals)

Usage:
  python short_squeeze_activations.py                  # Full production run
  python short_squeeze_activations.py --dry-run        # No writes, no emails
  python short_squeeze_activations.py --limit 10       # Test with top 10 only
"""

import argparse
import os
import smtplib
import time
import urllib.parse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import warnings
warnings.filterwarnings('ignore')

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from short_squeeze_watchlist import sanitize_number


# ============================================================================
# CONFIG
# ============================================================================

AT_BASE = "appIUFp3KFrf8KXez"
AT_API = os.getenv("AT_API", "")
AT_TABLE = "Short_Squeeze_Watchlist"
AIRTABLE_BATCH_SIZE = 10

session = requests.Session()
retry_strategy = Retry(
    total=3, backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PATCH"],
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

AT_HEADERS = {
    "Authorization": f"Bearer {AT_API}",
    "Content-Type": "application/json",
}

# 2026 NYSE holidays
MARKET_HOLIDAYS_2026 = {
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 19),   # MLK Day
    date(2026, 2, 16),   # Presidents' Day
    date(2026, 4, 3),    # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 6, 19),   # Juneteenth
    date(2026, 7, 3),    # Independence Day (observed)
    date(2026, 9, 7),    # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
}


def is_market_holiday(d: date = None) -> bool:
    d = d or date.today()
    if d.weekday() >= 5:
        return True
    return d in MARKET_HOLIDAYS_2026


# ============================================================================
# AIRTABLE READ
# ============================================================================

def fetch_top_watchlist(limit: int = 50) -> List[Dict]:
    """Read top N candidates from Airtable sorted by Score descending."""
    if not AT_API:
        print("   AT_API not set")
        return []

    url = f"https://api.airtable.com/v0/{AT_BASE}/{urllib.parse.quote(AT_TABLE)}"
    params = {
        "pageSize": 100,
        "sort[0][field]": "Score",
        "sort[0][direction]": "desc",
    }
    all_records = []

    try:
        while True:
            resp = session.get(url, headers=AT_HEADERS, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"   Airtable read failed: {resp.status_code}")
                break
            data = resp.json()
            all_records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset
    except Exception as e:
        print(f"   Airtable read error: {e}")

    # Return top N with id + fields
    return all_records[:limit]


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_daily_bars(symbol: str, period: str = '60d') -> Optional[pd.DataFrame]:
    """Fetch daily OHLCV bars from yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval='1d')
        if df is None or df.empty or len(df) < 10:
            return None
        return df
    except Exception:
        return None


def fetch_premarket_data(symbol: str) -> Dict:
    """Fetch pre-market data. Returns {pre_market_price, gap_pct, prev_close}."""
    result = {'pre_market_price': None, 'gap_pct': None, 'prev_close': None}
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        prev_close = info.get('previousClose') or info.get('regularMarketPreviousClose', 0)
        pre_price = info.get('preMarketPrice')

        result['prev_close'] = float(prev_close) if prev_close else None

        if pre_price and prev_close and prev_close > 0 and pre_price != prev_close:
            result['pre_market_price'] = float(pre_price)
            result['gap_pct'] = (float(pre_price) - float(prev_close)) / float(prev_close)
        else:
            # Pre-market not available or equals prev close
            pass
    except Exception:
        pass
    return result


# ============================================================================
# ACTIVATION SIGNAL COMPUTATION
# ============================================================================

def compute_activation_signals(symbol: str, df: pd.DataFrame,
                                premarket: Dict,
                                finra_trend: Optional[float] = None,
                                prior_breakout_level: Optional[float] = None,
                                prior_breakout_days: int = 0) -> Dict:
    """
    Compute activation score (0-13) and status from daily bars + pre-market.

    Returns {score, signals, status, breakout_level, breakout_days_held, gap_pct}.
    """
    signals = []
    score = 0
    close = df['Close']
    high = df['High']
    low = df['Low']
    volume = df['Volume']
    current_price = float(close.iloc[-1])
    today_high = float(high.iloc[-1])
    today_low = float(low.iloc[-1])

    # Close position in today's range (0.0 = at low, 1.0 = at high)
    day_range = today_high - today_low
    close_position = (current_price - today_low) / day_range if day_range > 0 else 0.5

    # ---- Price breakout (validated: close above + range position) ----
    breakout_level = None
    breakout_days_held = 0

    # Compute prior N-day highs (excluding today)
    tiers = []
    if len(df) >= 21:
        tiers.append(('20-day', float(high.iloc[-21:-1].max()), 1))
    if len(df) >= 51:
        tiers.append(('50-day', float(high.iloc[-51:-1].max()), 2))
    if len(df) >= 200:
        tiers.append(('52-week', float(high.iloc[:-1].max()), 3))

    # Sort by tier points descending — check highest first
    tiers.sort(key=lambda x: x[2], reverse=True)

    breakout_pts = 0
    for tier_name, tier_level, tier_pts in tiers:
        # a) VALID FRESH BREAKOUT: close above + good range position
        if current_price > tier_level and close_position >= 0.5:
            breakout_pts = tier_pts
            breakout_level = tier_level
            breakout_days_held = 1
            signals.append(f"Broke {tier_name} high (${tier_level:.2f}), closed near high")
            break
        # c) FAILED BREAKOUT: high touched but close below
        elif today_high > tier_level and current_price <= tier_level:
            signals.append(f"Failed breakout: tested {tier_name} ${tier_level:.2f}, closed ${current_price:.2f}")
            break

    # b) VALID CONTINUATION: holding above prior breakout level
    if breakout_pts == 0 and prior_breakout_level and prior_breakout_days > 0:
        if current_price > prior_breakout_level and prior_breakout_days < 5:
            # Still holding — award same points as original breakout tier
            for tier_name, tier_level, tier_pts in tiers:
                if abs(tier_level - prior_breakout_level) < 0.01:
                    breakout_pts = tier_pts
                    break
            if breakout_pts == 0:
                breakout_pts = 1  # default to lowest tier if we can't match
            breakout_level = prior_breakout_level
            breakout_days_held = prior_breakout_days + 1
            signals.append(f"Holding breakout ${prior_breakout_level:.2f} (day {breakout_days_held} of 5)")
        elif current_price <= prior_breakout_level:
            # d) BROKE DOWN from prior breakout
            signals.append(f"Lost breakout ${prior_breakout_level:.2f}")
            breakout_level = None
            breakout_days_held = 0

    score += breakout_pts

    # EMA alignment (additive)
    ema_8 = close.ewm(span=8, adjust=False).mean()
    ema_21 = close.ewm(span=21, adjust=False).mean()
    if current_price > float(ema_8.iloc[-1]) and current_price > float(ema_21.iloc[-1]):
        score += 1
        signals.append("Above 8-EMA and 21-EMA")

    # ---- Volume (take max within group) ----
    avg_vol_20 = float(volume.tail(20).mean())
    latest_vol = float(volume.iloc[-1])
    rel_vol = latest_vol / avg_vol_20 if avg_vol_20 > 0 else 0

    vol_pts = 0
    if rel_vol > 2.5:
        vol_pts = 2
        signals.append(f"RelVol {rel_vol*100:.0f}% (>250%)")
    elif rel_vol > 1.5:
        vol_pts = 1
        signals.append(f"RelVol {rel_vol*100:.0f}% (>150%)")
    score += vol_pts

    # Consecutive above-average volume (additive)
    consec_high_vol = 0
    for i in range(1, min(6, len(volume))):
        if float(volume.iloc[-i]) > avg_vol_20:
            consec_high_vol += 1
        else:
            break
    if consec_high_vol >= 3:
        score += 1
        signals.append(f"{consec_high_vol} consecutive days above-avg volume")

    # ---- Momentum (take max within group) ----
    # Consecutive green closes
    consec_green = 0
    for i in range(1, min(8, len(close))):
        if float(close.iloc[-i]) > float(close.iloc[-i - 1]) if len(close) > i else False:
            consec_green += 1
        else:
            break

    mom_pts = 0
    if consec_green >= 5:
        mom_pts = 2
        signals.append(f"{consec_green} consecutive green closes")
    elif consec_green >= 3:
        mom_pts = 1
        signals.append(f"{consec_green} consecutive green closes")
    score += mom_pts

    # Momentum acceleration (additive)
    if len(close) >= 5:
        mom_3d = float(close.iloc[-1]) - float(close.iloc[-4])
        mom_prev_3d = float(close.iloc[-2]) - float(close.iloc[-5])
        if mom_3d > mom_prev_3d and mom_3d > 0:
            score += 1
            signals.append("Momentum accelerating")

    # 20-day price change (additive)
    if len(close) >= 20:
        pct_20d = (float(close.iloc[-1]) - float(close.iloc[-20])) / float(close.iloc[-20])
        if pct_20d > 0.10:
            score += 1
            signals.append(f"20-day change +{pct_20d*100:.1f}%")

    # ---- FINRA short volume trend ----
    if finra_trend is not None and finra_trend > 1.0:
        # Check if we can detect 3+ consecutive rising days from trend value
        # Simplified: trend > 1.0 means 5d avg > 20d avg = shorts intensifying
        if finra_trend > 1.1:
            score += 2
            signals.append(f"FINRA short volume rising (trend {finra_trend:.2f})")

    # ---- Pre-market ----
    gap_pct = premarket.get('gap_pct')
    if gap_pct is not None:
        if gap_pct > 0.08:
            score += 2
            signals.append(f"Pre-market gap +{gap_pct*100:.1f}%")
        elif gap_pct > 0.03:
            score += 1
            signals.append(f"Pre-market gap +{gap_pct*100:.1f}%")

    # ---- Status bands ----
    if score >= 11:
        status = 'PARABOLIC'
    elif score >= 7:
        status = 'ACTIVATED'
    elif score >= 4:
        status = 'ACTIVATING'
    else:
        status = 'WATCHING'

    return {
        'score': score,
        'signals': signals,
        'status': status,
        'breakout_level': breakout_level,
        'breakout_days_held': breakout_days_held,
        'gap_pct': gap_pct,
    }


def apply_earnings_hold(result: Dict, days_to_earnings) -> Dict:
    """Override status to EARNINGS_HOLD if earnings within 5 days."""
    if days_to_earnings is not None and days_to_earnings <= 5:
        result['original_status'] = result['status']
        result['status'] = 'EARNINGS_HOLD'
        result['signals'].insert(0, f"Earnings in {days_to_earnings}d — positioning locked")
    return result


def detect_status_transitions(current_results: Dict[str, Dict],
                               previous_states: Dict[str, str]) -> Dict:
    """Compare today's status vs yesterday's stored status."""
    transitions = {
        'newly_activated': [],
        'newly_parabolic': [],
        'newly_cooled': [],
        'newly_earnings_hold': [],
        'still_activated': [],
    }

    for sym, result in current_results.items():
        new_status = result['status']
        old_status = previous_states.get(sym, 'WATCHING')

        if new_status == 'ACTIVATED' and old_status not in ('ACTIVATED', 'PARABOLIC'):
            transitions['newly_activated'].append(result)
        elif new_status == 'PARABOLIC' and old_status != 'PARABOLIC':
            transitions['newly_parabolic'].append(result)
        elif new_status == 'EARNINGS_HOLD' and old_status != 'EARNINGS_HOLD':
            transitions['newly_earnings_hold'].append(result)
        elif new_status in ('WATCHING', 'ACTIVATING') and old_status in ('ACTIVATED', 'PARABOLIC'):
            result['status'] = 'COOLED'
            transitions['newly_cooled'].append(result)
        elif new_status == 'ACTIVATED' and old_status == 'ACTIVATED':
            transitions['still_activated'].append(result)

    return transitions


# ============================================================================
# AIRTABLE PUSH
# ============================================================================

def _at_batch(batch: List[Dict], method: str) -> int:
    """Process Airtable batch. Returns confirmed record count."""
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
            print(f"   Airtable {method} failed: {resp.status_code} - {resp.text[:300]}")
            return 0
        result = resp.json()
        confirmed = len(result.get('records', []))
        if confirmed != len(batch):
            print(f"   Airtable {method}: sent {len(batch)} but only {confirmed} confirmed")
        return confirmed
    except Exception as e:
        print(f"   Airtable {method} error: {e}")
        return 0


def push_activations_to_airtable(results: Dict[str, Dict],
                                  at_records: List[Dict]) -> int:
    """Update activation fields on existing Airtable records."""
    if not AT_API:
        print("   AT_API not set — skipping Airtable push")
        return 0

    # Build record_id lookup from fetched records
    id_map = {}
    for rec in at_records:
        sym = rec.get('fields', {}).get('Symbol', '').upper()
        if sym:
            id_map[sym] = rec['id']

    update_batch = []
    update_count = 0

    for sym, result in results.items():
        rec_id = id_map.get(sym.upper())
        if not rec_id:
            continue

        fields = {
            "Status": result['status'],
            "Activation Score": result['score'],
            "Activation Signals": "; ".join(result['signals']),
            "Last Updated": date.today().isoformat(),
        }

        if result.get('breakout_level') is not None:
            fields["Breakout Level"] = sanitize_number(result['breakout_level'])

        fields["Breakout Days Held"] = result.get('breakout_days_held', 0)

        if result.get('gap_pct') is not None:
            fields["Pre-market %"] = sanitize_number(result['gap_pct'])

        if result['status'] in ('ACTIVATED', 'PARABOLIC'):
            fields["Activation Date"] = datetime.utcnow().isoformat() + "Z"

        update_batch.append({"id": rec_id, "fields": fields})

        if len(update_batch) >= AIRTABLE_BATCH_SIZE:
            update_count += _at_batch(update_batch, "PATCH")
            update_batch = []

    if update_batch:
        update_count += _at_batch(update_batch, "PATCH")

    return update_count


# ============================================================================
# EMAIL
# ============================================================================

def _send_email(subject: str, html_body: str, dry_run: bool = False) -> bool:
    gmail_user = os.environ.get('GMAIL_USER')
    gmail_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient = os.environ.get('EMAIL_TO') or os.environ.get('EMAIL_RECIPIENT') or gmail_user

    if not gmail_user or not gmail_password:
        if dry_run:
            print(f"\n   DRY RUN email — Subject: {subject}")
            return True
        print("   Gmail credentials not set — email skipped")
        return False

    if dry_run:
        print(f"\n   DRY RUN email — Subject: {subject}")
        return True

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = gmail_user
    msg['To'] = recipient
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipient, msg.as_string())
        print(f"   Email sent: {subject}")
        return True
    except Exception as e:
        print(f"   Email failed: {e}")
        return False


def _activation_row_html(r: Dict, alert: bool = False) -> str:
    """Build a single row for activation tables."""
    sym = r['symbol']
    price = r.get('price', 0)
    score = r.get('score', 0)
    status = r.get('status', '?')
    gap = r.get('gap_pct')
    gap_str = f"{gap*100:+.1f}%" if gap is not None else "N/A"
    signals_str = "; ".join(r.get('signals', [])[:3])
    dte = r.get('days_to_earnings')
    dte_str = f"{dte}d" if dte is not None else ""
    chart_url = f"https://www.tradingview.com/chart/?symbol={sym}"

    status_colors = {
        'ACTIVATED': '#22c55e',
        'PARABOLIC': '#ef4444',
        'ACTIVATING': '#f59e0b',
        'EARNINGS_HOLD': '#7c3aed',
        'COOLED': '#3b82f6',
        'WATCHING': '#6b7280',
    }
    color = status_colors.get(status, '#6b7280')

    return f"""
    <tr>
        <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; font-weight: bold;">
            <a href="{chart_url}" style="color: #2563eb; text-decoration: none;">{sym}</a></td>
        <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: right;">${price:.2f}</td>
        <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: center;">{score}</td>
        <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: center;
            color: {color}; font-weight: bold;">{status}</td>
        <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: right;">{gap_str}</td>
        <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; font-size: 12px;">{signals_str}</td>
    </tr>"""


def _wrap_table(rows_html: str, header_bg: str = '#374151') -> str:
    return f"""
    <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px;
                  overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 15px;">
        <thead><tr style="background: {header_bg}; color: white;">
            <th style="padding: 10px; text-align: left;">Symbol</th>
            <th style="padding: 10px; text-align: right;">Price</th>
            <th style="padding: 10px; text-align: center;">Score</th>
            <th style="padding: 10px; text-align: center;">Status</th>
            <th style="padding: 10px; text-align: right;">Gap%</th>
            <th style="padding: 10px; text-align: left;">Signals</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
    </table>"""


def format_immediate_email(newly_activated: List, newly_parabolic: List) -> Tuple[str, str]:
    today = date.today().strftime("%b %d")
    count = len(newly_activated) + len(newly_parabolic)
    subject = f"Short Squeeze Activation — {count} new ({today})"

    sections = ""

    if newly_parabolic:
        rows = "".join(_activation_row_html(r, alert=True) for r in newly_parabolic)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #ef4444; margin: 0 0 8px 0;">PARABOLIC ({len(newly_parabolic)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Extreme activation. Exhaustion risk elevated.</p>
            {_wrap_table(rows, '#991b1b')}
        </div>"""

    if newly_activated:
        rows = "".join(_activation_row_html(r, alert=True) for r in newly_activated)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #22c55e; margin: 0 0 8px 0;">NEWLY ACTIVATED ({len(newly_activated)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Squeeze activation detected. Review chart before entry.</p>
            {_wrap_table(rows, '#166534')}
        </div>"""

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
        <div style="background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%); color: white;
                    padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">Short Squeeze Activation Alert</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">{today} Pre-Market</p>
        </div>
        <div style="background: #fef2f2; padding: 20px; border: 1px solid #fecaca; border-top: none;">
            {sections}
        </div>
        <div style="background: #f3f4f6; padding: 10px; border-radius: 0 0 8px 8px;
                    border: 1px solid #e5e5e5; border-top: none;">
            <p style="margin: 0; font-size: 11px; color: #6b7280;">
                Short Squeeze Tracker Phase 2 — Pre-market activation scan</p>
        </div>
    </body></html>"""

    return subject, html


def format_digest_email(all_results: Dict[str, Dict],
                        transitions: Dict) -> Tuple[str, str]:
    today = date.today().strftime("%b %d")
    activated_count = (len(transitions['newly_activated']) +
                       len(transitions['still_activated']) +
                       len(transitions['newly_parabolic']))
    subject = f"Short Squeeze Status — {activated_count} active ({today})"

    sections = ""

    # Newly activated
    items = transitions['newly_activated']
    if items:
        rows = "".join(_activation_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #22c55e; margin: 0 0 8px 0;">Newly Activated ({len(items)})</h3>
            {_wrap_table(rows, '#166534')}
        </div>"""

    # Parabolic
    items = transitions['newly_parabolic']
    if items:
        rows = "".join(_activation_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #ef4444; margin: 0 0 8px 0;">Parabolic ({len(items)})</h3>
            {_wrap_table(rows, '#991b1b')}
        </div>"""

    # Still activated
    items = transitions['still_activated']
    if items:
        rows = "".join(_activation_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #16a34a; margin: 0 0 8px 0;">Still Activated ({len(items)})</h3>
            {_wrap_table(rows)}
        </div>"""

    # Earnings hold
    items = transitions['newly_earnings_hold']
    if items:
        rows = "".join(_activation_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #7c3aed; margin: 0 0 8px 0;">Earnings Hold ({len(items)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Earnings imminent — volatility play, not directional squeeze.</p>
            {_wrap_table(rows, '#5b21b6')}
        </div>"""

    # Cooled
    items = transitions['newly_cooled']
    if items:
        rows = "".join(_activation_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #3b82f6; margin: 0 0 8px 0;">Cooled ({len(items)})</h3>
            {_wrap_table(rows, '#1e40af')}
        </div>"""

    # Top 10 watchlist (activating or watching, sorted by score)
    watching = sorted(
        [r for r in all_results.values() if r['status'] in ('WATCHING', 'ACTIVATING')],
        key=lambda x: x['score'], reverse=True
    )[:10]
    if watching:
        rows = "".join(_activation_row_html(r) for r in watching)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #6b7280; margin: 0 0 8px 0;">Top 10 Watchlist</h3>
            {_wrap_table(rows)}
        </div>"""

    if not sections:
        sections = '<p style="color: #9ca3af;">No significant activity today.</p>'

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
        <div style="background: #1e293b; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">Short Squeeze Daily Status</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">{today}</p>
        </div>
        <div style="background: #f9fafb; padding: 20px; border: 1px solid #e5e5e5; border-top: none;">
            {sections}
        </div>
        <div style="background: #f3f4f6; padding: 10px; border-radius: 0 0 8px 8px;
                    border: 1px solid #e5e5e5; border-top: none;">
            <p style="margin: 0; font-size: 11px; color: #6b7280;">
                Short Squeeze Tracker Phase 2 — Daily digest</p>
        </div>
    </body></html>"""

    return subject, html


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Short Squeeze Activation Tracker — Phase 2')
    parser.add_argument('--dry-run', action='store_true', help='No writes, no emails')
    parser.add_argument('--limit', type=int, default=50, help='Number of candidates to check')
    args = parser.parse_args()

    print("=" * 60)
    print("Short Squeeze Activation Tracker — Phase 2")
    print("=" * 60)
    print(f"Date: {date.today().strftime('%A, %B %d, %Y')}")
    if args.dry_run:
        print("MODE: DRY RUN (no Airtable writes, no emails)")
    print()

    # Holiday check
    if is_market_holiday():
        print("Market holiday — skipping.")
        return

    # Load candidates from Airtable
    print("Loading candidates from Airtable...")
    at_records = fetch_top_watchlist(limit=args.limit)
    if not at_records:
        print("No candidates found in Airtable. Run Phase 1 first.")
        return
    print(f"   Loaded {len(at_records)} candidates")

    # Extract previous status for transition detection
    previous_states = {}
    for rec in at_records:
        sym = rec.get('fields', {}).get('Symbol', '').upper()
        if sym:
            previous_states[sym] = rec.get('fields', {}).get('Status', 'WATCHING') or 'WATCHING'

    # Analyze each candidate
    print("\nAnalyzing activation signals...")
    all_results = {}
    analyzed = 0

    for rec in at_records:
        fields = rec.get('fields', {})
        sym = fields.get('Symbol', '').upper()
        if not sym:
            continue

        # Fetch daily bars
        df = fetch_daily_bars(sym)
        if df is None:
            continue

        # Fetch pre-market
        premarket = fetch_premarket_data(sym)
        if premarket['pre_market_price'] is None:
            pass  # graceful degradation — continue without pre-market

        # Get FINRA trend from Phase 1 data (stored in Airtable)
        finra_trend = fields.get('Short Volume Trend')

        # Get prior breakout state for continuation tracking
        prior_bo_level = fields.get('Breakout Level')
        prior_bo_days = int(fields.get('Breakout Days Held', 0) or 0)

        # Compute signals
        result = compute_activation_signals(
            sym, df, premarket, finra_trend,
            prior_breakout_level=prior_bo_level,
            prior_breakout_days=prior_bo_days,
        )
        result['symbol'] = sym
        result['price'] = float(fields.get('Price', 0))
        result['si_pct'] = fields.get('SI % Float', 0)
        result['grade'] = fields.get('Grade', '?')
        result['days_to_earnings'] = fields.get('Days to Earnings')
        result['sector'] = fields.get('Sector', '')

        # Apply earnings hold
        apply_earnings_hold(result, result.get('days_to_earnings'))

        all_results[sym] = result
        analyzed += 1

        if analyzed % 10 == 0:
            print(f"   Progress: {analyzed}/{len(at_records)}")

    print(f"   Analyzed {analyzed} symbols")

    # Detect transitions
    transitions = detect_status_transitions(all_results, previous_states)

    # Print summary
    print(f"\n{'='*60}")
    print(f"  ACTIVATION SUMMARY")
    print(f"{'='*60}")
    print(f"  Newly Activated:  {len(transitions['newly_activated'])}")
    print(f"  Parabolic:        {len(transitions['newly_parabolic'])}")
    print(f"  Still Activated:  {len(transitions['still_activated'])}")
    print(f"  Earnings Hold:    {len(transitions['newly_earnings_hold'])}")
    print(f"  Cooled:           {len(transitions['newly_cooled'])}")
    print(f"{'='*60}")

    # Print detail table
    print(f"\n  {'Symbol':<8} {'Prev':<14} {'New':<14} {'Score':>6} {'Gap%':>7} {'DTE':>5}  Signals")
    print(f"  {'-'*8} {'-'*14} {'-'*14} {'-'*6} {'-'*7} {'-'*5}  {'-'*40}")
    for sym in sorted(all_results.keys()):
        r = all_results[sym]
        prev = previous_states.get(sym, 'WATCHING')
        gap = r.get('gap_pct')
        gap_s = f"{gap*100:+.1f}%" if gap is not None else "N/A"
        dte = r.get('days_to_earnings')
        dte_s = f"{dte}d" if dte is not None else "?"
        sigs = "; ".join(r.get('signals', [])[:2])
        changed = " *" if prev != r['status'] else ""
        print(f"  {sym:<8} {prev:<14} {r['status']:<14} {r['score']:>6} {gap_s:>7} {dte_s:>5}  {sigs}{changed}")

    # Push to Airtable
    if not args.dry_run:
        print("\nPushing activations to Airtable...")
        count = push_activations_to_airtable(all_results, at_records)
        print(f"   Updated {count} records")
    else:
        print("\n   DRY RUN — Airtable writes skipped")

    # Emails
    newly = transitions['newly_activated'] + transitions['newly_parabolic']
    if newly and not args.dry_run:
        print("\nSending immediate activation email...")
        subj, html = format_immediate_email(
            transitions['newly_activated'], transitions['newly_parabolic'])
        _send_email(subj, html, dry_run=args.dry_run)
    elif newly:
        print(f"\n   DRY RUN — Would send immediate email for {len(newly)} activations")

    # Always send digest
    print("\nSending daily digest email...")
    subj, html = format_digest_email(all_results, transitions)
    _send_email(subj, html, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == '__main__':
    main()
