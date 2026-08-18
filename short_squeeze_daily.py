#!/usr/bin/env python3
"""
Short Squeeze Daily Scanner — Unified Phase 2 + Phase 3
========================================================
Post-close daily scanner that runs Phase 2 (activation detection)
and Phase 3 (exhaustion detection) in a single pass, sending one
unified email.

Runs daily at 21:00 UTC (post-close ET), Mon-Fri.

Airtable Schema Additions (add before first run):
  Phase 2 fields (already added):
    - Status, Activation Score, Activation Signals, Activation Date,
      Breakout Level, Pre-market %, Breakout Days Held
  Phase 3 fields (NEW — add these):
    - Exhaustion Score     (Number, integer, 0-18)
    - Exhaustion Signals   (Long text)
    - Exhaustion Status    (Single select: HEALTHY, WARNING, EXHAUSTING, EXHAUSTED)
    - Days Since Activation (Number, integer)

Usage:
  python short_squeeze_daily.py                  # Full production run
  python short_squeeze_daily.py --dry-run        # No writes, no emails
  python short_squeeze_daily.py --limit 10       # Test with top 10
"""

import argparse
import os
import smtplib
import urllib.parse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

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
from short_squeeze_activations import (
    compute_activation_signals,
    apply_earnings_hold,
    is_market_holiday,
    fetch_daily_bars,
)
from short_squeeze_exhaustion import compute_exhaustion_signals


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


# ============================================================================
# AIRTABLE I/O
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

    return all_records[:limit]


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
        return len(result.get('records', []))
    except Exception as e:
        print(f"   Airtable {method} error: {e}")
        return 0


def push_daily_results(results: Dict[str, Dict], at_records: List[Dict]) -> int:
    """Push Phase 2 + Phase 3 results to Airtable."""
    if not AT_API:
        print("   AT_API not set — skipping Airtable push")
        return 0

    id_map = {}
    for rec in at_records:
        sym = rec.get('fields', {}).get('Symbol', '').upper()
        if sym:
            id_map[sym] = rec['id']

    update_batch = []
    update_count = 0

    for sym, r in results.items():
        rec_id = id_map.get(sym.upper())
        if not rec_id:
            continue

        fields = {
            "Status": r.get('combined_status', r.get('activation_status', 'WATCHING')),
            "Activation Score": r.get('activation_score', 0),
            "Activation Signals": "; ".join(r.get('activation_signals', [])),
            "Last Updated": date.today().isoformat(),
            # Per-writer stamp -- see the note in short_squeeze_watchlist.py.
            # This moves only when Status/Activation/Exhaustion refreshes, which
            # is computed on LIVE bars (fetch_daily_bars), not the Price field.
            "Status Updated": date.today().isoformat(),
        }

        if r.get('breakout_level') is not None:
            fields["Breakout Level"] = sanitize_number(r['breakout_level'])
        fields["Breakout Days Held"] = r.get('breakout_days_held', 0)

        if r.get('combined_status') in ('ACTIVATED', 'PARABOLIC') and not r.get('prior_activation_date'):
            fields["Activation Date"] = date.today().isoformat()

        # Phase 3 fields
        fields["Exhaustion Score"] = r.get('exhaustion_score', 0)
        fields["Exhaustion Signals"] = "; ".join(r.get('exhaustion_signals', []))
        fields["Exhaustion Status"] = r.get('exhaustion_status', 'HEALTHY')
        if r.get('days_since_activation') is not None:
            fields["Days Since Activation"] = r['days_since_activation']

        update_batch.append({"id": rec_id, "fields": fields})

        if len(update_batch) >= AIRTABLE_BATCH_SIZE:
            update_count += _at_batch(update_batch, "PATCH")
            update_batch = []

    if update_batch:
        update_count += _at_batch(update_batch, "PATCH")

    return update_count


# ============================================================================
# COMBINED PIPELINE
# ============================================================================

def run_daily_pipeline(at_records: List[Dict]) -> Tuple[Dict[str, Dict], Dict]:
    """Run Phase 2 + Phase 3 for all candidates. Returns (results, transitions)."""
    results = {}
    previous_states = {}

    for rec in at_records:
        fields = rec.get('fields', {})
        sym = fields.get('Symbol', '').upper()
        if not sym:
            continue
        previous_states[sym] = fields.get('Status', 'WATCHING') or 'WATCHING'

    analyzed = 0
    total = len(at_records)

    for rec in at_records:
        fields = rec.get('fields', {})
        sym = fields.get('Symbol', '').upper()
        if not sym:
            continue

        # Fetch daily bars
        df = fetch_daily_bars(sym)
        if df is None:
            continue

        # Prior breakout state
        prior_bo_level = fields.get('Breakout Level')
        prior_bo_days = int(fields.get('Breakout Days Held', 0) or 0)
        finra_trend = fields.get('Short Volume Trend')
        prior_activation_date = fields.get('Activation Date')

        # Phase 2: Activation
        activation = compute_activation_signals(
            sym, df, {},  # empty premarket dict — post-close, no pre-market data
            finra_trend,
            prior_breakout_level=prior_bo_level,
            prior_breakout_days=prior_bo_days,
        )

        # Days since activation
        days_since_activation = None
        if prior_activation_date:
            try:
                act_d = date.fromisoformat(str(prior_activation_date)[:10])
                days_since_activation = (date.today() - act_d).days
            except (ValueError, TypeError):
                pass

        # Phase 3: Exhaustion (only for ACTIVATED/PARABOLIC)
        exhaustion = {'score': 0, 'signals': [], 'status': 'HEALTHY', 'reversal_strength': 0.0}
        act_status = activation['status']
        prior_status = previous_states.get(sym, 'WATCHING')

        if act_status in ('ACTIVATED', 'PARABOLIC') or prior_status in ('ACTIVATED', 'PARABOLIC'):
            exhaustion = compute_exhaustion_signals(
                sym, df,
                prior_status=prior_status,
                activation_date=str(prior_activation_date)[:10] if prior_activation_date else None,
            )

        # Combine statuses
        combined_status = act_status
        if act_status in ('ACTIVATED', 'PARABOLIC'):
            if exhaustion['status'] == 'EXHAUSTED':
                combined_status = 'EXHAUSTED'
            elif exhaustion['status'] == 'EXHAUSTING':
                combined_status = f"{act_status}_EXHAUSTING"

        # Build result
        r = {
            'symbol': sym,
            'price': float(fields.get('Price', 0)),
            'si_pct': fields.get('SI % Float', 0),
            'grade': fields.get('Grade', '?'),
            'days_to_earnings': fields.get('Days to Earnings'),
            'sector': fields.get('Sector', ''),
            'activation_score': activation['score'],
            'activation_signals': activation['signals'],
            'activation_status': activation['status'],
            'breakout_level': activation.get('breakout_level'),
            'breakout_days_held': activation.get('breakout_days_held', 0),
            'exhaustion_score': exhaustion['score'],
            'exhaustion_signals': exhaustion['signals'],
            'exhaustion_status': exhaustion['status'],
            'reversal_strength': exhaustion.get('reversal_strength', 0),
            'days_since_activation': days_since_activation,
            'combined_status': combined_status,
            'prior_activation_date': prior_activation_date,
        }

        # Apply earnings hold override
        dte = fields.get('Days to Earnings')
        if dte is not None and dte <= 5:
            r['combined_status'] = 'EARNINGS_HOLD'
            r['activation_signals'].insert(0, f"Earnings in {int(dte)}d — positioning locked")
            # Cap exhaustion at WARNING for earnings-hold stocks
            if exhaustion['status'] in ('EXHAUSTING', 'EXHAUSTED'):
                r['exhaustion_status'] = 'WARNING'
                r['exhaustion_signals'].append("Earnings event — exhaustion capped at WARNING")

        results[sym] = r
        analyzed += 1

        if analyzed % 10 == 0 or analyzed == total:
            print(f"   Progress: {analyzed}/{total}")

    # Detect transitions
    transitions = {
        'newly_activated': [],
        'newly_parabolic': [],
        'newly_exhausted': [],
        'exhaustion_watch': [],
        'newly_cooled': [],
        'newly_earnings_hold': [],
        'still_activated': [],
    }

    for sym, r in results.items():
        new = r['combined_status']
        old = previous_states.get(sym, 'WATCHING')

        if new == 'EXHAUSTED' and old != 'EXHAUSTED':
            transitions['newly_exhausted'].append(r)
        elif new in ('ACTIVATED_EXHAUSTING', 'PARABOLIC_EXHAUSTING'):
            transitions['exhaustion_watch'].append(r)
        elif new == 'ACTIVATED' and old not in ('ACTIVATED', 'PARABOLIC', 'ACTIVATED_EXHAUSTING'):
            transitions['newly_activated'].append(r)
        elif new == 'PARABOLIC' and old != 'PARABOLIC':
            transitions['newly_parabolic'].append(r)
        elif new == 'EARNINGS_HOLD' and old != 'EARNINGS_HOLD':
            transitions['newly_earnings_hold'].append(r)
        elif new in ('WATCHING', 'ACTIVATING') and old in ('ACTIVATED', 'PARABOLIC', 'ACTIVATED_EXHAUSTING', 'PARABOLIC_EXHAUSTING'):
            r['combined_status'] = 'COOLED'
            transitions['newly_cooled'].append(r)
        elif new == 'ACTIVATED' and old == 'ACTIVATED':
            transitions['still_activated'].append(r)

    return results, transitions


# ============================================================================
# EMAIL
# ============================================================================

def _send_email(subject: str, html_body: str, dry_run: bool = False) -> bool:
    gmail_user = os.environ.get('GMAIL_USER')
    gmail_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient = os.environ.get('EMAIL_TO') or os.environ.get('EMAIL_RECIPIENT') or gmail_user

    if dry_run:
        print(f"\n   DRY RUN email — Subject: {subject}")
        return True

    if not gmail_user or not gmail_password:
        print("   Gmail credentials not set — email skipped")
        return False

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


def _row_html(r: Dict) -> str:
    """Build a table row for the unified email."""
    sym = r['symbol']
    status = r.get('combined_status', '?')
    act_score = r.get('activation_score', 0)
    exh_score = r.get('exhaustion_score', 0)
    act_sigs = "; ".join(r.get('activation_signals', [])[:2])
    exh_sigs = "; ".join(r.get('exhaustion_signals', [])[:2])
    sigs = act_sigs
    if exh_sigs and exh_score > 0:
        sigs = f"{act_sigs} | Exh: {exh_sigs}" if act_sigs else f"Exh: {exh_sigs}"
    chart = f"https://www.tradingview.com/chart/?symbol={sym}"

    colors = {
        'EXHAUSTED': '#ef4444', 'ACTIVATED_EXHAUSTING': '#f97316', 'PARABOLIC_EXHAUSTING': '#f97316',
        'PARABOLIC': '#dc2626', 'ACTIVATED': '#22c55e', 'ACTIVATING': '#f59e0b',
        'EARNINGS_HOLD': '#7c3aed', 'COOLED': '#3b82f6', 'WATCHING': '#6b7280',
        'WARNING': '#eab308', 'HEALTHY': '#6b7280',
    }
    color = colors.get(status, '#6b7280')

    return f"""
    <tr>
        <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; font-weight: bold;">
            <a href="{chart}" style="color: #2563eb; text-decoration: none;">{sym}</a></td>
        <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;
            color: {color}; font-weight: bold;">{status}</td>
        <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;">{act_score}</td>
        <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;">{exh_score}</td>
        <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; font-size: 12px;">{sigs[:80]}</td>
    </tr>"""


def _table(rows_html: str, header_bg: str = '#374151') -> str:
    return f"""
    <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px;
                  overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 15px;">
        <thead><tr style="background: {header_bg}; color: white;">
            <th style="padding: 8px; text-align: left;">Symbol</th>
            <th style="padding: 8px; text-align: center;">Status</th>
            <th style="padding: 8px; text-align: center;">Act</th>
            <th style="padding: 8px; text-align: center;">Exh</th>
            <th style="padding: 8px; text-align: left;">Signals</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
    </table>"""


def format_daily_email(results: Dict[str, Dict], transitions: Dict) -> Tuple[str, str]:
    """Build the unified daily email."""
    today_str = date.today().strftime("%b %d")

    # Subject logic
    n_exhausted = len(transitions['newly_exhausted'])
    n_activated = len(transitions['newly_activated'])
    if n_exhausted > 0:
        subject = f"Short Squeeze Daily — {n_exhausted} exhausting ({today_str})"
    elif n_activated > 0:
        subject = f"Short Squeeze Daily — {n_activated} new activations ({today_str})"
    else:
        subject = f"Short Squeeze Daily — Status update ({today_str})"

    sections = ""

    # Section 1: EXHAUSTION SIGNALS
    items = transitions['newly_exhausted']
    if items:
        rows = "".join(_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #ef4444; margin: 0 0 8px 0;">EXHAUSTED — SHORT SETUP ({len(items)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Squeeze exhaustion detected. Reversal signals firing. Review for short entry.</p>
            {_table(rows, '#991b1b')}
        </div>"""

    # Section 2: NEWLY ACTIVATED
    items = transitions['newly_activated'] + transitions['newly_parabolic']
    if items:
        rows = "".join(_row_html(r) for r in items)
        label = "PARABOLIC" if transitions['newly_parabolic'] else "NEWLY ACTIVATED"
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #22c55e; margin: 0 0 8px 0;">{label} ({len(items)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Squeeze activation confirmed at close. Breakout validated.</p>
            {_table(rows, '#166534')}
        </div>"""

    # Section 3: STILL ACTIVATED
    items = transitions['still_activated']
    if items:
        rows = "".join(_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #16a34a; margin: 0 0 8px 0;">STILL ACTIVATED ({len(items)})</h3>
            {_table(rows)}
        </div>"""

    # Section 4: EXHAUSTION WATCH
    items = transitions['exhaustion_watch']
    if items:
        rows = "".join(_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #f97316; margin: 0 0 8px 0;">EXHAUSTION WATCH ({len(items)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Early exhaustion signs detected. Watch for confirmation tomorrow.</p>
            {_table(rows, '#c2410c')}
        </div>"""

    # Section 5: EARNINGS HOLD
    items = transitions['newly_earnings_hold']
    if items:
        rows = "".join(_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #7c3aed; margin: 0 0 8px 0;">EARNINGS HOLD ({len(items)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">
                Earnings imminent — volatility play, not directional squeeze.</p>
            {_table(rows, '#5b21b6')}
        </div>"""

    # Section 6: COOLED
    items = transitions['newly_cooled']
    if items:
        rows = "".join(_row_html(r) for r in items)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #3b82f6; margin: 0 0 8px 0;">COOLED ({len(items)})</h3>
            {_table(rows, '#1e40af')}
        </div>"""

    # Section 7: TOP 10 WATCHLIST
    watching = sorted(
        [r for r in results.values() if r.get('combined_status') in ('WATCHING', 'ACTIVATING')],
        key=lambda x: x.get('activation_score', 0), reverse=True
    )[:10]
    if watching:
        rows = "".join(_row_html(r) for r in watching)
        sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #6b7280; margin: 0 0 8px 0;">TOP 10 WATCHLIST</h3>
            {_table(rows)}
        </div>"""

    if not sections:
        sections = '<p style="color: #9ca3af;">No significant activity today.</p>'

    # Summary counts
    status_counts = {}
    for r in results.values():
        s = r.get('combined_status', 'WATCHING')
        status_counts[s] = status_counts.get(s, 0) + 1
    counts_html = " | ".join(f"{k}: {v}" for k, v in sorted(status_counts.items()))

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
        <div style="background: #1e293b; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">Short Squeeze Daily</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">{today_str} — Post-Close</p>
        </div>
        <div style="background: #f9fafb; padding: 20px; border: 1px solid #e5e5e5; border-top: none;">
            {sections}
        </div>
        <div style="background: #f3f4f6; padding: 10px; border-radius: 0 0 8px 8px;
                    border: 1px solid #e5e5e5; border-top: none;">
            <p style="margin: 0; font-size: 11px; color: #9ca3af;">{counts_html}</p>
            <p style="margin: 3px 0 0 0; font-size: 11px; color: #9ca3af;">
                Phase 2 (activation) + Phase 3 (exhaustion) unified scanner</p>
        </div>
    </body></html>"""

    return subject, html


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Short Squeeze Daily Scanner — Phase 2 + 3')
    parser.add_argument('--dry-run', action='store_true', help='No writes, no emails')
    parser.add_argument('--limit', type=int, default=50, help='Number of candidates')
    args = parser.parse_args()

    print("=" * 60)
    print("Short Squeeze Daily Scanner — Phase 2 + Phase 3")
    print("=" * 60)
    print(f"Date: {date.today().strftime('%A, %B %d, %Y')}")
    if args.dry_run:
        print("MODE: DRY RUN (no Airtable writes, no emails)")
    print()

    if is_market_holiday():
        print("Market holiday — skipping.")
        return

    # Load candidates
    print("Loading candidates from Airtable...")
    at_records = fetch_top_watchlist(limit=args.limit)
    if not at_records:
        print("No candidates found. Run Phase 1 first.")
        return
    print(f"   Loaded {len(at_records)} candidates")

    # Run pipeline
    print("\nRunning Phase 2 (activation) + Phase 3 (exhaustion)...")
    results, transitions = run_daily_pipeline(at_records)

    # Summary
    print(f"\n{'='*60}")
    print(f"  DAILY SUMMARY")
    print(f"{'='*60}")
    for key in ['newly_exhausted', 'newly_activated', 'newly_parabolic',
                'still_activated', 'exhaustion_watch', 'newly_earnings_hold', 'newly_cooled']:
        print(f"  {key:<22}: {len(transitions[key])}")
    print(f"{'='*60}")

    # Detail table
    print(f"\n  {'Sym':<8} {'Status':<22} {'Act':>4} {'Exh':>4} {'DTE':>5}  Signals")
    print(f"  {'-'*8} {'-'*22} {'-'*4} {'-'*4} {'-'*5}  {'-'*45}")
    for sym in sorted(results.keys()):
        r = results[sym]
        dte = r.get('days_to_earnings')
        dte_s = f"{int(dte)}d" if dte is not None else "?"
        sigs = "; ".join((r.get('activation_signals', []) + r.get('exhaustion_signals', []))[:3])
        print(f"  {sym:<8} {r['combined_status']:<22} {r['activation_score']:>4} {r['exhaustion_score']:>4} {dte_s:>5}  {sigs[:55]}")

    # Push to Airtable
    if not args.dry_run:
        print("\nPushing results to Airtable...")
        count = push_daily_results(results, at_records)
        print(f"   Updated {count} records")
    else:
        print("\n   DRY RUN — Airtable writes skipped")

    # Email
    print("\nSending daily email...")
    subj, html = format_daily_email(results, transitions)
    _send_email(subj, html, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == '__main__':
    main()
