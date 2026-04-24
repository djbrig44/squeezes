#!/usr/bin/env python3
"""
Squeeze Email Reporter
======================
Runs the weekend squeeze scanner and emails GREEN fire results via Gmail SMTP.

Environment Variables Required:
  - GMAIL_USER: Your Gmail address (e.g., user@gmail.com)
  - GMAIL_APP_PASSWORD: Gmail App Password (16-character code from Google)
  - EMAIL_RECIPIENT: Email address to send report to (defaults to GMAIL_USER)

Setup Gmail App Password:
  1. Go to https://myaccount.google.com/apppasswords
  2. Select "Mail" and your device
  3. Copy the 16-character password
  4. Set as GMAIL_APP_PASSWORD environment variable

Usage:
  python send_squeeze_email.py              # Run scan and send email
  python send_squeeze_email.py --dry-run    # Run scan, show email, don't send
"""

import os
import sys
import smtplib
import argparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date

# Import the scanner - adjust path as needed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from weekend_squeeze_scanner import (
    get_swing_universe,
    scan_for_squeeze_fires,
    calculate_sunday_score,
    push_squeeze_signals_to_airtable,
    fetch_airtable_records,
)
from short_squeeze_watchlist import fetch_watchlist_records


def _build_fire_table(stocks: list, color: str, change_key: str = 'weekly_change_pct') -> str:
    """Build an HTML table for fired stocks (GREEN or RED)."""
    if not stocks:
        return ""
    rows = ""
    for stock in stocks:
        symbol = stock['symbol']
        price = stock.get('current_price', 0)
        momentum = stock.get('momentum', 0)
        change = stock.get(change_key, 0)
        bars = stock.get('bars_in_squeeze', 0)
        score = stock.get('sunday_score', 0)
        change_color = "#22c55e" if change >= 0 else "#ef4444"
        rows += f"""
        <tr>
            <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; font-weight: bold;">{symbol}</td>
            <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: right;">${price:.2f}</td>
            <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: right; color: {change_color};">{change:+.1f}%</td>
            <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: right;">{momentum:.2f}</td>
            <td style="padding: 10px; border-bottom: 1px solid #e5e5e5; text-align: center;">{bars}</td>
        </tr>"""
    header_bg = "#16a34a" if color == "green" else "#dc2626"
    return f"""
    <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 10px;">
        <thead><tr style="background: {header_bg}; color: white;">
            <th style="padding: 10px; text-align: left;">Ticker</th>
            <th style="padding: 10px; text-align: right;">Price</th>
            <th style="padding: 10px; text-align: right;">Week %</th>
            <th style="padding: 10px; text-align: right;">Momentum</th>
            <th style="padding: 10px; text-align: center;">Bars</th>
        </tr></thead>
        <tbody>{rows}</tbody>
    </table>"""


def _format_daily_status(daily_fields: dict) -> str:
    """Format daily overlay data into a compact status string for the email."""
    if not daily_fields:
        return '<span style="color: #9ca3af;">&mdash;</span>'
    status = daily_fields.get('Daily Squeeze Status', '')
    bars = int(daily_fields.get('Daily Bars', 0))
    alignment = daily_fields.get('Alignment', '')
    if alignment and 'DAILY FIRED' in alignment:
        return '<span style="color: #22c55e; font-weight: bold;">FIRED</span>'
    if status == 'FIRED_GREEN':
        return '<span style="color: #22c55e; font-weight: bold;">FIRED</span>'
    if status == 'FIRED_RED':
        return '<span style="color: #ef4444; font-weight: bold;">FIRED</span>'
    if status == 'READY':
        return f'<span style="color: #f59e0b;">Ready ({bars}b)</span>'
    if status == 'IN_SQUEEZE':
        return f'<span style="color: #3b82f6;">In Sq ({bars}b)</span>'
    return '<span style="color: #9ca3af;">&mdash;</span>'


def _build_ready_table(stocks: list, limit: int, daily_overlay: dict = None) -> str:
    """Build an HTML table for ready-to-fire or in-squeeze stocks."""
    if not stocks:
        return ""
    daily_overlay = daily_overlay or {}
    display = stocks[:limit]
    rows = ""
    for stock in display:
        symbol = stock['symbol']
        price = stock.get('current_price', 0)
        kc = stock.get('squeeze_state', '?')
        bars = stock.get('bars_in_squeeze', 0)
        momentum = stock.get('momentum', 0)
        rising = stock.get('momentum_rising', False)
        mom_sign = "+" if momentum > 0 else ""
        rising_icon = "^" if rising else "v"
        daily_fields = daily_overlay.get(symbol.upper(), {}).get('fields', {})
        daily_html = _format_daily_status(daily_fields)
        rows += f"""
        <tr>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; font-weight: bold;">{symbol}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: right;">${price:.2f}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;">{kc}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;">{bars}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: right;">{mom_sign}{momentum:.2f} {rising_icon}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;">{daily_html}</td>
        </tr>"""
    overflow = len(stocks) - limit
    if overflow > 0:
        rows += f"""
        <tr><td colspan="6" style="padding: 8px; text-align: center; color: #6b7280; font-size: 12px;">
            +{overflow} more in Airtable
        </td></tr>"""
    return f"""
    <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 10px;">
        <thead><tr style="background: #374151; color: white;">
            <th style="padding: 8px; text-align: left;">Ticker</th>
            <th style="padding: 8px; text-align: right;">Price</th>
            <th style="padding: 8px; text-align: center;">KC</th>
            <th style="padding: 8px; text-align: center;">Wk Bars</th>
            <th style="padding: 8px; text-align: right;">Momentum</th>
            <th style="padding: 8px; text-align: center;">Daily</th>
        </tr></thead>
        <tbody>{rows}</tbody>
    </table>"""


def _build_short_squeeze_table(watchlist: list, limit: int = 15) -> str:
    """Build HTML table for short squeeze watchlist top N."""
    if not watchlist:
        return '<p style="color: #9ca3af; font-size: 13px;">No short squeeze data available. Run short_squeeze_watchlist.py first.</p>'
    display = watchlist[:limit]
    rows = ""
    for item in display:
        f = item.get('fields', {})
        symbol = f.get('Symbol', '?')
        score = f.get('Score', 0)
        grade = f.get('Grade', '?')
        si_pct = f.get('SI % Float', 0)
        dtc = f.get('Days to Cover', 0)
        mspr = f.get('MSPR', 0)

        grade_color = '#22c55e' if grade == 'A' else '#f59e0b' if grade == 'B' else '#6b7280'
        rows += f"""
        <tr>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; font-weight: bold;">{symbol}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center;">{score:.0f}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: center; color: {grade_color}; font-weight: bold;">{grade}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: right;">{si_pct*100:.1f}%</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: right;">{dtc:.1f}</td>
            <td style="padding: 8px; border-bottom: 1px solid #e5e5e5; text-align: right;">{mspr:.1f}</td>
        </tr>"""
    overflow = len(watchlist) - limit
    if overflow > 0:
        rows += f"""
        <tr><td colspan="6" style="padding: 8px; text-align: center; color: #6b7280; font-size: 12px;">
            +{overflow} more in Airtable
        </td></tr>"""
    return f"""
    <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 10px;">
        <thead><tr style="background: #7c3aed; color: white;">
            <th style="padding: 8px; text-align: left;">Symbol</th>
            <th style="padding: 8px; text-align: center;">Score</th>
            <th style="padding: 8px; text-align: center;">Grade</th>
            <th style="padding: 8px; text-align: right;">SI%</th>
            <th style="padding: 8px; text-align: right;">DTC</th>
            <th style="padding: 8px; text-align: right;">MSPR</th>
        </tr></thead>
        <tbody>{rows}</tbody>
    </table>"""


def format_squeeze_email(fired_green: list, fired_red: list = None,
                         ready_to_fire: list = None, in_squeeze: list = None,
                         scan_stats: dict = None, daily_overlay: dict = None,
                         short_squeeze_watchlist: list = None) -> tuple:
    """
    Format full weekend squeeze report into an HTML email.
    Returns (subject, html_body).
    """
    fired_red = fired_red or []
    ready_to_fire = ready_to_fire or []
    in_squeeze = in_squeeze or []
    scan_stats = scan_stats or {}
    daily_overlay = daily_overlay or {}
    short_squeeze_watchlist = short_squeeze_watchlist or []
    today = date.today().strftime("%B %d, %Y")

    total_fires = len(fired_green) + len(fired_red)
    subject = f"Weekend Squeeze Report - {total_fires} Fires, {len(ready_to_fire)} Ready, {len(in_squeeze)} Building ({today})"

    # Split ready_to_fire by KC level
    ready_high = [s for s in ready_to_fire if s.get('squeeze_state') in ('HIGH', 'MID')]
    ready_low = [s for s in ready_to_fire if s.get('squeeze_state') not in ('HIGH', 'MID')]
    # Sort each by bars descending
    ready_high.sort(key=lambda x: x.get('bars_in_squeeze', 0), reverse=True)
    ready_low.sort(key=lambda x: x.get('bars_in_squeeze', 0), reverse=True)

    # Split in_squeeze by KC level
    squeeze_deep = [s for s in in_squeeze if s.get('squeeze_state') in ('HIGH', 'MID')]
    squeeze_deep.sort(key=lambda x: x.get('bars_in_squeeze', 0), reverse=True)

    # --- Summary banner ---
    summary_html = f"""
    <div style="background: #1e293b; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
        <h2 style="margin: 0;">Weekend Squeeze Report</h2>
        <p style="margin: 5px 0 0 0; opacity: 0.9;">{today}</p>
        <div style="margin-top: 15px; display: flex; gap: 20px; flex-wrap: wrap;">
            <span style="background: rgba(255,255,255,0.15); padding: 6px 12px; border-radius: 4px;">
                GREEN: <strong>{len(fired_green)}</strong></span>
            <span style="background: rgba(255,255,255,0.15); padding: 6px 12px; border-radius: 4px;">
                RED: <strong>{len(fired_red)}</strong></span>
            <span style="background: rgba(255,255,255,0.15); padding: 6px 12px; border-radius: 4px;">
                Ready (H/M): <strong>{len(ready_high)}</strong></span>
            <span style="background: rgba(255,255,255,0.15); padding: 6px 12px; border-radius: 4px;">
                Ready (L): <strong>{len(ready_low)}</strong></span>
            <span style="background: rgba(255,255,255,0.15); padding: 6px 12px; border-radius: 4px;">
                In Squeeze: <strong>{len(in_squeeze)}</strong></span>
        </div>
    </div>"""

    # --- Sections ---
    body_sections = ""

    # Section 1: GREEN FIRES
    if fired_green:
        sorted_green = sorted(fired_green, key=lambda x: x.get('sunday_score', 0), reverse=True)
        body_sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #16a34a; margin: 0 0 8px 0;">GREEN FIRES ({len(fired_green)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">Weekly squeeze fired bullish.</p>
            {_build_fire_table(sorted_green, "green")}
        </div>"""
    else:
        body_sections += """
        <div style="margin-bottom: 20px;">
            <h3 style="color: #16a34a; margin: 0 0 8px 0;">GREEN FIRES (0)</h3>
            <p style="color: #9ca3af; font-size: 13px;">No bullish fires this week.</p>
        </div>"""

    # Section 2: RED FIRES
    if fired_red:
        body_sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #dc2626; margin: 0 0 8px 0;">RED FIRES ({len(fired_red)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">Weekly squeeze fired bearish.</p>
            {_build_fire_table(fired_red, "red")}
        </div>"""

    # Section 3: READY TO FIRE — HIGH CONVICTION
    if ready_high:
        body_sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #7c3aed; margin: 0 0 8px 0;">READY TO FIRE — HIGH CONVICTION ({len(ready_high)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">HIGH or MID KC compression, 6+ weekly bars. Sorted by duration.</p>
            {_build_ready_table(ready_high, 15, daily_overlay)}
        </div>"""

    # Section 4: READY TO FIRE — BUILDING
    if ready_low:
        body_sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #6b7280; margin: 0 0 8px 0;">READY TO FIRE — BUILDING ({len(ready_low)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">LOW KC compression, 6+ weekly bars.</p>
            {_build_ready_table(ready_low, 10, daily_overlay)}
        </div>"""

    # Section 5: IN SQUEEZE — DEEP COMPRESSION
    if squeeze_deep:
        body_sections += f"""
        <div style="margin-bottom: 20px;">
            <h3 style="color: #2563eb; margin: 0 0 8px 0;">IN SQUEEZE — DEEP COMPRESSION ({len(squeeze_deep)})</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">HIGH or MID KC, building toward ready (&lt;6 bars).</p>
            {_build_ready_table(squeeze_deep, 10, daily_overlay)}
        </div>"""

    # Section 6: SHORT SQUEEZE WATCHLIST
    if short_squeeze_watchlist:
        body_sections += f"""
        <div style="margin-bottom: 20px; border-top: 2px solid #e5e5e5; padding-top: 20px;">
            <h3 style="color: #7c3aed; margin: 0 0 8px 0;">SHORT SQUEEZE WATCHLIST — Top 15</h3>
            <p style="color: #374151; margin: 0 0 10px 0; font-size: 13px;">Highest-scoring short squeeze candidates. Updated daily.</p>
            {_build_short_squeeze_table(short_squeeze_watchlist, 15)}
        </div>"""

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
        {summary_html}
        <div style="background: #f9fafb; padding: 20px; border: 1px solid #e5e5e5; border-top: none;">
            {body_sections}
        </div>
        <div style="background: #f3f4f6; padding: 15px; border-radius: 0 0 8px 8px; border: 1px solid #e5e5e5; border-top: none;">
            <p style="margin: 0; font-size: 12px; color: #6b7280;">
                Generated by Weekend Squeeze Scanner •
                <a href="https://github.com/djbrig44/squeezes" style="color: #2563eb;">GitHub</a>
            </p>
            {f'<p style="margin: 4px 0 0 0; font-size: 11px; color: #9ca3af;">Scanned {scan_stats["scanned"]} of {scan_stats["total"]} symbols successfully.</p>' if scan_stats.get("total") else ""}
        </div>
    </body>
    </html>
    """

    return subject, html


def send_email(subject: str, html_body: str, dry_run: bool = False) -> bool:
    """
    Send email via Gmail SMTP.
    Returns True on success.
    """
    gmail_user = os.environ.get('GMAIL_USER')
    gmail_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient = os.environ.get('EMAIL_RECIPIENT') or gmail_user  # Fallback to sender if not set

    if not gmail_user or not gmail_password:
        print("❌ Error: GMAIL_USER and GMAIL_APP_PASSWORD environment variables required")
        print("   Set these in your environment or GitHub Actions secrets")
        return False

    if not recipient:
        print("❌ Error: No recipient email address")
        return False

    if dry_run:
        print(f"\n📧 DRY RUN - Would send email:")
        print(f"   From: {gmail_user}")
        print(f"   To: {recipient}")
        print(f"   Subject: {subject}")
        print(f"\n   HTML Body Preview (first 500 chars):")
        print(f"   {html_body[:500]}...")
        return True

    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = gmail_user
    msg['To'] = recipient

    # Attach HTML
    html_part = MIMEText(html_body, 'html')
    msg.attach(html_part)

    try:
        print(f"📧 Sending email to {recipient}...")

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipient, msg.as_string())

        print(f"✅ Email sent successfully!")
        return True

    except smtplib.SMTPAuthenticationError:
        print("❌ Authentication failed. Check your Gmail app password.")
        print("   Get one at: https://myaccount.google.com/apppasswords")
        return False
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False


def run_scan_and_email(dry_run: bool = False, tickers: list = None):
    """
    Run the squeeze scanner and email GREEN fire results.
    """
    print("=" * 60)
    print("Weekend Squeeze Scanner - Email Reporter")
    print("=" * 60)
    print(f"Date: {date.today().strftime('%A, %B %d, %Y')}")
    print()

    # Get tickers
    if tickers:
        symbols = tickers
        print(f"📊 Scanning {len(symbols)} custom tickers...")
    else:
        print("📊 Loading swing trading universe...")
        symbols = get_swing_universe()

    # Run scanner
    print("\n🔍 Running squeeze analysis...")
    fired_green, fired_red, ready_to_fire, in_squeeze, scan_stats = scan_for_squeeze_fires(symbols, timeframe='weekly')

    print(f"\n📈 Results:")
    print(f"   🟢 GREEN Fires: {len(fired_green)}")
    print(f"   🔴 RED Fires: {len(fired_red)}")
    print(f"   ⚡ Ready to Fire: {len(ready_to_fire)}")
    print(f"   🔵 In Squeeze: {len(in_squeeze)}")
    print(f"   📊 Scanned {scan_stats['scanned']} of {scan_stats['total']} symbols successfully")

    # Calculate sunday scores for sorting
    for stock in fired_green:
        if 'sunday_score' not in stock:
            stock['sunday_score'] = calculate_sunday_score(stock)

    # Push weekly results to Airtable
    push_squeeze_signals_to_airtable(fired_green, fired_red, ready_to_fire, in_squeeze)

    # Fetch daily overlay from Airtable (written by the most recent daily scan)
    print("\n📊 Fetching daily overlay from Airtable...")
    daily_overlay = fetch_airtable_records()
    if daily_overlay:
        print(f"   Loaded daily data for {len(daily_overlay)} symbols")
    else:
        print("   No daily overlay available (Airtable read returned empty)")

    # Fetch short squeeze watchlist from Airtable
    print("\n📊 Fetching short squeeze watchlist from Airtable...")
    ss_records = fetch_watchlist_records()
    ss_watchlist = sorted(
        [{'fields': rec['fields']} for rec in ss_records.values()],
        key=lambda x: x['fields'].get('Score', 0),
        reverse=True,
    ) if ss_records else []
    print(f"   Loaded {len(ss_watchlist)} short squeeze candidates")

    # Format and send email
    subject, html_body = format_squeeze_email(
        fired_green, fired_red, ready_to_fire, in_squeeze,
        scan_stats=scan_stats, daily_overlay=daily_overlay,
        short_squeeze_watchlist=ss_watchlist,
    )
    success = send_email(subject, html_body, dry_run=dry_run)

    return success


def main():
    parser = argparse.ArgumentParser(description='Run squeeze scanner and email results')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show email preview without sending')
    parser.add_argument('--tickers', nargs='+',
                        help='Custom list of tickers to scan')

    args = parser.parse_args()

    success = run_scan_and_email(
        dry_run=args.dry_run,
        tickers=args.tickers
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
