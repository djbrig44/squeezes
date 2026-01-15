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
    get_default_tickers,
    scan_for_squeeze_fires,
    calculate_sunday_score,
)


def format_squeeze_email(fired_green: list) -> tuple:
    """
    Format GREEN fire results into an HTML email.
    Returns (subject, html_body).
    """
    today = date.today().strftime("%B %d, %Y")

    subject = f"🟢 Weekend Squeeze Report - {len(fired_green)} GREEN Fires ({today})"

    if not fired_green:
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #333;">Weekend Squeeze Scanner</h2>
            <p style="color: #666;">{today}</p>
            <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
                <p style="color: #888;">No GREEN fires detected this weekend.</p>
                <p style="color: #888; font-size: 12px;">The scanner found no stocks where the weekly TTM Squeeze fired bullish.</p>
            </div>
        </body>
        </html>
        """
        return subject, html

    # Sort by sunday_score descending
    sorted_stocks = sorted(fired_green, key=lambda x: x.get('sunday_score', 0), reverse=True)

    # Build table rows
    rows_html = ""
    for i, stock in enumerate(sorted_stocks, 1):
        symbol = stock['symbol']
        score = stock.get('sunday_score', 0)
        price = stock.get('current_price', 0)
        momentum = stock.get('momentum', 0)
        weekly_change = stock.get('weekly_change_pct', 0)
        bars = stock.get('bars_in_squeeze', 0)

        # Color code by score
        if score >= 80:
            score_color = "#22c55e"  # green
            row_bg = "#f0fdf4"
        elif score >= 60:
            score_color = "#84cc16"  # lime
            row_bg = "#fefce8"
        else:
            score_color = "#facc15"  # yellow
            row_bg = "#fffbeb"

        change_color = "#22c55e" if weekly_change >= 0 else "#ef4444"

        rows_html += f"""
        <tr style="background: {row_bg};">
            <td style="padding: 12px; border-bottom: 1px solid #e5e5e5; font-weight: bold;">{symbol}</td>
            <td style="padding: 12px; border-bottom: 1px solid #e5e5e5; text-align: right;">${price:.2f}</td>
            <td style="padding: 12px; border-bottom: 1px solid #e5e5e5; text-align: right; color: {change_color};">{weekly_change:+.1f}%</td>
            <td style="padding: 12px; border-bottom: 1px solid #e5e5e5; text-align: center; color: {score_color}; font-weight: bold;">{score:.0f}</td>
            <td style="padding: 12px; border-bottom: 1px solid #e5e5e5; text-align: right;">{momentum:.2f}</td>
            <td style="padding: 12px; border-bottom: 1px solid #e5e5e5; text-align: center;">{bars}</td>
        </tr>
        """

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
        <div style="background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%); color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">🟢 Weekend Squeeze Report</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">{today}</p>
        </div>

        <div style="background: #f9fafb; padding: 20px; border: 1px solid #e5e5e5; border-top: none;">
            <p style="margin: 0 0 15px 0; color: #374151;">
                <strong>{len(fired_green)} GREEN Fire{'' if len(fired_green) == 1 else 's'}</strong> detected -
                Weekly TTM Squeeze fired bullish on these stocks.
            </p>

            <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <thead>
                    <tr style="background: #374151; color: white;">
                        <th style="padding: 12px; text-align: left;">Ticker</th>
                        <th style="padding: 12px; text-align: right;">Price</th>
                        <th style="padding: 12px; text-align: right;">Week %</th>
                        <th style="padding: 12px; text-align: center;">Score</th>
                        <th style="padding: 12px; text-align: right;">Momentum</th>
                        <th style="padding: 12px; text-align: center;">Bars</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>

            <div style="margin-top: 20px; padding: 15px; background: #eff6ff; border-radius: 8px; font-size: 13px; color: #1e40af;">
                <strong>Score Legend:</strong> Higher scores indicate stronger setups based on momentum acceleration,
                time in squeeze, and weekly performance. Top candidates typically have scores above 70.
            </div>
        </div>

        <div style="background: #f3f4f6; padding: 15px; border-radius: 0 0 8px 8px; border: 1px solid #e5e5e5; border-top: none;">
            <p style="margin: 0; font-size: 12px; color: #6b7280;">
                Generated by Weekend Squeeze Scanner •
                <a href="https://github.com/djbrig44/squeezes" style="color: #2563eb;">GitHub</a>
            </p>
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
    recipient = os.environ.get('EMAIL_RECIPIENT', gmail_user)

    # Debug: show what we got (mask password)
    print(f"   DEBUG: GMAIL_USER={'set' if gmail_user else 'NOT SET'}")
    print(f"   DEBUG: GMAIL_APP_PASSWORD={'set' if gmail_password else 'NOT SET'}")
    print(f"   DEBUG: EMAIL_RECIPIENT={recipient or 'NOT SET'}")

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
        print("📊 Loading default trading universe...")
        symbols = get_default_tickers()

    # Run scanner
    print("\n🔍 Running squeeze analysis...")
    fired_green, fired_red, ready_to_fire, in_squeeze = scan_for_squeeze_fires(symbols)

    print(f"\n📈 Results:")
    print(f"   🟢 GREEN Fires: {len(fired_green)}")
    print(f"   🔴 RED Fires: {len(fired_red)}")
    print(f"   ⚡ Ready to Fire: {len(ready_to_fire)}")
    print(f"   🔵 In Squeeze: {len(in_squeeze)}")

    # Calculate sunday scores for sorting
    for stock in fired_green:
        if 'sunday_score' not in stock:
            stock['sunday_score'] = calculate_sunday_score(stock)

    # Format and send email
    subject, html_body = format_squeeze_email(fired_green)
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
