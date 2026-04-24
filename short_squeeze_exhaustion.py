#!/usr/bin/env python3
"""
Short Squeeze Exhaustion Detection — Phase 3
==============================================
Pure function module. No Airtable writes, no emails.
Computes exhaustion signals from daily bar data for stocks
in ACTIVATED or PARABOLIC status.

Imported by short_squeeze_daily.py (the orchestrator).
"""

from datetime import date
from typing import Dict, Optional

import numpy as np
import pandas as pd


def compute_exhaustion_signals(symbol: str, df: pd.DataFrame,
                                prior_status: str = 'WATCHING',
                                activation_date: Optional[str] = None) -> Dict:
    """
    Compute exhaustion score (0-18) for a stock in ACTIVATED/PARABOLIC status.

    Returns {score, signals, status, reversal_strength}.
    Status bands: HEALTHY (0-3), WARNING (4-6), EXHAUSTING (7-10), EXHAUSTED (11+).
    """
    signals = []
    score = 0

    if df is None or len(df) < 10:
        return {'score': 0, 'signals': [], 'status': 'HEALTHY', 'reversal_strength': 0.0}

    close = df['Close']
    high = df['High']
    low = df['Low']
    volume = df['Volume']
    op = df['Open']

    today_close = float(close.iloc[-1])
    today_high = float(high.iloc[-1])
    today_low = float(low.iloc[-1])
    today_open = float(op.iloc[-1])
    today_vol = float(volume.iloc[-1])

    yesterday_close = float(close.iloc[-2])
    yesterday_high = float(high.iloc[-2])
    yesterday_low = float(low.iloc[-2])
    yesterday_open = float(op.iloc[-2])

    day_range = today_high - today_low
    close_position = (today_close - today_low) / day_range if day_range > 0 else 0.5

    # =====================================================================
    # REVERSAL CANDLE (max 5 points — take highest single, then add engulfing)
    # =====================================================================
    reversal_pts = 0

    # Gap up + close below prior close (blow-off top)
    gapped_up = today_open > yesterday_close * 1.02
    closed_below_prior = today_close < yesterday_close
    if gapped_up and closed_below_prior:
        reversal_pts = max(reversal_pts, 3)
        signals.append("Gap up + close below prior close (blow-off pattern)")
    elif close_position <= 0.2:
        reversal_pts = max(reversal_pts, 3)
        signals.append(f"Closed in bottom 20% of range (weak close, pos={close_position:.2f})")
    elif close_position <= 0.3:
        reversal_pts = max(reversal_pts, 2)
        signals.append(f"Closed in bottom 30% of range (pos={close_position:.2f})")
    elif close_position <= 0.5:
        reversal_pts = max(reversal_pts, 1)

    # Bearish engulfing (additive)
    yesterday_was_green = yesterday_close > yesterday_open
    today_is_red = today_close < today_open
    yesterday_body_high = max(yesterday_close, yesterday_open)
    yesterday_body_low = min(yesterday_close, yesterday_open)
    if (today_is_red and yesterday_was_green and
            today_open >= yesterday_body_high and today_close <= yesterday_body_low):
        reversal_pts += 2
        signals.append("Bearish engulfing candle")

    score += reversal_pts

    # =====================================================================
    # VOLUME CLIMAX (max 4 points)
    # =====================================================================
    avg_vol_20 = float(volume.tail(20).mean()) if len(volume) >= 20 else float(volume.mean())
    rel_vol = today_vol / avg_vol_20 if avg_vol_20 > 0 else 0
    today_is_red_close = today_close < today_open

    vol_pts = 0
    if today_is_red_close:
        if rel_vol > 3.0:
            vol_pts = 3
            signals.append(f"Distribution climax (RelVol {rel_vol*100:.0f}% on red)")
        elif rel_vol > 2.0:
            vol_pts = 2
            signals.append(f"Elevated distribution (RelVol {rel_vol*100:.0f}% on red)")

        # Highest volume in 20 days on red (additive)
        if len(volume) >= 20:
            max_vol_20 = float(volume.tail(20).max())
            if today_vol >= max_vol_20 and vol_pts > 0:
                vol_pts = min(vol_pts + 2, 4)
                signals.append("Highest volume in 20 days on red day")

    score += vol_pts

    # =====================================================================
    # MOMENTUM EXHAUSTION (max 5 points)
    # =====================================================================
    mom_pts = 0

    # 12-period ROC and acceleration
    if len(close) >= 14:
        roc = close.pct_change(12)
        roc_accel = roc.diff()

        current_roc = float(roc.iloc[-1]) if not pd.isna(roc.iloc[-1]) else 0
        prev_roc = float(roc.iloc[-2]) if not pd.isna(roc.iloc[-2]) else 0
        current_accel = float(roc_accel.iloc[-1]) if not pd.isna(roc_accel.iloc[-1]) else 0
        prev_accel = float(roc_accel.iloc[-2]) if not pd.isna(roc_accel.iloc[-2]) else 0

        # Acceleration crossed from positive to negative
        if prev_accel > 0 and current_accel < 0:
            mom_pts = max(mom_pts, 3)
            signals.append("Momentum acceleration rolled negative today")
        elif current_roc > 0 and current_accel < 0:
            mom_pts = max(mom_pts, 2)
            signals.append("Momentum positive but decelerating")

    # RSI(14) overbought and declining
    if len(close) >= 16:
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        rsi_vals = rsi.dropna().tail(3)
        if len(rsi_vals) >= 3:
            current_rsi = float(rsi_vals.iloc[-1])
            if current_rsi > 70 and float(rsi_vals.iloc[-1]) < float(rsi_vals.iloc[-2]) < float(rsi_vals.iloc[-3]):
                mom_pts = min(mom_pts + 2, 5)
                signals.append(f"RSI overbought ({current_rsi:.0f}) and rolling down")

    # Bear divergence: price new high AND momentum lower high (5-day window)
    if len(close) >= 7 and len(close) >= 14:
        price_5d_highs = high.tail(5)
        roc_5d = close.pct_change(12).tail(5)
        if (float(price_5d_highs.iloc[-1]) >= float(price_5d_highs.max()) and
                not roc_5d.empty and float(roc_5d.iloc[-1]) < float(roc_5d.max()) * 0.95):
            mom_pts = min(mom_pts + 2, 5)
            signals.append("Bear divergence — price higher, momentum lower")

    score += mom_pts

    # =====================================================================
    # CONTEXT AMPLIFIERS (max 4 points — require prior_status)
    # =====================================================================
    ctx_pts = 0

    if prior_status == 'PARABOLIC':
        ctx_pts += 2
        signals.append("Parabolic state — blow-off candidate")
    elif prior_status == 'ACTIVATED':
        # Check if recently activated
        if activation_date:
            try:
                act_date = date.fromisoformat(activation_date[:10])
                days_since = (date.today() - act_date).days
                if days_since <= 10:
                    ctx_pts += 1
                    signals.append(f"Recently activated ({days_since}d ago), now reversing")
            except (ValueError, TypeError):
                pass

    # Extension check
    if len(close) >= 20:
        pct_20d = (float(close.iloc[-1]) - float(close.iloc[-20])) / float(close.iloc[-20])
        if pct_20d > 0.60:
            ctx_pts = min(ctx_pts + 2, 4)
            signals.append(f"Extreme extension +{pct_20d*100:.0f}% in 20 days")
        elif pct_20d > 0.30:
            ctx_pts = min(ctx_pts + 1, 4)
            signals.append(f"Extended +{pct_20d*100:.0f}% in 20 days")

    score += ctx_pts

    # =====================================================================
    # STATUS BANDS
    # =====================================================================
    if score >= 11:
        status = 'EXHAUSTED'
    elif score >= 7:
        status = 'EXHAUSTING'
    elif score >= 4:
        status = 'WARNING'
    else:
        status = 'HEALTHY'

    # Reversal strength composite (0-1)
    reversal_strength = min(score / 18.0, 1.0)

    return {
        'score': score,
        'signals': signals,
        'status': status,
        'reversal_strength': reversal_strength,
    }
