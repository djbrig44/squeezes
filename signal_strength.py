"""
Signal Strength Module for Sharp/Swing Trading System
Updated with Tiered Sizing based on EV Analysis
"""
from dataclasses import dataclass
from typing import Tuple, Optional

# Configuration - tuned via EV analysis
DEAD_ZONE_THRESHOLD = 0.10      # Below this = skip entirely
MEDIUM_THRESHOLD = 0.20         # Below this = reduced size
FULL_CONFIDENCE_THRESHOLD = 0.20  # At or above = full size

# Confidence tiers
DEAD_ZONE_CONFIDENCE = 0.0      # No trade
MEDIUM_CONFIDENCE = 0.50        # 50% position size
FULL_CONFIDENCE = 1.0           # 100% position size

@dataclass
class SignalStrength:
    direction: str
    confidence: float
    raw_score: float
    should_trade: bool
    tier: str
    reason: str

def get_signal_strength(score: float, 
                        dead_zone: float = DEAD_ZONE_THRESHOLD,
                        medium_threshold: float = MEDIUM_THRESHOLD) -> SignalStrength:
    """
    Convert raw score to actionable signal with tiered confidence.
    
    Tiers (based on EV analysis):
    - Score < 0.10: SKIP (dead zone, 52% win rate)
    - Score 0.10-0.20: MEDIUM (50% size, transitional)
    - Score >= 0.20: FULL (100% size, 100% win rate in backtest)
    """
    abs_score = abs(score)
    
    # Tier 1: Dead zone - skip entirely
    if abs_score < dead_zone:
        return SignalStrength(
            direction="HOLD",
            confidence=DEAD_ZONE_CONFIDENCE,
            raw_score=score,
            should_trade=False,
            tier="DEAD_ZONE",
            reason=f"Score {abs_score:.3f} < {dead_zone} (dead zone - skip)"
        )
    
    direction = "BUY" if score > 0 else "SELL"
    
    # Tier 2: Medium confidence - reduced size
    if abs_score < medium_threshold:
        return SignalStrength(
            direction=direction,
            confidence=MEDIUM_CONFIDENCE,
            raw_score=score,
            should_trade=True,
            tier="MEDIUM",
            reason=f"Score {abs_score:.3f} in [{dead_zone}, {medium_threshold}) - 50% size"
        )
    
    # Tier 3: Full confidence - full size
    return SignalStrength(
        direction=direction,
        confidence=FULL_CONFIDENCE,
        raw_score=score,
        should_trade=True,
        tier="FULL",
        reason=f"Score {abs_score:.3f} >= {medium_threshold} - full size"
    )

def integrate_with_existing_system(signal_data: dict, score_key: str = "combined_score",
                                   dead_zone: float = DEAD_ZONE_THRESHOLD) -> dict:
    """Drop-in integration for existing signal dictionaries."""
    score = signal_data.get(score_key, 0)
    
    # Also check fib_score as fallback
    if score == 0:
        score = signal_data.get('fib_score', 0)
    
    sig = get_signal_strength(score, dead_zone=dead_zone)
    
    if not sig.should_trade:
        return None
    
    result = signal_data.copy()
    result["signal_direction"] = sig.direction
    result["signal_confidence"] = sig.confidence
    result["signal_should_trade"] = sig.should_trade
    result["signal_tier"] = sig.tier
    result["signal_reason"] = sig.reason
    return result

def calculate_position_size(base_size: float, score: float,
                           dead_zone: float = DEAD_ZONE_THRESHOLD,
                           medium_threshold: float = MEDIUM_THRESHOLD) -> float:
    """
    Calculate position size based on signal tier.
    
    Args:
        base_size: Base position size (e.g., 0.05 for 5%)
        score: Signal score (fib_score or combined_score)
    
    Returns:
        Adjusted position size
    """
    sig = get_signal_strength(score, dead_zone, medium_threshold)
    return base_size * sig.confidence

# Quick test
if __name__ == "__main__":
    print("=" * 60)
    print("TIERED SIGNAL STRENGTH TEST")
    print("=" * 60)
    
    test_scores = [0.05, 0.08, 0.12, 0.15, 0.18, 0.22, 0.30, 0.45]
    base_position = 0.05  # 5% base
    
    print(f"\n{'Score':<8} {'Tier':<12} {'Confidence':<12} {'Pos Size':<10} {'Trade?'}")
    print("-" * 60)
    
    for score in test_scores:
        sig = get_signal_strength(score)
        pos_size = calculate_position_size(base_position, score)
        print(f"{score:<8.2f} {sig.tier:<12} {sig.confidence:<12.0%} {pos_size:<10.2%} {sig.should_trade}")
    
    print("\n✅ Tiered sizing active:")
    print(f"   Dead zone: < {DEAD_ZONE_THRESHOLD}")
    print(f"   Medium (50%): {DEAD_ZONE_THRESHOLD} - {MEDIUM_THRESHOLD}")
    print(f"   Full (100%): >= {MEDIUM_THRESHOLD}")
