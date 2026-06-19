#!/usr/bin/env python3
"""
forward_dump.py — reporting-only tap for the forward-test harness.

Writes the engine's raw BUY/STRONG_BUY classifications (pre-sizing, pre-Airtable) to
a known path so the forward orchestration can read them. NO mutation of the input.

This is the symmetric reporting hook called from both Swing_System.py and
Swing_baseline.py — one tap site per engine, same point (right after final_signals
is extracted from system.run_analysis()).

Output path resolution:
  1. `path=` argument if explicitly passed
  2. Env var FWD_DUMP_PATH if set (orchestration sets this per-engine workdir)
  3. Default "raw_signals_dump.json" in CWD

Failures during write are SWALLOWED — reporting-only must never break the engine.
"""
import json
import os
from datetime import datetime


DEFAULT_PATH = "raw_signals_dump.json"


def dump_raw_signals(final_signals, path=None):
    """Write raw classified signals to a JSON sidecar. Returns count of BUY/STRONG_BUY
    signals captured (0 = no eligible signals; -1 = write failure).

    final_signals: dict[symbol -> signal_data] from system.run_analysis() results
    """
    if path is None:
        path = os.environ.get("FWD_DUMP_PATH", DEFAULT_PATH)

    raw = []
    for sym, data in (final_signals or {}).items():
        try:
            signal = data.get("signal")
        except AttributeError:
            continue
        if signal not in ("BUY", "STRONG_BUY"):
            continue
        try:
            score = float(data.get("combined_score", 0))
        except (TypeError, ValueError):
            score = 0.0
        raw.append({"ticker": sym, "score": score, "label": signal})

    try:
        with open(path, "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "raw_count": len(raw),
                "signals": raw,
            }, f, indent=2)
    except Exception:
        return -1

    return len(raw)


if __name__ == "__main__":
    # self-test (no engine / no network)
    test_signals = {
        "NVDA": {"signal": "STRONG_BUY", "combined_score": 0.62},
        "MU":   {"signal": "BUY",        "combined_score": 0.38},
        "AAPL": {"signal": "HOLD",       "combined_score": 0.15},   # filtered out
        "TSLA": {"signal": "SELL",       "combined_score": -0.42},  # filtered out
    }
    import tempfile, pathlib
    tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name
    n = dump_raw_signals(test_signals, path=tmp)
    assert n == 2, f"Expected 2 BUY/STRONG_BUY, got {n}"

    with open(tmp) as f:
        data = json.load(f)
    print(json.dumps(data, indent=2))
    print(f"OK — {n} signals written to {tmp}")
