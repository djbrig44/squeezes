#!/usr/bin/env python3
"""
forward_signal_adapter.py — turns one nightly engine run into forward_signals.csv rows.

Wire this into BOTH engines' nightly path (see the >>> CC WIRES HERE <<< block at the
bottom). It is deliberately thin: you hand it the night's BUY/STRONG_BUY candidates as a
list of dicts; it handles versioning, ranking, dedup, no-signal sentinels, header
creation, and the append. The only environment-specific piece is how you extract that
candidate list from daily_exec.run_daily_update() — that's the seam you fill.

Schema written (exact column order the scorer reads):
    signal_date, engine, engine_version, ticker, score, label, rank

Conventions baked in (decided, not guessed):
  * No-signal night  -> ONE sentinel row: ticker="", label="NONE", rank=0. Keeps the
                        (date, engine) pair visible so gaps mean "didn't run", not
                        "ran, nothing fired". Sentinels are inert to forward_test_score
                        (empty ticker -> NaN fwd returns -> dropped from aggregation).
  * engine_version   -> explicit pin wins; else auto-resolve `git rev-parse --short HEAD`
                        in engine_dir; else "unknown". Pin the FROZEN baseline; let the
                        LIVE bundle auto-resolve.
  * rank             -> single ranking over BUY+STRONG_BUY combined, score desc (1=best).
  * label filter     -> only BUY / STRONG_BUY are logged; everything else dropped.
  * dedup            -> if a ticker appears twice in one night, keep the higher score.
"""

import csv
import os
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

SCHEMA = ["signal_date", "engine", "engine_version", "ticker", "score", "label", "rank"]
VALID_LABELS = {"BUY", "STRONG_BUY"}
LA = ZoneInfo("America/Los_Angeles")


def resolve_version(engine_dir=None, pin=None):
    """Pin wins (use for the frozen baseline). Else git short-hash of engine_dir's HEAD
    (use for the live bundle). Else 'unknown'."""
    if pin:
        return str(pin)
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=engine_dir or os.getcwd(),
            capture_output=True, text=True, timeout=10,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _normalize(signals):
    """Filter to BUY/STRONG_BUY, coerce types, dedup by ticker (keep higher score),
    return sorted-by-score-desc list of (ticker, score, label)."""
    best = {}
    for s in signals or []:
        label = str(s.get("label", "")).upper().strip()
        if label not in VALID_LABELS:
            continue
        tk = str(s.get("ticker", "")).upper().strip()
        if not tk:
            continue
        try:
            score = float(s.get("score"))
        except (TypeError, ValueError):
            continue
        if tk not in best or score > best[tk][1]:
            best[tk] = (tk, score, label)
    return sorted(best.values(), key=lambda x: x[1], reverse=True)


def append_signals(csv_path, engine, signals, signal_date=None,
                   engine_version=None, engine_dir=None):
    """Append one night's rows for one engine. Returns the number of signal rows written
    (0 means a sentinel row was written instead)."""
    if signal_date is None:
        signal_date = datetime.now(LA).strftime("%Y-%m-%d")
    version = resolve_version(engine_dir=engine_dir, pin=engine_version)
    ranked = _normalize(signals)

    rows = []
    if ranked:
        for i, (tk, score, label) in enumerate(ranked, start=1):
            rows.append({
                "signal_date": signal_date, "engine": engine, "engine_version": version,
                "ticker": tk, "score": round(score, 6), "label": label, "rank": i,
            })
    else:  # no-signal night -> sentinel
        rows.append({
            "signal_date": signal_date, "engine": engine, "engine_version": version,
            "ticker": "", "score": "", "label": "NONE", "rank": 0,
        })

    new_file = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SCHEMA)
        if new_file:
            w.writeheader()
        w.writerows(rows)

    return len(ranked)


# --------------------------------------------------------------------------- #
# >>> CC WIRES HERE <<<  — the one environment-specific seam.
# After daily_exec.run_daily_update() returns, extract the night's BUY/STRONG_BUY
# candidates into a list of {"ticker","score","label"} dicts and call append_signals.
# Replace the extraction below with the real shape of run_daily_update()'s output
# (or read back what it wrote). Everything else above is engine-agnostic.
#
#   from forward_signal_adapter import append_signals
#
#   FWD_LOG = "/Users/djbrig/squeezes/forward_signals.csv"
#
#   # ----- BUNDLE (live; auto-resolve version from the deployed HEAD) -----
#   result = daily_exec.run_daily_update(...)          # normal live run (writes prod)
#   candidates = [                                     # <-- adapt to real output
#       {"ticker": s["ticker"], "score": s["combined_score"], "label": s["label"]}
#       for s in result.signals                        #     e.g. result.signals / a dict
#   ]
#   append_signals(FWD_LOG, engine="bundle", signals=candidates,
#                  engine_dir="/Users/djbrig/squeezes")   # version = git HEAD
#
#   # ----- BASELINE (paper; --no-airtable; PINNED frozen version) -----
#   result_b = daily_exec_baseline.run_daily_update(..., no_airtable=True)  # NO prod write
#   candidates_b = [ {"ticker": s["ticker"], "score": s["combined_score"],
#                     "label": s["label"]} for s in result_b.signals ]
#   append_signals(FWD_LOG, engine="baseline", signals=candidates_b,
#                  engine_version="c5cda9b569")            # frozen pin, NOT git HEAD
#
# Guardrail check: the baseline branch must run with --no-airtable and must touch ONLY
# FWD_LOG — never the production tables.
# --------------------------------------------------------------------------- #


if __name__ == "__main__":
    # self-test (no repo / no network needed)
    import tempfile, pathlib
    d = tempfile.mkdtemp()
    p = str(pathlib.Path(d) / "fwd.csv")

    # night 1: bundle with signals (dedup + ranking + label filter)
    n = append_signals(p, "bundle", [
        {"ticker": "nvda", "score": 0.61, "label": "STRONG_BUY"},
        {"ticker": "MU", "score": 0.34, "label": "BUY"},
        {"ticker": "MU", "score": 0.41, "label": "BUY"},     # dup -> keep 0.41
        {"ticker": "HOLDME", "score": 0.9, "label": "HOLD"}, # filtered out
    ], signal_date="2026-06-22", engine_version="c118791c43")
    assert n == 2, n

    # night 1: baseline no signals -> sentinel
    n0 = append_signals(p, "baseline", [], signal_date="2026-06-22",
                        engine_version="c5cda9b569")
    assert n0 == 0, n0

    print(pathlib.Path(p).read_text())
    print("version auto-resolve (no repo) ->", resolve_version(engine_dir="/tmp"))
    print("OK")
