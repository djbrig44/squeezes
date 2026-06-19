#!/usr/bin/env python3
"""
nightly_forward_harness.py — runs both engines, captures raw signal dumps via the
forward_dump tap, and appends to forward_signals.csv via forward_signal_adapter.

Architecture (per user spec):
  - BUNDLE  (live)  : Swing_System.py, --mode live, writes prod Airtable as normal,
                      version pin = MD5 of the file on disk at run-time
  - BASELINE (paper): Swing_baseline.py, --mode live --no-airtable, version pin
                      FROZEN at f9f9936a2f (post-hook MD5; do not change after freeze)

Guardrails:
  (a) Baseline runs --no-airtable → no production Airtable write
  (b) Baseline runs in a separate working dir → its incidental file writes
      (picks_*.csv, fibonacci_metrics_*.csv, execution_log) cannot collide with
      the bundle's live outputs that the SOP reads
  (c) Both engines pinned to their MD5 hash at run-time; baseline's pin is constant
      across nights because the file is frozen — drift would indicate freeze break

Versioning: matches engine's internal _bt_code_version() (md5[:10] of file content),
not git short-hash — so forward log provenance lines up with backtest_runs.csv.

Pre-registered parameters (set by user, NEVER moved):
  - --sample-target  (forward_test_score.py CLI)
  - --edge-bar       (forward_test_score.py CLI)
  These do not appear in this harness — they live in the scoring step. This script
  only collects nightly signals.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Append-signals lives in the adapter module.
from forward_signal_adapter import append_signals

REPO = Path(__file__).resolve().parent  # harness lives at repo root; works local + Actions
FWD_LOG = REPO / "forward_signals.csv"
LA = ZoneInfo("America/Los_Angeles")

BUNDLE_ENGINE = "Swing_System.py"
BASELINE_ENGINE = "Swing_baseline.py"

# Per-engine isolated working dir (guardrail b)
BUNDLE_WORKDIR = REPO / "nightly_runs" / "bundle"
BASELINE_WORKDIR = REPO / "nightly_runs" / "baseline"

# Per-engine raw-signal dump path passed via FWD_DUMP_PATH env var
BUNDLE_DUMP = BUNDLE_WORKDIR / "raw_signals_dump.json"
BASELINE_DUMP = BASELINE_WORKDIR / "raw_signals_dump.json"

# Frozen baseline pin (post-hook MD5; matches engine's _bt_code_version() scheme)
# NEVER change this after the freeze starts. A live md5 drift on the baseline file
# would indicate the freeze has been broken.
BASELINE_PIN_FROZEN = "d8db114bc8"

# Bundle freeze tripwire — the bundle's MD5 at test start. The bundle is live, so we
# pin it to a runtime md5 in the log, but commit to NOT editing signal logic during
# the test window. If the live md5 drifts off this value, the test has been
# contaminated: segment the forward log at that date or restart. Warning, not halt —
# bundle still runs (production trades depend on it); user decides on segmentation.
BUNDLE_PIN_EXPECTED = "aa2e590339"

# Data dependencies the engines need to find at runtime, relative to the engine's CWD.
# These are symlinked into each workdir so the engine sees them at the expected path.
# Update this list if you add new runtime deps that resolve relative to CWD.
RUNTIME_DEPS = ["cache", ".env"]


def md5_short(path):
    """MD5 of file content, first 10 chars. Matches Swing_*.py _bt_code_version()."""
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()[:10]


def _ensure_workdir(workdir):
    """Create workdir and symlink runtime deps from REPO so the engine resolves them."""
    workdir.mkdir(parents=True, exist_ok=True)
    for dep in RUNTIME_DEPS:
        src = REPO / dep
        dst = workdir / dep
        if src.exists() and not dst.exists():
            try:
                os.symlink(src, dst)
            except OSError:
                pass  # symlink may fail on some filesystems; engine will surface


def _check_baseline_freeze():
    """Guardrail: live MD5 of baseline file must equal the frozen pin."""
    current = md5_short(REPO / BASELINE_ENGINE)
    if current != BASELINE_PIN_FROZEN:
        print(f"⚠️  BASELINE FREEZE BROKEN: live md5={current} vs pin={BASELINE_PIN_FROZEN}",
              file=sys.stderr)
        print(f"   Forward-test integrity compromised. Halting paper leg.", file=sys.stderr)
        return False
    return True


def _check_bundle_drift(current_pin):
    """Tripwire (warning, not halt): if bundle md5 drifts off the test-start pin,
    signal logic changed during the test window. Bundle still runs — production
    trades depend on it — but user must decide: segment forward log at this date,
    or restart the test. Logged loudly to stderr; orchestration does not stop."""
    if current_pin != BUNDLE_PIN_EXPECTED:
        print(f"\n⚠️  ⚠️  BUNDLE HASH DRIFTED — TEST CONTAMINATED  ⚠️  ⚠️",
              file=sys.stderr)
        print(f"   live md5={current_pin} vs test-start pin={BUNDLE_PIN_EXPECTED}",
              file=sys.stderr)
        print(f"   Signal logic changed mid-test. Forward log integrity broken from this date.",
              file=sys.stderr)
        print(f"   Action: segment forward_signals.csv at this date OR restart the test.",
              file=sys.stderr)
        print(f"   (Bundle still runs — production depends on it. User decides on segmentation.)\n",
              file=sys.stderr)
        return False
    return True


def run_engine(engine_file, workdir, dump_path, no_airtable):
    """Invoke engine in --mode live; set FWD_DUMP_PATH so the tap writes per-engine."""
    _ensure_workdir(workdir)
    env = os.environ.copy()
    env["FWD_DUMP_PATH"] = str(dump_path)
    cmd = [sys.executable, str(REPO / engine_file), "--mode", "live"]
    if no_airtable:
        cmd.append("--no-airtable")
    print(f"[{datetime.now(LA).strftime('%H:%M:%S')}] {engine_file} → cwd={workdir.name}, "
          f"dump={dump_path.name}, no_airtable={no_airtable}")
    proc = subprocess.run(cmd, cwd=workdir, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"   ⚠️ exit {proc.returncode} — stderr tail:", file=sys.stderr)
        print(proc.stderr[-2000:], file=sys.stderr)
    return proc.returncode


def read_dump(path):
    """Read the engine's raw-signal JSON sidecar; return list of signal dicts."""
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        return data.get("signals", [])
    except Exception as e:
        print(f"⚠️ Failed reading {path}: {e}", file=sys.stderr)
        return []


def main():
    today = datetime.now(LA).strftime("%Y-%m-%d")
    print(f"\n=== Forward harness — {today} ===")

    # Track engine failures and propagate to workflow exit code. Sentinel rows are
    # only written when an engine succeeded but produced zero signals (legitimate
    # no-signal night). On engine crash, we skip the sentinel and exit non-zero —
    # workflow goes RED, the log isn't polluted with rows that misrepresent failure.
    exit_code = 0

    # ---- BUNDLE (live) ----
    bundle_pin = md5_short(REPO / BUNDLE_ENGINE)
    print(f"\n[BUNDLE]  pin (live md5): {bundle_pin}")
    _check_bundle_drift(bundle_pin)   # warns if md5 drifted off test-start pin
    rc_b = run_engine(BUNDLE_ENGINE, BUNDLE_WORKDIR, BUNDLE_DUMP, no_airtable=False)
    if rc_b == 0:
        bundle_signals = read_dump(BUNDLE_DUMP)
        n_b = append_signals(str(FWD_LOG), engine="bundle", signals=bundle_signals,
                             signal_date=today, engine_version=bundle_pin)
        print(f"   appended {n_b} signal rows")
    else:
        print(f"   ⚠️ BUNDLE engine FAILED (exit {rc_b}) — no log write; workflow will exit red",
              file=sys.stderr)
        exit_code = max(exit_code, rc_b)

    # ---- BASELINE (paper) ----
    print(f"\n[BASELINE] pin (frozen): {BASELINE_PIN_FROZEN}")
    if not _check_baseline_freeze():
        print("   skipping baseline paper run due to freeze break")
        return max(exit_code, 1)
    rc_x = run_engine(BASELINE_ENGINE, BASELINE_WORKDIR, BASELINE_DUMP, no_airtable=True)
    if rc_x == 0:
        baseline_signals = read_dump(BASELINE_DUMP)
        n_x = append_signals(str(FWD_LOG), engine="baseline", signals=baseline_signals,
                             signal_date=today, engine_version=BASELINE_PIN_FROZEN)
        print(f"   appended {n_x} signal rows")
    else:
        print(f"   ⚠️ BASELINE engine FAILED (exit {rc_x}) — no log write; workflow will exit red",
              file=sys.stderr)
        exit_code = max(exit_code, rc_x)

    print(f"\n[{datetime.now(LA).strftime('%H:%M:%S')}] forward log updated: {FWD_LOG}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
