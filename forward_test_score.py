#!/usr/bin/env python3
"""
forward_test_score.py — Forward/paper A/B scorer for the signal-engine comparison.

Compares two engines (baseline = paper control, bundle = live) on OUT-OF-SAMPLE,
real forward returns. It scores the DIVERGENT picks only — names one engine flagged
that the other did not on the same night — because shared picks carry no edge signal.

This sidesteps every backtest caveat: real forward data (out-of-sample), real prices
(no fixed/Close-exit modeling), and it scores the signal LISTS, not a portfolio sim,
so the 10-vs-25 position cap is irrelevant.

INPUT — append-only signals log (CSV), one row per (signal_date, engine, ticker):
    signal_date    YYYY-MM-DD   (EOD date the signal was generated)
    engine         baseline | bundle
    engine_version short code hash, e.g. c5cda9b569 / c118791c43   (for integrity)
    ticker         e.g. NVDA
    score          float
    label          BUY | STRONG_BUY
    rank           int (rank within that night's list; optional)

Entry/exit convention (applied IDENTICALLY to both engines so the comparison is fair —
this is a mechanical proxy for relative signal quality, NOT your actual fills):
    entry  = NEXT trading day's OPEN after signal_date
    exit   = CLOSE at +5 / +10 / +21 trading days (counted on the price calendar)

Usage:
    python3 forward_test_score.py --signals forward_signals.csv --as-of 2026-07-15 \
        --sample-target 30 --edge-bar 0.50

The decision rule is PRE-REGISTERED via CLI flags. Set them once at test start and do
not move them — that is the whole discipline of a forward test.
"""

import argparse
import sys
import pandas as pd
import numpy as np

HORIZONS = [5, 10, 21]   # trading-day forward horizons


# --------------------------------------------------------------------------- #
# Price fetch — isolated so it can be mocked in tests. Uses yfinance in prod.
# --------------------------------------------------------------------------- #
def fetch_prices(tickers, start, end):
    """Bulk-download OHLC for all tickers. Returns {ticker: DataFrame[Open,Close]}
    indexed by trading date. Replace/extend the source if you use a different feed."""
    import yfinance as yf
    raw = yf.download(list(tickers), start=start, end=end, progress=False,
                      group_by="ticker", auto_adjust=False)
    out = {}
    for t in tickers:
        try:
            df = raw[t] if isinstance(raw.columns, pd.MultiIndex) else raw
            df = df[["Open", "Close"]].dropna()
            if not df.empty:
                out[t] = df
        except Exception:
            pass
    return out


# --------------------------------------------------------------------------- #
# Core logic (pure / testable — no network)
# --------------------------------------------------------------------------- #
def classify_divergence(sig: pd.DataFrame) -> pd.DataFrame:
    """Tag each row's cohort per (signal_date, ticker): bundle-only / baseline-only /
    shared, based on whether the OTHER engine also flagged that name that night."""
    sig = sig.copy()
    flagged = (sig.groupby(["signal_date", "ticker"])["engine"]
                  .agg(lambda s: set(s)).rename("engines"))
    sig = sig.merge(flagged, on=["signal_date", "ticker"], how="left")

    def cohort(row):
        eng = row["engines"]
        if "baseline" in eng and "bundle" in eng:
            return "shared"
        return f"{row['engine']}-only"

    sig["cohort"] = sig.apply(cohort, axis=1)
    return sig.drop(columns=["engines"])


def score_forward_returns(sig: pd.DataFrame, prices: dict, as_of: pd.Timestamp,
                          horizons=HORIZONS) -> pd.DataFrame:
    """For each signal, entry = next trading-day open after signal_date; forward
    return at each horizon = close(entry_idx + h) / entry_open - 1. Marks a horizon
    NaN (pending) if that bar hasn't occurred on/before as_of."""
    rows = []
    for _, r in sig.iterrows():
        rec = r.to_dict()
        px = prices.get(r["ticker"])
        sd = pd.Timestamp(r["signal_date"])
        if px is None or px.empty:
            for h in horizons:
                rec[f"fwd_{h}d"] = np.nan
            rows.append(rec)
            continue
        future = px.index[px.index > sd]
        if len(future) == 0:
            for h in horizons:
                rec[f"fwd_{h}d"] = np.nan
            rows.append(rec)
            continue
        entry_date = future[0]
        entry_open = float(px.loc[entry_date, "Open"])
        entry_pos = px.index.get_loc(entry_date)
        for h in horizons:
            exit_pos = entry_pos + h
            # complete only if the exit bar exists AND has occurred by as_of
            if exit_pos < len(px.index) and px.index[exit_pos] <= as_of:
                exit_close = float(px.iloc[exit_pos]["Close"])
                rec[f"fwd_{h}d"] = exit_close / entry_open - 1.0
            else:
                rec[f"fwd_{h}d"] = np.nan
        rows.append(rec)
    return pd.DataFrame(rows)


def aggregate(scored: pd.DataFrame, horizons=HORIZONS) -> pd.DataFrame:
    """Per cohort x horizon: completed n, mean %, median %, hit rate %."""
    recs = []
    for cohort in ["bundle-only", "baseline-only", "shared"]:
        sub = scored[scored["cohort"] == cohort]
        for h in horizons:
            col = f"fwd_{h}d"
            done = sub[col].dropna()
            recs.append({
                "cohort": cohort, "horizon_d": h, "n_completed": len(done),
                "mean_pct": round(done.mean() * 100, 3) if len(done) else np.nan,
                "median_pct": round(done.median() * 100, 3) if len(done) else np.nan,
                "hit_rate_pct": round((done > 0).mean() * 100, 1) if len(done) else np.nan,
            })
    return pd.DataFrame(recs)


def edge_table(agg: pd.DataFrame, horizons=HORIZONS) -> pd.DataFrame:
    """bundle-only minus baseline-only, per horizon."""
    recs = []
    for h in horizons:
        b = agg[(agg.cohort == "bundle-only") & (agg.horizon_d == h)]
        c = agg[(agg.cohort == "baseline-only") & (agg.horizon_d == h)]
        bm = b["mean_pct"].iloc[0] if len(b) else np.nan
        cm = c["mean_pct"].iloc[0] if len(c) else np.nan
        recs.append({"horizon_d": h, "bundle_only_mean_pct": bm,
                     "baseline_only_mean_pct": cm,
                     "edge_pp": round(bm - cm, 3) if pd.notna(bm) and pd.notna(cm) else np.nan})
    return pd.DataFrame(recs)


def decision(agg: pd.DataFrame, edges: pd.DataFrame,
             confirm_sample: int, confirm_bar: float,
             revert_sample: int, revert_bar: float) -> str:
    """Pre-registered ASYMMETRIC rule. The forward test's design value is detecting
    catastrophic divergence (REVERT), not statistically confirming a thin edge
    (CONFIRM). Hence asymmetric thresholds and sample requirements.

    Decision order (REVERT checked first because it's the higher-leverage outcome):
      1. PENDING   — neither side has reached revert_sample completed 21d obs
      2. REVERT    — edge_21 ≤ revert_bar (negative pp) AND both sides ≥ revert_sample
                     Set by economic magnitude, not SE precision.
      3. CONFIRM   — edge_21 ≥ confirm_bar (positive pp) AND both sides ≥ confirm_sample
                     AND sign positive across all horizons. Symbolic high bar.
      4. CONTINUE  — default; bundle runs live, chart adjudication backstops,
                     accumulate more data. Expected indefinitely.

    Both bars are matched to the SCORER'S pooled-cohort-means estimator (SE =
    σ_pooled × √(2/n_per_side), n in completed PICKS not nights).
    """
    n_bundle = agg[(agg.cohort == "bundle-only") & (agg.horizon_d == 21)]["n_completed"]
    n_base = agg[(agg.cohort == "baseline-only") & (agg.horizon_d == 21)]["n_completed"]
    n_bundle = int(n_bundle.iloc[0]) if len(n_bundle) else 0
    n_base = int(n_base.iloc[0]) if len(n_base) else 0

    edge_21_row = edges[edges.horizon_d == 21]["edge_pp"]
    edge_21 = float(edge_21_row.iloc[0]) if len(edge_21_row) and pd.notna(edge_21_row.iloc[0]) else float("nan")

    # 1) PENDING
    if n_bundle < revert_sample or n_base < revert_sample:
        return (f"PENDING — need {revert_sample} completed 21d obs/side to evaluate "
                f"REVERT (the disaster tripwire); have bundle-only={n_bundle}, "
                f"baseline-only={n_base}.")

    # 2) REVERT — checked first (higher-leverage outcome the test is powered for)
    if pd.notna(edge_21) and edge_21 <= revert_bar:
        return (f"REVERT — bundle 21d edge {edge_21:+.2f}pp ≤ {revert_bar:+.2f}pp "
                f"(economic-magnitude tripwire) at n_bundle={n_bundle}, "
                f"n_base={n_base}. Bundle is materially underperforming baseline on "
                f"out-of-sample divergent picks. Halt bundle, fall back to baseline, "
                f"investigate root cause before resuming.")

    # 3) CONFIRM — requires confirm_sample AND positive across all horizons
    if n_bundle >= confirm_sample and n_base >= confirm_sample:
        edges_ok = edges["edge_pp"].dropna()
        signs_consistent = (edges_ok > 0).all() and len(edges_ok) == len(HORIZONS)
        if pd.notna(edge_21) and edge_21 >= confirm_bar and signs_consistent:
            return (f"CONFIRM — bundle 21d edge {edge_21:+.2f}pp ≥ {confirm_bar:+.2f}pp "
                    f"at n_bundle={n_bundle}, n_base={n_base}; sign positive across all "
                    f"horizons. Out-of-sample edge confirms the bundle's signal-quality "
                    f"advantage. (Note: bundle was already deployed live based on in-sample "
                    f"validation — CONFIRM is corroboration, not the adoption gate.)")

    # 4) CONTINUE — default, expected indefinitely
    return (f"CONTINUE — bundle 21d edge {edge_21:+.2f}pp at n_bundle={n_bundle}, "
            f"n_base={n_base}. No tripwire. Bundle runs live, harness keeps "
            f"accumulating data. (CONFIRM bar={confirm_bar:+.2f}pp; "
            f"REVERT bar={revert_bar:+.2f}pp.)")


# --------------------------------------------------------------------------- #
def run(signals_path, as_of,
        confirm_sample, confirm_bar, revert_sample, revert_bar,
        prices=None):
    sig = pd.read_csv(signals_path, dtype={"ticker": str})
    sig["signal_date"] = pd.to_datetime(sig["signal_date"])
    as_of = pd.Timestamp(as_of)

    # Drop sentinel / non-tradeable rows (no-signal-night markers written as
    # ticker="" / label="NONE") BEFORE grouping: empty tickers read back as NaN and
    # would break the cohort classifier. Sentinels exist only to make gaps visible.
    sig = sig[sig["label"].isin(["BUY", "STRONG_BUY"])].copy()
    sig = sig[sig["ticker"].notna()
              & (sig["ticker"].astype(str).str.strip() != "")].copy()

    # Dedupe by (signal_date, engine, ticker) keeping LAST occurrence. Append-only log
    # is chronological, so last = most recent run. This makes the scorer robust to
    # manual re-triggers / retried nights that would otherwise inflate n on both sides
    # and smear the edge toward whatever that day did. Belt-and-suspenders alongside
    # post-hoc CSV cleanup. One day cannot carry > 1× weight.
    sig = sig.drop_duplicates(subset=["signal_date", "engine", "ticker"],
                              keep="last").reset_index(drop=True)

    sig = classify_divergence(sig)

    if prices is None:  # production path
        tickers = sorted(sig["ticker"].unique())
        start = (sig["signal_date"].min() - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        end = (as_of + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
        prices = fetch_prices(tickers, start, end)

    scored = score_forward_returns(sig, prices, as_of)
    agg = aggregate(scored)
    edges = edge_table(agg)
    verdict = decision(agg, edges, confirm_sample, confirm_bar, revert_sample, revert_bar)
    return scored, agg, edges, verdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", required=True, help="path to forward_signals.csv")
    ap.add_argument("--as-of", required=True, help="scoring date YYYY-MM-DD")
    # ASYMMETRIC pre-registered rule (locked by user; do not modify):
    # CONFIRM: symbolic high bar at 162 picks/side (~60 nights @ 2.7 picks/night).
    #   Bar = +4.00pp matches 2× pooled SE at n=162 (≈3.88pp), rounded up.
    #   Acknowledged likely never to fire; CONFIRM is corroboration, not adoption gate.
    # REVERT: economic-magnitude tripwire at 81 picks/side (~30 nights).
    #   Bar = -2.50pp set by "sustained underperformance worth investigating," not SE.
    #   At n=81 this is ~0.9× pooled SE — picks up regression earlier than strict 2×SE.
    ap.add_argument("--confirm-sample", type=int, default=162,
                    help="picks/side required to evaluate CONFIRM (default 162 ≈ 60 nights)")
    ap.add_argument("--confirm-bar", type=float, default=4.0,
                    help="min 21d edge in pp to CONFIRM (default +4.0pp, ≈2× pooled SE at n=162)")
    ap.add_argument("--revert-sample", type=int, default=81,
                    help="picks/side required to evaluate REVERT (default 81 ≈ 30 nights)")
    ap.add_argument("--revert-bar", type=float, default=-2.5,
                    help="max (most-negative) 21d edge in pp to trigger REVERT (default -2.5pp, "
                         "economic-magnitude tripwire, not SE-derived)")
    args = ap.parse_args()

    _, agg, edges, verdict = run(args.signals, args.as_of,
                                  args.confirm_sample, args.confirm_bar,
                                  args.revert_sample, args.revert_bar)
    print("\n=== COHORT FORWARD RETURNS ===")
    print(agg.to_string(index=False))
    print("\n=== EDGE (bundle-only − baseline-only) ===")
    print(edges.to_string(index=False))
    print(f"\n=== DECISION ===")
    print(f"  Asymmetric rule: CONFIRM ≥ +{args.confirm_bar:.2f}pp @ n≥{args.confirm_sample}/side")
    print(f"                   REVERT  ≤ {args.revert_bar:+.2f}pp @ n≥{args.revert_sample}/side")
    print(f"                   CONTINUE otherwise (default)")
    print(verdict)


if __name__ == "__main__":
    main()
