# Scanner Fire-Detection Fix Plan

**Status:** SHIPPED in commit `57cac1b` (2026-04-28).
**Target file:** `weekend_squeeze_scanner.py` (single file, fire-detection logic only).
**Created:** 2026-04-28 after diagnostic confirmed systemic miss.

## Outcome

Fix shipped same session. Manual saturday-scan trigger post-deploy produced
1 GREEN fire on partial-week data — first non-zero fire count in production
in 10+ weeks. Additional refinement beyond the original plan: added
`MIN_FIRE_MOMENTUM = 1.0` threshold + rising-momentum filter (current_mom and
mom_accel must share sign) after backtest spot-checks revealed EVEX-style
near-zero false positives (+0.03 momentum) and late-cycle peaked-momentum
edge cases. Backtest validation: 65.2% / 72.0% 5-week post-fire win rates.

The naming-cleanup deferral noted below stands — naming pass remains a future
v2 concern.

---

## Empirical evidence

### NVDA reproduction (`python3 weekend_squeeze_scanner.py -s NVDA --debug`)

```
Bar dates:                ['2026-02-27','03-06','03-13','03-20','03-27','04-03','04-10','04-17','04-24','05-01']
Low squeeze (1.5× tier):  [T, T, T, T, T, T, T, T, T, F]  ← ends 2026-05-01
Mid+High squeeze (1.0/1.2): [T, T, T, T, T, T, T, T, F, F]  ← ends 2026-04-24

squeeze_fired:      False  ← BUG. Should be True with fire on 2026-04-24 bar.
prev_squeeze:       True
bars_in_squeeze:    25
Squeeze state:      NONE
Momentum:           +15.57 (rising, positive — would have been GREEN fire)
```

**Interpretation:** Mid+High squeeze ended on 2026-04-24 — that bar should have been the GREEN fire. Scanner waited for the loose 1.5× tier to also end (2026-05-01). By then, the freshness check sees mid+high already ended a bar ago and suppresses the fire as stale.

### Lifetime fire count across scheduled Saturday runs

| Date | GREEN | RED | Ready | InSqueeze |
|---|---|---|---|---|
| 2026-04-25 | 0 | 0 | 41 | 26 |
| 2026-04-18 | 0 | 0 | 39 | 27 |
| 2026-04-11 | 0 | 0 | 33 | 33 |
| 2026-04-04 | 0 | 0 | 31 | 28 |
| 2026-03-28 | 0 | 0 | 29 | 31 |
| 2026-03-21 | 0 | 0 | 32 | 33 |
| 2026-03-14 | 0 | **1** | 31 | 30 |
| 2026-03-07 | 0 | 0 | 28 | 36 |
| 2026-02-28 | 0 | 0 | 18 | 43 |
| 2026-02-21 | 0 | 0 | 15 | 38 |
| **TOTAL (10 weeks)** | **0** | **1** | ~297 | ~325 |

Expected baseline for a healthy fire detector across 1,177 symbols × 10 weekends: **50–150 GREEN fires**. Actual: **0**. Estimated true catch rate: **<1%**.

The single RED fire on 2026-03-14 is a statistical anomaly — it slipped through a rare alignment of conditions where the loose tier and freshness check both happened to permit it.

---

## Root cause — code references

`weekend_squeeze_scanner.py`:

- **Line 937**: `squeeze_on = in_low_squeeze`
  - Wires the fire-trigger variable to the WIDEST KC tier (1.5× ATR).
  - Comment on line 962 incorrectly claims "match TOS" — ToS TTMSqzPro fires on the TIGHTEST tier (1.0× ATR), not the loosest.
- **Line 985**: `squeeze_fired = squeeze_just_ended and bars_in_squeeze >= 6`
  - Fires when the 1.5× tier exits.
  - 6-bar threshold is calibrated against this loose tier.
- **Lines 990–997**: freshness check
  - Suppresses the fire if mid+high (1.0/1.2× tiers) already exited 2+ bars ago.
  - Acts as a band-aid — partially compensates for the wrong-tier trigger but kills real fires when tight tiers resolve before the loose tier does (which is the normal pattern).
- **Lines 928–934**: tier classification + `meaningful_squeeze` definition
  - `meaningful_squeeze = squeeze_high | squeeze_mid` already exists in the code.
  - Comment: "MEANINGFUL squeeze = Mid + High only (ignore Low like TOS)".
  - The author KNEW the right logic — the variable is defined and named correctly. It's just not used by the fire trigger.

### Git archaeology

| Commit | Date | Behavior |
|---|---|---|
| `d212a0b` | Feb 2026 | Fired on `meaningful_squeeze_ended` — **CORRECT** |
| `edb9d74` | Feb 2026 | Reverted to `in_low_squeeze` — **REGRESSION** |
| `5804785` | Feb 14 | Added freshness check — **BAND-AID** |

The fix existed and was working. It was reverted, then a band-aid was added that compounds the problem. Net effect: 10 weeks of <1% catch rate.

---

## Fix design

### Logic change (the core fix)

Change line 937 (and update the comment):

```python
# BEFORE
# Legacy squeeze_on for compatibility (any squeeze)
squeeze_on = in_low_squeeze

# AFTER
# Fire-trigger variable: matches ToS TTMSqzPro semantics (fire when
# tightest compression resolves, not when loose compression resolves).
squeeze_on = meaningful_squeeze
```

This is the entire correctness fix. Everything else in the function — `squeeze_just_ended`, `squeeze_fired`, `bars_in_squeeze`, fire direction, ready/in_squeeze classification — already uses `squeeze_on` and will inherit the corrected semantics.

### Freshness check — REMOVE entirely

Lines 990–997 should be deleted. The check exists only to suppress false positives that the wrong-tier trigger introduces. Once the trigger uses the correct tier, the freshness check is at best redundant and at worst harmful (it kills legitimate fires where the loose tier hasn't yet resolved).

Verification: the check looks at how many bars ago `meaningful_squeeze` ended. After the fix, `squeeze_on = meaningful_squeeze`, so `squeeze_just_ended` already encodes "meaningful squeeze ended this bar." The check becomes tautological.

### Threshold recalibration — KEEP at 6 bars, but document why

Current: `bars_in_squeeze >= 6`, counted using the loose 1.5× tier.

After fix: same threshold, but counted using the meaningful tier (1.0× + 1.2×).

**Two paths considered:**

1. **Keep 6 bars in meaningful tier** — stricter, fewer fires. 6 weeks of mid-or-tight compression is a strong signal. Reasonable starting point for v1.
2. **Lower to 4 bars in meaningful tier** — more fires, including shorter compressions. 4 weeks of tight compression resolving is still a meaningful event.

**Recommendation: keep 6 bars for v1.** Reasons:
- Conservative on first deploy — better to under-fire than flood the email
- Easy to lower later after observing actual fire frequency
- 6 weeks of mid-or-tight compression is the kind of setup the user wants to act on
- Lowering to 4 introduces noise that needs separate calibration

If post-deploy the email consistently shows 0–1 fires per week, drop to 4 bars in v2. If it shows 15+ per week (overwhelming), raise to 8.

### Naming cleanup — DEFER to v2

The `in_low_squeeze` (1.5× = widest, "low compression") naming is genuinely confusing. The natural reading is "low squeeze = small squeeze," but it actually means "loose squeeze." This naming contributed to the regression — `edb9d74` likely reverted the fix because the variable name made the wrong assignment look right.

Suggested renames (NOT in v1):
- `in_low_squeeze` → `in_loose_squeeze` (1.5× ATR)
- `in_mid_squeeze` → `in_mid_squeeze` (unchanged, 1.2× ATR)
- `in_high_squeeze` → `in_tight_squeeze` (1.0× ATR)

**Defer rationale:** v1 fix is one line + delete one block. Mixing in a naming pass triples the diff size and risks introducing other bugs. Ship the correctness fix first, validate, then naming cleanup as a separate commit.

---

## Validation plan

### Pre-deploy validation

1. Run NVDA reproduction with the fix applied locally:
   ```
   python3 weekend_squeeze_scanner.py -s NVDA --debug
   ```
   Expected: `squeeze_fired: True`, `fire_direction: 'GREEN'`, fire bar = 2026-04-24.

2. Run a small spot-check set:
   ```
   python3 weekend_squeeze_scanner.py -s NVDA AAPL MSFT TSLA AMZN GOOGL META AVGO
   ```
   For each, compare scanner output to ToS visual inspection. If the user can confirm 5+ matches and no false positives, fix is validated.

3. Full universe dry-run (no Airtable write):
   ```
   python3 weekend_squeeze_scanner.py --no-airtable
   ```
   Count GREEN/RED fires. Expected: 5–15 GREEN fires (from ~3 months of accumulated compression resolutions becoming visible the first time the corrected logic runs).

### Post-deploy validation

1. **Manual workflow trigger** before letting Saturday auto-run:
   ```
   gh workflow run saturday-scan.yml
   ```
   Inspect the email it sends. Cross-reference 3–5 fires against ToS.

2. **Forward observation** — next 3 Saturday emails should show 5–15 GREEN fires/week (vs. 0 prior). If consistently 0 or consistently 50+, something is still wrong.

3. **Backfill is NOT required.** Unlike the Airtable bugs, this isn't data corruption — historical fires that were missed simply weren't surfaced in their respective weekend emails. The opportunity is gone (those trades have already played out). Going forward is what matters.

---

## Rollout safety

**Recommendation: ship with a manual workflow trigger first, then let Saturday auto-run.**

Rationale:
- The fix will produce a dramatic visible change (0 → 5–15 fires in one weekend).
- Manual trigger lets you spot-check 3–5 names against ToS before committing the full email to your inbox.
- Cost is ~5 minutes of validation; benefit is catching any unexpected fix-induced regression before it lands as the official Saturday email.

**Do NOT** run the new scanner alongside the old for parallel comparison. The old scanner is so broken (<1% catch rate) that "agreement" between them tells us nothing useful, and the comparison overhead isn't worth the additional confidence.

---

## Documentation updates required after fix lands

### `AIRTABLE_AUDIT.md` — add new pattern

The audit doc currently catalogs 9 write-path patterns. This bug is a **read-side / classifier pattern** that wasn't on the list.

Add as **P10**:

> **P10 — Classifier producing systematic false negatives that look like "no signal"**
>
> Detection logic that should fire on real-world events instead reports "all clear" silently. The all-clear is itself the bug, but it's indistinguishable from a quiet market without external validation.
>
> Origin: `weekend_squeeze_scanner.py` fire-trigger wired to wrong KC tier (commit `edb9d74` regression). 10 weeks of "0 GREEN fires" emails when actual count should have been 50–150.
>
> Detection: any classifier whose output is "absence of signal" — compare against a ground-truth source (chart inspection, alternative implementation) on a known-positive case. If ground truth says signal exists and code says no signal, investigate.
>
> Mitigation: classifiers should produce auditable intermediate values (the NVDA debug output is a model — every tier classification visible per bar) so the difference between "no signal" and "signal but suppressed" is observable.

### Meta-pattern note

This is the 4th silent-success bug in this codebase across two sessions:

| # | Bug | What looked successful but wasn't |
|---|---|---|
| 1 | Phase 1 field-name mismatch | Counter incremented, no actual write |
| 2 | Bug A (update-only gate) | Skipped writes counted as "complete" |
| 3 | Bug B (blanket-NONE) | Overwrote real data with defaults |
| 4 | Fire-detection regression | "0 fires" reported when fires existed |

**Unifying theme:** systems that report success or "all clear" when they should report nothing or failure. Worth documenting as a meta-pattern in `AIRTABLE_AUDIT.md` (or splitting into a broader `SILENT_SUCCESS_PATTERNS.md` if the catalog grows).

### Memory note

Save to user memory:

> **Scanner fire-detection regression (closed 2026-04-28):** `weekend_squeeze_scanner.py` fire trigger was wired to 1.5× KC tier instead of meaningful (1.0/1.2×) tier from commit `edb9d74` (Feb 2026) onward, producing <1% catch rate. Fixed by changing `squeeze_on = in_low_squeeze` to `squeeze_on = meaningful_squeeze` and removing the freshness check band-aid (lines 990-997). NVDA reproduction (Apr 2026 fire) is the canonical regression test.

---

## Estimated next-session effort

| Phase | Task | Time |
|---|---|---|
| 1 | Apply 1-line fix + delete freshness check (~10 lines deleted) | 5 min |
| 2 | Run NVDA reproduction locally + spot-check 7 mega-caps | 10 min |
| 3 | Full-universe dry-run + count fires | 10 min |
| 4 | Commit + push (no Airtable changes; this is pure logic fix) | 5 min |
| 5 | Manual workflow trigger + email spot-check | 10 min |
| 6 | Update AIRTABLE_AUDIT.md with P10 pattern | 10 min |
| 7 | Save memory note | 5 min |
| **Total** | | **~55 min** |

This is a ~1-hour focused next session. No tired-night coding required.

---

## Open questions for the user (resolve before next session)

1. **Threshold for v1: 6 bars confirmed?** Or do you want to start at 4 bars (more responsive, more fires)?
2. **Naming cleanup: separate commit or skip indefinitely?** v1 plan defers it. Confirm or override.
3. **Rollout: manual trigger first, or just ship and trust validation?** v1 plan recommends manual trigger; quick to do but optional.
4. **P10 documentation: append to AIRTABLE_AUDIT.md, or new SILENT_SUCCESS_PATTERNS.md?** v1 plan appends; switching later is cheap.
