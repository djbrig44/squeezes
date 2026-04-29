# Airtable Write-Path Audit — Scope & Checklist

**Status:** Planning artifact. Not yet executed.
**Estimated effort:** 1–2 hours, single focused session.
**Created:** 2026-04-26 after the comprehensive weekend-email fix.
**Last schema update:** 2026-04-26 — added `Signal Type` and `Last Weekly Updated` fields (commit `da734ad`).

## Squeeze Signals schema (current)

Fields used by the audit and downstream systems:

| Field | Type | Stamped by | Notes |
|---|---|---|---|
| `Ticker` | Single line text | Both writers | Primary key (uppercase) |
| `Squeeze Status` | Single select | Weekly writer | READY / IN_SQUEEZE / FIRED_GREEN / FIRED_RED |
| `Daily Squeeze Status` | Single select | Daily writer | Same options + NONE |
| `Last Updated` | Date | Both writers | Date only — proxy for "record touched today" |
| `Last Weekly Updated` | DateTime | Weekly writer | UTC ISO timestamp; freshness window 14 days |
| `Last Daily Updated` | DateTime | Daily writer | UTC ISO timestamp; freshness window 48 hours |
| `Signal Type` | Single select | Both writers | Weekly Only / Daily Only / Both Timeframes |

## Symmetric-freshness pattern (design principle)

The fix in `da734ad` introduced **symmetric freshness checking** as a counterpart to the cross-table DELETE guard from commit `9204f23` (Bug D). Both apply when one writer's data could be misleading without context from the other writer.

Pattern shape:
- Each writer stamps its own `Last X Updated` timestamp on every write.
- Each writer reads the *other's* timestamp via a parallel `_is_X_fresh()` helper before making category claims about combined state.
- The freshness window is calibrated to the writer's cadence (48h for daily-cadence, 14d for weekly-cadence).
- Constants and helpers live in the *writer* module (`weekend_squeeze_scanner.py`); consumers import them.

Apply this pattern any time a value derived from multiple write paths needs to mean something at read-time. Without it, "both fields populated" silently degrades to "either field has been populated at some point in history" — which is the same shape of silent corruption pattern P3 catalogues at the write side.

---

## Background

Two production sessions in succession surfaced silent-data-corruption bugs in Airtable write paths:

1. **Phase 1 (short squeeze watchlist):** field-name mismatch (`Short Vol Trend` vs `Short Volume Trend`) caused writes to fail silently — success counter still incremented from request size.
2. **Weekend squeeze scanner (today, fixed in `9204f23`):** four distinct bugs in the daily/weekly Airtable sync path:
   - Update-only gate dropped 62 of 73 daily classifications per run.
   - Blanket-to-NONE logic overwrote real classifications on records not in the current scan.
   - Rendering ambiguity (NONE indistinguishable from missing).
   - Weekly DELETE swept daily-only records every Saturday, defeating the daily scan.

**Pattern:** Airtable write functions across this stack have been silently lying about their success. This audit catalogs the remaining write paths and scans each one for the same failure modes — **before** they cause an incident.

---

## Failure-Mode Patterns to Scan For

Each pattern below has been observed at least once in production. Treat any match as a high-priority finding.

### P1 — Silent-success counter
Counter increments from request batch size before the API response is checked. Failed writes get counted as successes.
```python
# BAD
update_count += len(batch)  # incremented before checking response
_process_batch(batch)

# GOOD
resp = _process_batch(batch)
update_count += resp.get("records_written", 0)
```
**Origin:** Phase 1 short-vol-trend incident.

### P2 — Update-only gate, no CREATE path
Iterates new data, skips symbols that don't already exist in the target table. New records silently dropped.
```python
# BAD
if sym not in existing:
    skip_count += 1
    continue
# only UPDATE happens, never CREATE
```
**Origin:** Bug A in today's fix (`weekend_squeeze_scanner.py:337-339`).

### P3 — Blanket-overwrite of unmatched records
After processing scan results, code iterates ALL existing records not in the current run and writes default/empty values, destroying real data captured by other code paths.
```python
# BAD
for ticker, rec in existing.items():
    if ticker not in updated_tickers:
        batch.append({"id": rec["id"], "fields": {"Status": "NONE", ...}})
```
**Origin:** Bug B in today's fix (`weekend_squeeze_scanner.py:394-418` removed).

### P4 — Cross-table / cross-operation DELETE side-effect
A scheduled operation deletes records based on local logic, but those records contain data written by a separate operation (different scanner, different timeframe, different ownership). The DELETE has no awareness of the cross-write.
```python
# BAD
stale = [rec for ticker, rec in existing.items() if ticker not in current_tickers]
# DELETEs daily-only records owned by a different scanner
```
**Origin:** Bug D in today's fix (`weekend_squeeze_scanner.py:290-306` — guarded with `_is_daily_fresh` check).

### P5 — Field-name string-literal drift
Field names are hard-coded strings that have to match the Airtable schema exactly. No verification at startup; a schema rename causes silent write failures.
```python
# BAD — typo or schema drift goes undetected
fields = {"Short Vol Trend": value}  # actual field is "Short Volume Trend"
```
**Mitigation to consider:** schema verification helper that fetches table metadata once per process, asserts every field name used by the writer exists.

### P6 — Symbol/key case sensitivity in lookup
Write side normalizes (`.upper()`) but read side or merge side does not, or vice versa. Lookups silently fail for any record where case diverges.
```python
# BAD
existing[sym] = ...           # sym is "AAPL" (uppercase)
match = existing.get(sym)     # sym is "aapl" (lowercase) — None
```
**Status:** No production incident yet, but adjacent to today's bugs. Worth grep-checking.

### P7 — Optimistic counter before write completes
Counter increments before async/queued write finishes; failures don't decrement.
**Origin:** noted in `memory/feedback_optimistic_counters.md`.

### P8 — Missing CREATE-vs-UPDATE branching
Single code path always PATCHes (assumes exists) or always POSTs (assumes new) without checking. Half the operations fail with 404 or duplicate record errors that get swallowed.

### P10 — Classifier producing systematic false negatives ("all-clear is the bug")

Detection logic that should fire on real-world events instead reports nothing, silently. The all-clear is itself the bug, but it's indistinguishable from a quiet market without external validation.

```python
# BAD — fire trigger wired to the wrong tier; suppressed by a band-aid filter.
squeeze_on = in_low_squeeze   # widest tier (1.5×), too loose for fires
squeeze_fired = squeeze_just_ended and bars_in_squeeze >= 6
if squeeze_fired:
    if bars_since_meaningful > 1:
        squeeze_fired = False  # band-aid hides the wrong-tier choice
```

**Origin:** `weekend_squeeze_scanner.py` fire-trigger wired to `in_low_squeeze` (commit `edb9d74`, Feb 2026 regression). 0 GREEN fires across 10 weeks of production runs when actual count should have been 50–150. Fixed in commit `57cac1b` (2026-04-28).

**Detection:** any classifier whose output is "absence of signal" — compare against a ground-truth source (chart inspection, alternative implementation, historical backtest) on a known-positive case. If ground truth says signal exists and code says no signal, investigate.

**Mitigation:** classifiers should produce auditable intermediate values per bar (BB/KC tier classifications, momentum, acceleration) so the difference between "no signal" and "signal but suppressed" is observable. The NVDA debug output that surfaced this bug is a model — visible per-bar tier states made the suppression instantly diagnosable.

### P9 — Stale-as-current (read-side adjacent)
Read paths present stored Airtable data as current without validating freshness via timestamp fields. Adjacent to write-path bugs but worth catching in the same audit pass.
```python
# BAD
status = record["fields"].get("Daily Squeeze Status")
if status == "READY":
    render_as_current(...)  # could be 30 days old

# GOOD
last_updated = record["fields"].get("Last Daily Updated")
if status == "READY" and _is_fresh(last_updated):
    render_as_current(...)
```
**Origin:** `send_squeeze_email._format_daily_status` before today's fix — treated any non-NONE status as current regardless of age. Now uses `_is_daily_fresh()` gate.
**Detection:** any function that reads Airtable records and uses field values without first checking when the record was last touched.

---

## Target Files

Listed by priority. Higher priority = more frequently run, more user-visible, or last touched longer ago without scrutiny.

### Squeezes repo (`~/squeezes/`)

| Priority | File | Notes |
|---|---|---|
| **P0** | `weekend_squeeze_scanner.py` | Recently fixed (`9204f23` + `da734ad`) — re-verify no regressions. P2/P3/P4 addressed in `9204f23`; P9 (stale-as-current) addressed via symmetric-freshness pattern in `da734ad`. Still check P1/P5/P6/P8 in both push functions and the new `_backfill_signal_types`. |
| **P0** | `short_squeeze_watchlist.py` | Phase 1; partially audited after the field-name incident, do full pass |
| **P0** | `short_squeeze_daily.py` | Phase 2/3 push — recently modified, has multiple write paths |
| **P1** | `short_squeeze_activations.py` | Saw activation-related Airtable writes; never deeply audited |
| **P1** | `short_squeeze_exhaustion.py` | Phase 3 exhaustion detection — same risk surface |
| **P1** | `send_squeeze_email.py` | Read paths only, but check Airtable fetch handles missing fields gracefully |

### sec-filing-processor-git (`~/sec-filing-processor-git/`)

| Priority | File | Notes |
|---|---|---|
| **P1** | `sec_processor.py` | Filing alerts to Airtable |
| **P1** | `dedupe_records.py` | DELETEs records — high blast radius if logic is wrong (P4 risk) |
| **Blocked** | `portfolio_iv_updater.py` | **Audit blocked** — no scheduled workflow exists in `sec-filing-processor-git/.github/workflows/` (confirmed 2026-04-26: only `afterhours_momentum`, `dedupe_records`, `premarket_momentum`, `sec_filing_processor` are scheduled). File is orphaned. Recommend file a `delete-or-relocate` work item BEFORE auditing. If it's still wanted, decide where it should run, then add to audit P1. |
| **P2** | `filing_router.py` | Routing logic — may not write to Airtable at all, scan first |
| **P2** | `backfill_categories.py`, `backfill_filing_family.py` | Backfill scripts — bulk writes, P1/P3 risk |

### Python Crypto Dashboard (`~/Python Crypto Dashboard/`)

| Priority | File | Notes |
|---|---|---|
| **P1** | `Airtable Push Module.py` | Central push module — if buggy, many callers affected |
| **P1** | `Daily Execution Module.py` | Daily writes — confirm via `daily_run.yml` workflow |
| **P2** | `Daily_Workflow.py`, `Daily_Trading_Workflow.py` | Workflow orchestration — likely calls Airtable Push Module |
| **P2** | `Cleanup duplicates.py`, `DuplicateDeleter.ipynb` | DELETE paths — P4 risk |
| **P3** | `small_cap_daily.yml`'s underlying script | Identify and audit; deprioritize if low-traffic |

---

## Audit Checklist (per file)

For each target file, run through this checklist and record findings.

### Reads / fetches
- [ ] Does fetch handle missing fields gracefully (`.get()` with default vs direct subscript)?
- [ ] Does fetch handle empty result sets without crashing?
- [ ] Are field names hard-coded strings that depend on Airtable schema?

### Writes (CREATE / UPDATE / PATCH)
- [ ] **P1** Does the success counter come from API response, not request batch size?
- [ ] **P2** If iterating new data: does it have BOTH update-existing AND create-new branches?
- [ ] **P3** Are there blanket-update loops that overwrite all records not in current scope?
- [ ] **P5** Are field names verified against schema at startup, or only at first write?
- [ ] **P6** Is symbol/key normalization consistent across write and read?
- [ ] **P7** Does any counter increment before the write API call returns?
- [ ] **P8** Are CREATE and UPDATE both possible code paths, or assumes one?

### Deletes
- [ ] **P4** Does the DELETE consider data ownership across multiple writers?
- [ ] Is there a guard for recently-written records (timestamp check) before deletion?
- [ ] Does DELETE log preserved-vs-deleted counts so silent over-deletion is visible?

### Error handling
- [ ] Does the function fail loudly (raise, exit non-zero) on auth errors?
- [ ] Does it log batch-level error responses with status code + body snippet?
- [ ] Does it count failures separately from successes in summary output?

### Documentation
- [ ] Does the docstring accurately describe the actual behavior (not aspirational)?
- [ ] Are field-name dependencies called out in docstrings or constants?

---

## Output Format (per file)

```markdown
### File: <path>

**Risk level:** High / Medium / Low

**Findings:**
- [Pattern code] <one-line description> — <file:line> — <impact>

**Recommended fixes:**
1. <minimal change to address each finding>

**Notes:**
- <any context worth capturing for future sessions>
```

---

## Deliverables

When the audit session executes:

1. **Per-file findings document** — append all findings to a new section in this file (`## Audit Findings — <date>`).
2. **Triaged fix list** — separate file `AIRTABLE_AUDIT_FIXES.md` with one prioritized fix list across all files.
3. **Pattern updates** — if any new failure modes are surfaced, add them to the "Failure-Mode Patterns" section above.
4. **Memory update** — if patterns generalize beyond Airtable (e.g., affect BigQuery writes too), note in the user's auto-memory.

---

## Execution Notes

- **Do not fix during the audit.** Catalog only. Fixes happen as a separate work item, prioritized by risk.
- **Run grep across all target files for each pattern** before reading individual files — pattern-first scan is faster than file-first for catching P1/P5/P6.
- **Test the audit findings** by spot-checking one or two known-good patches (Bug A/B/D fixed in `9204f23`) to verify the checklist would have caught them.
- **Time-box the audit to 2 hours.** If a single file reveals more than 5 issues, stop and triage before continuing.
