# Engineering notes

Design decisions, the reasoning behind them, how the work is tested, and what is still
wrong. The SDMX reasoning lives in [SDMX.md](SDMX.md); this is about the software.

## Shape of the solution

Six stages and a driver. Each stage is independently runnable and defines one function.

```
framework/00_bootstrap      schemas, volume, control and reference tables, seeds
framework/01_structures     load_structures(flow_id)
framework/02_ingest         ingest_flow(flow_id)
framework/03_validate       validate_flow(flow_id)
framework/04_completeness   completeness_check(flow_id)
framework/05_gold           publish_gold(flow_id), refresh_dq_overview()
run_flow                    chains all five; widgets for flow_id, dry_run, fetch
```

Onboarding a dataflow is an `INSERT` into `flow_config` followed by `run_flow`.

## Design decisions

**Rules are data, not code.** Every rule derives its parameters from `dsd_components`,
`codes`, `rule_config`, `freq_time_patterns` and `obs_status_missing`. Severity is
configurable per rule and per component without touching the validator. The cost is a layer
of indirection and a hard dependency on correct structure metadata; the benefit is that a
new dataflow needs no new code, which is the entire point.

**Bronze is all-STRING.** The raw CSV is stored faithfully, with `source_url`,
`ingested_at` and `batch_id` added and nothing else changed. Typing happens on the way into
silver, where a failure is a recorded finding rather than a load error. A pipeline that
casts on ingest has already lost the evidence it needs to explain itself.

**`dry_run=True` is the default** on every function that writes. Each returns its
DataFrames and reports its counts without touching a table. This made it possible to verify
a rebuilt gold layer against the live one before overwriting anything, and it is how all
three regression suites run.

**Writes are refused for test sources.** `validate_flow` raises if asked to write while
reading from a `source_table` override, so the seeded-error fixture can never be mistaken
for real data.

**The rule core is pure Python.** `src/sdmx_lakehouse/rules.py` holds the logic that is
worth testing in isolation — period parsing, numeric checks, mandatory-attribute severity,
duplicate detection, dedup ordering — with no Spark import. The Spark layer handles
distribution and I/O. That split is what makes 49 tests runnable in under a second on a
laptop.

**Bootstrap is idempotent.** `CREATE ... IF NOT EXISTS` for structure; reference tables are
overwritten from literals, because their contents *are* the configuration. Re-running it is
safe and is how a clean workspace is rebuilt.

**Gold is derived, never hand-maintained.** `publish_gold` rebuilds `<flow>_timeseries`,
`<flow>_series` and the `<flow>_sdmx_csv` view from silver plus the DSD each run. Earlier,
these had been built by hand at different times and had drifted into inconsistent column
orders — the two `_timeseries` tables disagreed about where dataset-level attributes go.
Deriving them removes the drift by construction.

## Testing strategy

Three layers, each answering a different question.

**Unit tests — is the logic right?** 49 pytest cases over `rules.py`, no Spark. Includes a
case per seeded error, leap-year boundaries (`2024-02-29` valid, `2023-02-29` not), the
`2024-13` shape-versus-calendar distinction, and the revision-versus-duplicate rule.

**Seeded-error fixture — does it catch what it claims?** A bronze table with exactly one
injected instance of each error type plus three clean control rows. Zero errors on real data
proves very little; the first run of this fixture caught 4 of 6 and exposed two real gaps —
a regex-only time check that accepted `2024-13`, and a `cast` that threw under Spark ANSI
mode where `try_cast` returns null. Both fixed; now 6 of 6, with the controls still clean.

That is **six of the seven rules**. `STRUCT` is not seeded: it asserts that every DSD
component has a corresponding column, which cannot be provoked by a bad row — only by a
malformed table. It fires on every live run instead.

**Regression suites — does it still reproduce known-good output?** Three suites, 47
assertions, run as Databricks jobs against live data with `dry_run=True`:

| Suite | Asserts |
|---|---|
| `_regression_v2_1` (16) | row counts and superseded counts for all three flows, 6-of-7 rules caught on the seeded fixture, 0 findings on controls, and that the rebuilt silver is row-for-row identical to each existing table |
| `_regression_gold` (26) | row counts, `STRUCTURE_ID`, row-level diff, **column order**, SDMX-CSV column list for all three flows, plus `dq_overview` contents |
| `_regression_completeness` (5) | the exact gaps found, and that they match the original implementation's output |

**One lesson worth recording.** The first gold suite passed while the column ordering was
wrong, because its comparison helper did `spark.table(t).select(*df.columns)` — it
reordered the expected result to match the candidate before comparing. A helper that
normalises the property under test cannot test it. Explicit `*_col_order` assertions now
exist for every gold object.

## Known issues

**`expected_series` conflates two datasets.** It declares `US`, `DE` and `JP` under
`flow_id = 'WS_CBPOL'`, but those describe the collection-simulation fixture. The real
`WS_CBPOL` contains `CH, GB, JP, US, XM` and no `DE`, so a completeness run against
production data reports 24 phantom gaps for a series that was never expected. The check is
correct; the configuration is wrong. It needs either a `scope` column or a distinct
`flow_id` for fixture rows — a data-modelling decision, deliberately not patched silently.

**`freq_time_patterns` is keyed on a bare frequency code.** No agency, no version, so two
versions of `CL_FREQ` with different meanings for `H` collide. See
[SDMX.md](SDMX.md#codelist-identity-is-agency--id--version--never-id). Widening the key
touches the bootstrap schema and the validator; recorded as a known limitation.

## Deliberate omissions

**The collection simulation is documented but not reproducible.** Notebook 06 describes a
non-SDMX reporter file mapped onto the DSD through a mapping table, and its results are
real — the tables exist. The code that produced the transform was lost before it was
committed. Rather than reconstruct it from its own description and present that as the
original, the notebook carries an explicit warning and the phase is marked incomplete in the
README. The `completeness_check` function was rewritten from scratch and is verified to
reproduce the original's output exactly; the reporter transform was not, because proving
the framework on a third dataflow was the better use of the remaining time.

**No orchestration.** Stages run from the driver notebook or as one-off jobs. A scheduled
job and incremental loading are listed as not done rather than sketched.

**Small slices.** One year of `WS_CBTA`, six of `WS_CBPOL`. Enough to exercise the logic,
not enough to say anything about scale.
