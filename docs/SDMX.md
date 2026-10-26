# SDMX notes

What this project demonstrates about SDMX, and where each concept lives in the code. The
non-obvious points are the ones that cost me something to learn.

## The information model, as implemented

| Concept | Where it appears |
|---|---|
| Dataflow | `flow_config` — one row per dataflow is the entire onboarding step |
| Data Structure Definition | `dsd_components` — one row per component, with type, ordinal, attachment level and assignment status |
| Dimensions, time dimension, measure, attributes | `component_type` on `dsd_components`; everything downstream reads this rather than naming columns |
| Codelists and codes | `codelists`, `codes`, keyed on `agency + id + version` |
| Series key | the dimension columns in ordinal order; `dim_cols + [time_col]` identifies an observation |
| `OBS_STATUS` and missing-value codes | `obs_status_missing`, derived once from `CL_OBS_STATUS` |
| Attachment level | drives attribute ordering in the gold layer |
| SDMX-CSV | `<flow>_sdmx_csv` views |

## Codelist identity is `agency + id + version` — never `id`

This bit me twice, in two different places.

**Once in a join.** `dsd_components → codelists → codes` originally matched on
`agency + codelist_id`, omitting `version`. Separately, a codelist shared by two components
duplicated its codes once per component. `CL_BIS_UNIT` is used by both `UNIT_TYPE` and
`UNIT_MEASURE` in `BIS_TOTAL_CREDIT`, so it reported 2,210 codes instead of 1,105. The fix
was `SELECT DISTINCT` on the codelist reference plus `version` in every join.

**Once in reference data.** The `CL_FREQ` version used by `CBTA` defines `H = Hourly` and
`S = Half-yearly`. The version used by `BIS_CBPOL` is the one behind the
`freq_time_patterns` table, where `H` means half-yearly. Same codelist id, incompatible
meanings. `freq_time_patterns` is keyed on the bare frequency code with no agency or
version — so it carries exactly the bug the join was fixed to avoid. Harmless today, wrong
for the first flow that uses either code. Recorded as an open issue rather than patched.

The lesson generalises: a lookup table keyed on a code alone is a version collision waiting
to happen.

## Attachment levels decide attribute order

An SDMX attribute attaches to the dataset, a series, a group, or a single observation.
That is not just documentation — it determines the order attributes appear in SDMX-CSV
output, and it is why the BIS API's `sdmx-csv` format differs from its plain `csv` format.

Sorting by **attachment level, then DSD ordinal** reproduces the BIS ordering exactly:

| Flow | Attribute order |
|---|---|
| `WS_CBPOL` | `UNIT_MEASURE, UNIT_MULT` (dataset) · `TIME_FORMAT, COMPILATION, DECIMALS, SOURCE_REF, SUPP_INFO_BREAKS, TITLE` (series) · `OBS_STATUS, OBS_CONF, OBS_PRE_BREAK` (observation) |
| `WS_TC` | `COLLECTION` (dataset) · `UNIT_MULT, UNIT_MEASURE, TITLE_TS` (series) · `DECIMALS` (group) · `OBS_STATUS, OBS_PRE_BREAK, OBS_CONF` (observation) |

In `BIS_TOTAL_CREDIT`, `TITLE_TS` has ordinal 15 but sorts fourth because it is
series-level, and `DECIMALS` has ordinal 11 but sorts after it because it is group-level.
I initially tried DSD ordinal, then silver column order, then source CSV order — all three
were wrong. Only the attachment hierarchy reproduced the published output.

The payoff came later: `WS_CBTA`, a six-dimension DSD the code had never seen, produced
`COMMENT_DSET` (dataset) → fourteen series-level attributes → `FISCAL_YEAR, CONF_STATUS,
OBS_STATUS, OBS_PRE_BREAK` (observation), correctly, with no intervention.

## Series keys, including partial keys

The REST data endpoint takes a dot-separated key in dimension order:

```
/data/dataflow/{agency}/{flow}/{version}/{key}?format=csv&startPeriod=&endPeriod=
```

An empty position means *all values for that dimension*. `WS_CBTA` has six dimensions
(`FREQ.REF_AREA.COMP_METHOD.UNIT_MEASURE.CURRENCY.TRANSFORMATION`) and was ingested with:

```
M.US+XM+JP....
```

— monthly, three reference areas, and every combination of the remaining four. The API
returned 8 series. `+` within a position is an OR.

## A valid code does not guarantee a series

`DE` is a perfectly valid `CL_AREA` code, but Germany has no individual policy-rate series
in `WS_CBPOL` — the euro area aggregate `XM` applies. Code validity and data availability
are different questions, and the codelist answers only the first.

## Mandatory does not mean populated

`TIME_FORMAT` is declared Mandatory in `BIS_CBPOL` and is empty in **every** row the API
returns, in both `csv` and `sdmx-csv` formats. Rejecting all 360 rows would be
technically defensible and practically useless.

So `ATTR_MANDATORY` distinguishes two cases: a mandatory attribute missing in *some* rows
is a row-level **error**, worth rejecting; missing in *100%* of a batch is a single
batch-level **warning** about the source, not a verdict on the data. The distinction is
the difference between a validator people use and one they switch off.

## Syntactically valid is not semantically valid

`2024-13` matches the monthly pattern `^\d{4}-\d{2}$` and is not a month. A regex check
alone passes it. `TIME_FORMAT` therefore does two things: match the shape implied by the
row's `FREQ`, then parse it as a real calendar period. `2024-13`, `2024-00` and
`2023-02-29` all fail the second step.

Frequency rules live in `freq_time_patterns` as data — a regex and a parse expression per
frequency — not in code. A frequency with no parse expression raises a warning rather than
silently reporting success; `W` is currently in that state.

## Revisions are not duplicates

The same series key and period can legitimately arrive again — that is a revision. The
same key and period arriving **twice in one delivery** is an error.

The validator separates them: `DUPLICATE` looks only within a `batch_id`, while across
batches the latest `ingested_at` wins and the superseded rows are counted and reported.
`WS_CBPOL` carries 408 bronze rows that resolve to 360 observations with 48 superseded —
the kind of number that should be visible, not silently dropped.

## `OBS_STATUS` and absent values

An empty `OBS_VALUE` is acceptable only when `OBS_STATUS` explains it. The acceptable codes
are not hard-coded — they are derived from `CL_OBS_STATUS` into `obs_status_missing`:
`H` (holiday or weekend), `L` (not collected), `M` (cannot exist), `Q` (suppressed). An
empty value with `OBS_STATUS = 'A'` (normal) is an error, because *normal* and *absent* are
contradictory claims.

## SDMX-CSV output

The dissemination views emit the standard header columns ahead of the data:

```sql
SELECT 'dataflow' AS STRUCTURE, 'BIS:WS_CBPOL(1.0)' AS STRUCTURE_ID, 'I' AS ACTION, ...
```

`STRUCTURE_ID` is `agency:flow_id(version)`, read from `flow_config`. `ACTION = 'I'`
denotes an informational/full replace. Column order after the header is dimensions, time,
measure, then attributes in attachment order. The output was compared against the BIS API's
own `sdmx-csv` for the same key: `WS_CBPOL` matched on all 360 rows and 5,400 values.

That comparison was a **one-time manual check**, recorded in `notebooks/99_Decision_Log`
with its methodology. Unlike the row counts and column orders, it is not re-run by the
regression suites.

## BIS API specifics

- **Structure resources are namespaced.** The standard top-level `/dataflow`,
  `/datastructure` and `/codelist` resources return HTTP 501. Structures are served under
  `/structure/{type}/{agency}/{id}/{version}`.
- **Two CSV flavours.** `format=csv` and `format=sdmx-csv` differ in attribute ordering —
  see the attachment-level section above.
- **DSD JSON shape.** `dimensionList`, `measureList` (containing `primaryMeasure`) and
  `attributeList` (where each attribute carries its `assignmentStatus` and
  `attachmentLevel`).

## Not covered

SDMX 3.0 differences. Structure-set and mapping-set resources. Dataset-level attributes are
read from the DSD but not checked per row. Registry interactions and the SDMX-ML formats —
this project works in SDMX-CSV and the JSON structure messages only.
