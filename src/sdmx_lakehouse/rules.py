"""Reference implementations of the SDMX validation rules.

These pure-Python functions are the *specification* of each rule used by the
metadata-driven validator (validate_flow). They have no Spark dependency, so
they can be unit-tested anywhere (laptop, CI, Databricks notebook).

Nothing here is flow-specific: valid codes, mandatory attributes and the
series key come from the DSD / codelist metadata and are passed in.

Independent personal project, not affiliated with the BIS.
"""
from __future__ import annotations

import re
from collections import Counter
from datetime import date
from typing import Any, Iterable, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# TIME_PERIOD (rule: TIME_FORMAT)
# Reference data from SDMX conventions, NOT derived from the DSD.
# ---------------------------------------------------------------------------
TIME_PATTERNS = {
    "A": re.compile(r"\d{4}", re.ASCII),                 # 2024
    "H": re.compile(r"\d{4}-S[12]", re.ASCII),           # 2024-S1
    "Q": re.compile(r"\d{4}-Q[1-4]", re.ASCII),          # 2024-Q3
    "M": re.compile(r"\d{4}-\d{2}", re.ASCII),           # 2024-03
    "W": re.compile(r"\d{4}-W\d{2}", re.ASCII),          # 2024-W05 (shape only)
    "D": re.compile(r"\d{4}-\d{2}-\d{2}", re.ASCII),     # 2024-03-15
}

# Frequencies whose pattern was confirmed against real BIS data.
# H follows SDMX conventions but is unverified against BIS data.
# W has a pattern but no parse expression yet: a well-formed value returns
# "no_parse" (a warning), a badly shaped one "invalid" (an error), matching
# the notebook validator.
VERIFIED_FREQS = frozenset({"A", "M", "Q", "D"})


def parse_time_period(freq: Optional[str], value: Optional[str]):
    """Check a TIME_PERIOD against the pattern implied by FREQ.

    Returns (status, period_start_date) where status is one of:
      "ok"          - matches the pattern and is a real calendar period
      "invalid"     - wrong shape, or impossible (e.g. month 13)  -> error
      "no_pattern"  - no pattern defined for this FREQ            -> warning
      "no_parse"    - right shape, but no date parse defined (W)   -> warning
    """
    pattern = TIME_PATTERNS.get(freq) if freq is not None else None
    if pattern is None:
        return "no_pattern", None
    if value is None or not pattern.fullmatch(value):
        return "invalid", None
    try:
        if freq == "A":
            start = date(int(value), 1, 1)
        elif freq == "H":
            start = date(int(value[:4]), 1 if value[-1] == "1" else 7, 1)
        elif freq == "Q":
            start = date(int(value[:4]), 3 * (int(value[-1]) - 1) + 1, 1)
        elif freq == "M":
            start = date(int(value[:4]), int(value[5:7]), 1)
        elif freq == "W":
            return "no_parse", None
        else:  # "D"
            start = date(int(value[:4]), int(value[5:7]), int(value[8:10]))
    except ValueError:
        return "invalid", None
    return "ok", start


# ---------------------------------------------------------------------------
# OBS_VALUE (rule: OBS_VALUE)
# ---------------------------------------------------------------------------
_NUMERIC = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?", re.ASCII)


def check_obs_value(
    value: Optional[str],
    obs_status: Optional[str],
    missing_status_codes: Iterable[str],
):
    """Check OBS_VALUE: numeric, or empty with a "missing" OBS_STATUS.

    missing_status_codes comes from the explicit obs_status_missing table
    (derived from the OBS_STATUS codelist), not from string matching.

    Returns (status, number):
      "ok"         - numeric value
      "missing_ok" - empty value with a missing-type OBS_STATUS
      "error"      - non-numeric text, or empty value without a missing status
    """
    if value is None or value == "":
        if obs_status in set(missing_status_codes):
            return "missing_ok", None
        return "error", None
    if _NUMERIC.fullmatch(value):
        return "ok", float(value)
    return "error", None


# ---------------------------------------------------------------------------
# Codes (rules: DIM_CODE, ATTR_CODE)
# ---------------------------------------------------------------------------
def is_valid_code(value: Optional[str], valid_codes: Iterable[str]) -> bool:
    """True if value is a code in the codelist (exact, case-sensitive match)."""
    return value is not None and value in set(valid_codes)


# ---------------------------------------------------------------------------
# Mandatory attributes (rule: ATTR_MANDATORY)
# ---------------------------------------------------------------------------
def classify_mandatory_attribute(null_count: int, total_count: int) -> str:
    """Severity for a mandatory attribute across one batch.

    "ok"      - never missing
    "warning" - missing in 100% of the batch: the source never populates it
    "error"   - missing in some rows: a genuine data problem
    """
    if null_count < 0 or null_count > total_count:
        raise ValueError("null_count must be between 0 and total_count")
    if total_count == 0 or null_count == 0:
        return "ok"
    if null_count == total_count:
        return "warning"
    return "error"


# ---------------------------------------------------------------------------
# Duplicates and revisions (rule: DUPLICATE, plus cross-batch dedup)
# ---------------------------------------------------------------------------
def find_same_batch_duplicates(
    rows: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
    batch_field: str = "batch_id",
):
    """Return keys (batch, *key values) seen more than once in the same batch.

    key_fields = all dimensions plus TIME_PERIOD. This is an ERROR: the same
    observation was delivered twice in one load. Run it BEFORE dedup_latest.
    """
    counts = Counter(
        (row[batch_field], *(row[f] for f in key_fields)) for row in rows
    )
    return [key for key, n in counts.items() if n > 1]


def dedup_latest(
    rows: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
    ts_field: str = "ingested_at",
):
    """Latest ingested_at wins per key. Returns (kept_rows, superseded_count).

    Repeats across different batches are revisions, not failures. Ties on
    ingested_at keep the later row. Original row order is preserved.
    """
    best = {}
    for i, row in enumerate(rows):
        key = tuple(row[f] for f in key_fields)
        rank = (row[ts_field], i)
        if key not in best or rank > best[key][0]:
            best[key] = (rank, i, row)
    kept = [row for _, _, row in sorted(best.values(), key=lambda t: t[1])]
    return kept, len(rows) - len(kept)