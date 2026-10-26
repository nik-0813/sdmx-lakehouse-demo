"""Unit tests for sdmx_lakehouse.rules (run with: pytest)."""
import pytest

from sdmx_lakehouse.rules import (
    check_obs_value,
    classify_mandatory_attribute,
    dedup_latest,
    find_same_batch_duplicates,
    is_valid_code,
    parse_time_period,
)

MISSING = {"H", "L", "M", "Q"}  # from the obs_status_missing table


# --- TIME_FORMAT -----------------------------------------------------------
@pytest.mark.parametrize(
    "freq,value,start",
    [
        ("A", "2024", "2024-01-01"),
        ("M", "2024-03", "2024-03-01"),
        ("Q", "2010-Q1", "2010-01-01"),
        ("Q", "2024-Q3", "2024-07-01"),
        ("D", "2024-02-29", "2024-02-29"),  # leap day
        ("H", "2024-S2", "2024-07-01"),
    ],
)
def test_time_period_valid(freq, value, start):
    status, parsed = parse_time_period(freq, value)
    assert status == "ok"
    assert parsed.isoformat() == start


@pytest.mark.parametrize(
    "freq,value",
    [
        ("M", "2024-13"),       # seeded error: month 13 matches shape, not calendar
        ("M", "2024-00"),
        ("M", "2024-3"),        # wrong shape
        ("M", "2024-03 "),      # trailing whitespace
        ("Q", "2024-Q5"),
        ("Q", "2024-03"),       # monthly value in a quarterly series
        ("D", "2023-02-29"),    # not a leap year
        ("A", "24"),
        ("M", None),
        ("M", ""),
    ],
)
def test_time_period_invalid(freq, value):
    assert parse_time_period(freq, value) == ("invalid", None)


@pytest.mark.parametrize("freq", ["B", "E", "Z", "", None])
def test_time_period_no_pattern_is_a_warning_not_a_failure(freq):
    assert parse_time_period(freq, "2024-01") == ("no_pattern", None)


def test_weekly_wellformed_is_warning_badly_shaped_is_error():
    assert parse_time_period("W", "2024-W05") == ("no_parse", None)
    assert parse_time_period("W", "2024-W5") == ("invalid", None)


# --- OBS_VALUE -------------------------------------------------------------
@pytest.mark.parametrize("value", ["4.25", "-0.5", "0", "1e3", ".5", "7."])
def test_obs_value_numeric(value):
    status, number = check_obs_value(value, "A", MISSING)
    assert status == "ok"
    assert number == float(value)


@pytest.mark.parametrize("value", ["N/A", "abc", "1,5", "NaN", "inf", "1_0", " 1"])
def test_obs_value_non_numeric_is_error(value):  # seeded error: 'N/A'
    assert check_obs_value(value, "A", MISSING) == ("error", None)


@pytest.mark.parametrize("value", [None, ""])
def test_obs_value_empty_with_missing_status_is_ok(value):
    assert check_obs_value(value, "M", MISSING) == ("missing_ok", None)


@pytest.mark.parametrize("value", [None, ""])
def test_obs_value_empty_without_missing_status_is_error(value):
    assert check_obs_value(value, "A", MISSING) == ("error", None)
    assert check_obs_value(value, None, MISSING) == ("error", None)


# --- DIM_CODE / ATTR_CODE ---------------------------------------------------
def test_codes():
    areas = {"US", "JP", "XM"}
    assert is_valid_code("US", areas)
    assert not is_valid_code("ZZ", areas)      # seeded error: invalid REF_AREA
    assert not is_valid_code("us", areas)      # case-sensitive
    assert not is_valid_code(None, areas)
    assert not is_valid_code("X", {"A", "B"})  # seeded error: invalid OBS_STATUS


# --- ATTR_MANDATORY --------------------------------------------------------
@pytest.mark.parametrize(
    "nulls,total,expected",
    [
        (0, 360, "ok"),
        (360, 360, "warning"),  # TIME_FORMAT: source never populates it
        (1, 360, "error"),      # seeded error: DECIMALS missing in some rows
        (0, 0, "ok"),
    ],
)
def test_mandatory_attribute_severity(nulls, total, expected):
    assert classify_mandatory_attribute(nulls, total) == expected


def test_mandatory_attribute_rejects_impossible_counts():
    with pytest.raises(ValueError):
        classify_mandatory_attribute(5, 3)


# --- DUPLICATE and dedup ---------------------------------------------------
KEY = ["FREQ", "REF_AREA", "TIME_PERIOD"]


def row(batch, ts, area="US", period="2024-01", value="1"):
    return {
        "batch_id": batch, "ingested_at": ts, "FREQ": "M",
        "REF_AREA": area, "TIME_PERIOD": period, "OBS_VALUE": value,
    }


def test_same_batch_duplicate_is_flagged():  # seeded error: duplicate key+period
    rows = [row("b1", 1), row("b1", 1)]
    assert find_same_batch_duplicates(rows, KEY) == [("b1", "M", "US", "2024-01")]


def test_cross_batch_repeat_is_a_revision_not_a_duplicate():
    rows = [row("b1", 1, value="1"), row("b2", 2, value="2")]
    assert find_same_batch_duplicates(rows, KEY) == []
    kept, superseded = dedup_latest(rows, KEY)
    assert superseded == 1
    assert kept[0]["OBS_VALUE"] == "2"  # latest ingested_at wins


def test_dedup_counts_match_phase_i():
    """Shape of the CBPOL run: an early load of US+JP is re-delivered by a
    later, wider load, so the early rows are superseded."""
    old = [row("b1", 1, area=a, period=f"2024-{m:02d}")
           for a in ("US", "JP") for m in range(1, 13)]  # 24 rows
    new = [row("b2", 2, area=a, period=f"2024-{m:02d}")
           for a in ("US", "JP", "XM") for m in range(1, 13)]  # 36 rows
    kept, superseded = dedup_latest(old + new, KEY)
    assert superseded == 24
    assert len(kept) == 36
    assert all(r["batch_id"] == "b2" for r in kept)


def test_dedup_preserves_order_and_distinct_keys():
    rows = [row("b1", 1, period="2024-02"), row("b1", 1, period="2024-01")]
    kept, superseded = dedup_latest(rows, KEY)
    assert superseded == 0
    assert [r["TIME_PERIOD"] for r in kept] == ["2024-02", "2024-01"]