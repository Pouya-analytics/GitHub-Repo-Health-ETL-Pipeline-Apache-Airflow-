"""
test_etl_logic.py
Unit tests for the validate / transform / load / quality-check stages,
using static fixtures (tests/fixtures.py) rather than live network calls.

Run with: python -m pytest tests/test_etl_logic.py -v
"""
import os
import sys
import sqlite3
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from etl_logic import (
    validate_record, validate_batch, ValidationError,
    transform_record, transform_batch,
    load_batch, get_connection, run_data_quality_checks,
)
from fixtures import (
    VALID_REPO_RESPONSE, VALID_REPO_RESPONSE_2,
    MISSING_FIELD_RESPONSE, NEGATIVE_COUNT_RESPONSE, UNPARSEABLE_DATE_RESPONSE,
)


# ---------------------------------------------------------------------
# VALIDATE
# ---------------------------------------------------------------------
def test_validate_record_accepts_valid_response():
    validate_record(VALID_REPO_RESPONSE)  # should not raise


def test_validate_record_rejects_missing_field():
    with pytest.raises(ValidationError, match="Missing required field"):
        validate_record(MISSING_FIELD_RESPONSE)


def test_validate_record_rejects_negative_count():
    with pytest.raises(ValidationError, match="Negative count"):
        validate_record(NEGATIVE_COUNT_RESPONSE)


def test_validate_record_rejects_unparseable_date():
    with pytest.raises(ValidationError, match="Unparseable created_at"):
        validate_record(UNPARSEABLE_DATE_RESPONSE)


def test_validate_batch_filters_bad_records_below_threshold():
    batch = [VALID_REPO_RESPONSE, VALID_REPO_RESPONSE_2, MISSING_FIELD_RESPONSE]
    valid = validate_batch(batch, max_failure_rate=0.5)
    assert len(valid) == 2
    assert all(r["full_name"] in ("apache/airflow", "duckdb/duckdb") for r in valid)


def test_validate_batch_raises_when_failure_rate_too_high():
    batch = [MISSING_FIELD_RESPONSE, NEGATIVE_COUNT_RESPONSE, UNPARSEABLE_DATE_RESPONSE, VALID_REPO_RESPONSE]
    with pytest.raises(ValidationError, match="failure rate"):
        validate_batch(batch, max_failure_rate=0.3)


# ---------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------
def test_transform_record_computes_derived_metrics():
    snap = transform_record(VALID_REPO_RESPONSE, snapshot_date="2026-06-26")
    assert snap.repo == "apache/airflow"
    assert snap.stars == 45939
    assert snap.days_since_creation > 0
    assert snap.stars_per_day > 0
    assert snap.issue_to_star_ratio == round(1715 / 45939, 5)


def test_transform_record_handles_zero_stars_without_division_error():
    zero_star = dict(VALID_REPO_RESPONSE, stargazers_count=0)
    snap = transform_record(zero_star, snapshot_date="2026-06-26")
    assert snap.issue_to_star_ratio == 0.0


def test_transform_batch_processes_multiple_records():
    snaps = transform_batch([VALID_REPO_RESPONSE, VALID_REPO_RESPONSE_2])
    assert len(snaps) == 2
    assert {s.repo for s in snaps} == {"apache/airflow", "duckdb/duckdb"}


# ---------------------------------------------------------------------
# LOAD (uses a real temporary SQLite file, not a live network call)
# ---------------------------------------------------------------------
@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.remove(path)


def test_load_batch_inserts_rows(temp_db):
    snaps = transform_batch([VALID_REPO_RESPONSE, VALID_REPO_RESPONSE_2])
    n = load_batch(snaps, temp_db)
    assert n == 2

    conn = sqlite3.connect(temp_db)
    count = conn.execute("SELECT COUNT(*) FROM repo_snapshots").fetchone()[0]
    conn.close()
    assert count == 2


def test_load_batch_is_idempotent_on_rerun(temp_db):
    """Running the same load twice for the same day should UPDATE, not
    duplicate -- this is what makes Airflow task retries safe."""
    snaps = transform_batch([VALID_REPO_RESPONSE])
    load_batch(snaps, temp_db)
    load_batch(snaps, temp_db)  # simulate an Airflow retry

    conn = sqlite3.connect(temp_db)
    count = conn.execute("SELECT COUNT(*) FROM repo_snapshots").fetchone()[0]
    conn.close()
    assert count == 1  # not 2


def test_data_quality_check_passes_on_healthy_load(temp_db):
    snaps = transform_batch([VALID_REPO_RESPONSE, VALID_REPO_RESPONSE_2])
    load_batch(snaps, temp_db)
    result = run_data_quality_checks(temp_db, expected_repo_count=2)
    assert result["passed"] is True
    assert result["rows_loaded_today"] == 2


def test_data_quality_check_fails_on_insufficient_rows(temp_db):
    snaps = transform_batch([VALID_REPO_RESPONSE])
    load_batch(snaps, temp_db)
    with pytest.raises(ValidationError, match="Data quality checks FAILED"):
        run_data_quality_checks(temp_db, expected_repo_count=5)
