"""
etl_logic.py
-------------
Framework-independent ETL logic for tracking GitHub repository health
metrics over time (stars, forks, open issues, etc.).

This module contains NO Airflow imports. That's deliberate: Airflow
should be a thin scheduling/orchestration wrapper around business logic,
not the place where business logic lives. This file is directly
unit-testable and runnable standalone (see scripts/run_pipeline_standalone.py),
which is also how I validated it actually works end-to-end in an
environment where a persistent Airflow scheduler isn't available.

The Airflow DAG (dags/github_repo_health_dag.py) imports these exact
functions as PythonOperator callables.
"""

import json
import sqlite3
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List, Dict, Any

# Repos tracked by this pipeline. In a real production version this list
# would live in an Airflow Variable or a config table, not hardcoded --
# noted as a known simplification.
TRACKED_REPOS = [
    "apache/airflow",
    "pandas-dev/pandas",
    "scikit-learn/scikit-learn",
    "duckdb/duckdb",
    "apache/superset",
]

GITHUB_API_BASE = "https://api.github.com/repos"


@dataclass
class RepoSnapshot:
    repo: str
    snapshot_date: str          # ISO date this snapshot was taken
    stars: int
    forks: int
    open_issues: int
    watchers: int
    language: str
    created_at: str
    days_since_creation: int
    stars_per_day: float
    issue_to_star_ratio: float


# ---------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------
class RateLimitError(Exception):
    """Raised when the GitHub API rate limit is exhausted and retries are exhausted too."""
    pass


def extract_repo_data(repo: str, timeout: int = 10, max_retries: int = 3,
                       base_backoff_sec: float = 2.0) -> Dict[str, Any]:
    """
    Pulls raw repository metadata from the GitHub REST API.

    Retries with exponential backoff on 403/429 (rate limit) up to
    max_retries times, since rate limiting is an expected, recoverable
    condition for an unauthenticated client -- not a reason to fail the
    whole pipeline on the first hit. Any OTHER HTTPError (404, 500, etc.)
    is raised immediately, since retrying a 404 just wastes time.

    In production this pipeline would use a GitHub personal access token
    (5,000 req/hr instead of 60/hr) passed via an Airflow Connection /
    environment variable -- omitted here to keep the project runnable
    with zero credentials, noted as a known simplification.
    """
    url = f"{GITHUB_API_BASE}/{repo}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            last_error = e
            if e.code in (403, 429):
                if attempt < max_retries:
                    backoff = base_backoff_sec * (2 ** attempt)
                    print(f"[extract_repo_data] {repo}: rate limited (HTTP {e.code}), "
                          f"retry {attempt + 1}/{max_retries} in {backoff:.1f}s")
                    time.sleep(backoff)
                    continue
                raise RateLimitError(
                    f"GitHub API rate limit exhausted for {repo} after {max_retries} retries"
                ) from e
            raise  # non-rate-limit HTTP error: fail fast, don't retry
    raise last_error


def extract_all(repos: List[str] = None) -> List[Dict[str, Any]]:
    repos = repos or TRACKED_REPOS
    results = []
    errors = []
    for repo in repos:
        try:
            results.append(extract_repo_data(repo))
        except (urllib.error.HTTPError, RateLimitError) as e:
            # Don't let one bad repo kill the whole batch -- log and continue,
            # but track it so validate_batch can decide whether the failure
            # rate is acceptable.
            errors.append({"repo": repo, "error": str(e)})
    if errors:
        print(f"[extract_all] {len(errors)} repo(s) failed to extract: {errors}")
    if not results:
        raise RuntimeError("extract_all: ALL repos failed to extract -- aborting pipeline")
    return results


# ---------------------------------------------------------------------
# VALIDATE  (the step most junior portfolios skip entirely)
# ---------------------------------------------------------------------
class ValidationError(Exception):
    pass


REQUIRED_FIELDS = ["full_name", "stargazers_count", "forks_count",
                   "open_issues_count", "created_at"]


def validate_record(raw: Dict[str, Any]) -> None:
    """Raises ValidationError on the first failed check. Called per-record
    so one bad record can be isolated rather than failing silently."""
    for field in REQUIRED_FIELDS:
        if field not in raw or raw[field] is None:
            raise ValidationError(f"Missing required field '{field}' for repo "
                                   f"{raw.get('full_name', '<unknown>')}")

    if raw["stargazers_count"] < 0 or raw["forks_count"] < 0:
        raise ValidationError(f"Negative count detected for {raw['full_name']} "
                               f"-- stars={raw['stargazers_count']}, forks={raw['forks_count']}")

    # Sanity bound: catches a malformed/truncated API response rather than
    # a real edge case (no real repo has 10 billion stars)
    if raw["stargazers_count"] > 10_000_000_000:
        raise ValidationError(f"Implausible star count for {raw['full_name']}: "
                               f"{raw['stargazers_count']}")

    try:
        datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00"))
    except (ValueError, AttributeError) as e:
        raise ValidationError(f"Unparseable created_at for {raw['full_name']}: {e}")


def validate_batch(raw_records: List[Dict[str, Any]], max_failure_rate: float = 0.3) -> List[Dict[str, Any]]:
    """
    Validates each record independently. If too many records fail
    (above max_failure_rate), raises and fails the whole task -- this is
    a circuit breaker so the pipeline doesn't silently load a batch where,
    say, the API schema changed and everything is malformed.
    """
    valid, invalid = [], []
    for r in raw_records:
        try:
            validate_record(r)
            valid.append(r)
        except ValidationError as e:
            invalid.append({"record": r.get("full_name", "<unknown>"), "reason": str(e)})

    failure_rate = len(invalid) / len(raw_records) if raw_records else 1.0
    if invalid:
        print(f"[validate_batch] {len(invalid)}/{len(raw_records)} records failed validation: {invalid}")
    if failure_rate > max_failure_rate:
        raise ValidationError(
            f"Validation failure rate {failure_rate:.0%} exceeds threshold "
            f"{max_failure_rate:.0%} -- aborting load to prevent bad data downstream"
        )
    return valid


# ---------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------
def transform_record(raw: Dict[str, Any], snapshot_date: str = None) -> RepoSnapshot:
    snapshot_date = snapshot_date or datetime.now(timezone.utc).date().isoformat()

    created_at = datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00"))
    days_since_creation = max((datetime.now(timezone.utc) - created_at).days, 1)

    stars = raw["stargazers_count"]
    forks = raw["forks_count"]
    open_issues = raw["open_issues_count"]

    stars_per_day = round(stars / days_since_creation, 3)
    issue_to_star_ratio = round(open_issues / stars, 5) if stars > 0 else 0.0

    return RepoSnapshot(
        repo=raw["full_name"],
        snapshot_date=snapshot_date,
        stars=stars,
        forks=forks,
        open_issues=open_issues,
        watchers=raw.get("watchers_count", stars),
        language=raw.get("language") or "unknown",
        created_at=raw["created_at"],
        days_since_creation=days_since_creation,
        stars_per_day=stars_per_day,
        issue_to_star_ratio=issue_to_star_ratio,
    )


def transform_batch(valid_records: List[Dict[str, Any]]) -> List[RepoSnapshot]:
    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    return [transform_record(r, snapshot_date) for r in valid_records]


# ---------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------
def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repo_snapshots (
            repo TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            stars INTEGER,
            forks INTEGER,
            open_issues INTEGER,
            watchers INTEGER,
            language TEXT,
            created_at TEXT,
            days_since_creation INTEGER,
            stars_per_day REAL,
            issue_to_star_ratio REAL,
            loaded_at TEXT,
            PRIMARY KEY (repo, snapshot_date)
        )
    """)
    conn.commit()
    return conn


def load_batch(snapshots: List[RepoSnapshot], db_path: str) -> int:
    """
    Idempotent upsert: re-running the pipeline for the same day overwrites
    that day's row rather than duplicating it (PRIMARY KEY on repo+date).
    This matters because Airflow tasks can and do get retried.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    loaded_at = datetime.now(timezone.utc).isoformat()

    rows = [
        (*asdict(s).values(), loaded_at) for s in snapshots
    ]
    cur.executemany("""
        INSERT INTO repo_snapshots
        (repo, snapshot_date, stars, forks, open_issues, watchers, language,
         created_at, days_since_creation, stars_per_day, issue_to_star_ratio, loaded_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(repo, snapshot_date) DO UPDATE SET
            stars=excluded.stars,
            forks=excluded.forks,
            open_issues=excluded.open_issues,
            watchers=excluded.watchers,
            language=excluded.language,
            days_since_creation=excluded.days_since_creation,
            stars_per_day=excluded.stars_per_day,
            issue_to_star_ratio=excluded.issue_to_star_ratio,
            loaded_at=excluded.loaded_at
    """, rows)
    conn.commit()
    n = cur.rowcount
    conn.close()
    return len(rows)


# ---------------------------------------------------------------------
# DATA QUALITY CHECK  (post-load gate -- an Airflow task that can fail
# the DAG and trigger alerting, distinct from in-pipeline validation)
# ---------------------------------------------------------------------
def run_data_quality_checks(db_path: str, expected_repo_count: int = None) -> Dict[str, Any]:
    expected_repo_count = expected_repo_count or len(TRACKED_REPOS)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    today = datetime.now(timezone.utc).date().isoformat()
    cur.execute("SELECT COUNT(*) FROM repo_snapshots WHERE snapshot_date = ?", (today,))
    rows_today = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repo_snapshots WHERE snapshot_date = ? AND stars < 0", (today,))
    negative_stars = cur.fetchone()[0]

    cur.execute("SELECT MAX(loaded_at) FROM repo_snapshots WHERE snapshot_date = ?", (today,))
    last_load = cur.fetchone()[0]

    conn.close()

    checks = {
        "rows_loaded_today": rows_today,
        "expected_min_rows": int(expected_repo_count * 0.7),  # allow some tolerance
        "negative_star_count": negative_stars,
        "last_loaded_at": last_load,
        "passed": True,
        "failures": [],
    }

    if rows_today < checks["expected_min_rows"]:
        checks["passed"] = False
        checks["failures"].append(
            f"Only {rows_today} rows loaded today, expected at least {checks['expected_min_rows']}"
        )
    if negative_stars > 0:
        checks["passed"] = False
        checks["failures"].append(f"{negative_stars} rows have negative star counts")

    if not checks["passed"]:
        raise ValidationError(f"Data quality checks FAILED: {checks['failures']}")

    return checks
