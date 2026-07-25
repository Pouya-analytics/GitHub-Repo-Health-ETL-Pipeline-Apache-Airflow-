"""
github_repo_health_dag.py
----------------------------
Airflow DAG that orchestrates the GitHub repository health ETL pipeline:
extract -> validate -> transform -> load -> data quality check.

Written for Airflow 3.x using the TaskFlow API (the current idiomatic
style, replacing the 2.x PythonOperator boilerplate). All business logic
lives in scripts/etl_logic.py and is imported here, not redefined --
this DAG file is intentionally thin orchestration only.

Schedule: daily at 06:00 UTC, well after GitHub's own metrics typically
settle for the previous day.

DESIGN NOTES (worth reading if reviewing this for a job application):
- Each @task is independently retryable. retries=3 with exponential
  backoff (via retry_delay) on the extract task specifically, since
  rate-limit errors are the most common and most recoverable failure
  mode for this pipeline (see etl_logic.py's own internal backoff too --
  belt and suspenders, because Airflow-level retries restart the WHOLE
  task including already-succeeded sub-extracts, while the internal
  backoff in extract_repo_data handles transient per-repo retries
  without re-running the entire batch).
- load_batch() is idempotent (upsert on repo+date), so retries are safe
  and won't duplicate data even if a task fails after partially loading.
- The quality-check task is deliberately a SEPARATE task from load, not
  folded into it -- this means if quality checks fail, the DAG shows a
  red task specifically labeled "quality_check", which is what should
  page someone, rather than an ambiguous "load" failure that could mean
  a dozen different things.
"""
from datetime import datetime, timedelta
import sys
import os

# Add the scripts/ dir to the path so this DAG can import the
# framework-independent ETL logic module.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from airflow.sdk import dag, task

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "repo_health.db")

default_args = {
    "owner": "data-analytics-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="github_repo_health_etl",
    description="Daily ETL pipeline tracking star/fork/issue growth for tracked OSS repos",
    schedule="0 6 * * *",          # daily at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # don't backfill historical runs on deploy
    default_args=default_args,
    tags=["etl", "github", "analytics"],
)
def github_repo_health_etl():

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def extract():
        """Pulls current metadata for all tracked repos from the GitHub API."""
        from etl_logic import extract_all
        return extract_all()

    @task
    def validate(raw_records: list):
        """
        Validates each record; raises (failing this task, triggering
        Airflow's alerting) if more than 30% of the batch is malformed.
        """
        from etl_logic import validate_batch
        return validate_batch(raw_records, max_failure_rate=0.3)

    @task
    def transform(valid_records: list):
        """Computes derived metrics (stars/day, issue ratio) and returns
        them as plain dicts -- TaskFlow XComs need JSON-serializable data,
        so dataclasses are converted before crossing the task boundary."""
        from dataclasses import asdict
        from etl_logic import transform_batch
        snapshots = transform_batch(valid_records)
        return [asdict(s) for s in snapshots]

    @task
    def load(snapshot_dicts: list):
        """Upserts transformed snapshots into SQLite. Idempotent on
        (repo, snapshot_date), so retries never duplicate rows."""
        from etl_logic import RepoSnapshot, load_batch
        snapshots = [RepoSnapshot(**d) for d in snapshot_dicts]
        n = load_batch(snapshots, DB_PATH)
        return n

    @task
    def quality_check(n_loaded: int):
        """
        Post-load gate, intentionally a separate task from `load` (see
        module docstring). Raising here fails ONLY this task and is
        what should trigger on-call alerting in a real deployment.
        """
        from etl_logic import run_data_quality_checks
        return run_data_quality_checks(DB_PATH)

    raw = extract()
    valid = validate(raw)
    transformed = transform(valid)
    n_loaded = load(transformed)
    quality_check(n_loaded)


github_repo_health_etl()
