# GitHub Repo Health ETL Pipeline (Apache Airflow)

A daily ETL pipeline that tracks star/fork/issue growth for a set of
open-source repositories, orchestrated with Apache Airflow 3.x (TaskFlow
API) and backed by the live GitHub REST API.

## About this project

This project demonstrates an ETL pipeline architecture, not just a script
that pulls data. The distinction matters: a script that calls an API and
writes a CSV is one task; a pipeline has separated, independently
retryable stages, idempotent loads, input validation that can halt
execution before bad data lands, and a post-load data quality gate
distinct from the load step itself.

I built the business logic (`scripts/etl_logic.py`) as a framework-
independent module first, unit-tested it (13 tests, all passing, see
`tests/`), and only then wrapped it in an Airflow DAG
(`dags/github_repo_health_dag.py`). Airflow should orchestrate; it
shouldn't contain business logic that becomes painful to test.

## Architecture

```
extract → validate → transform → load → quality_check
```

| Stage | What it does | Why it's a separate step |
|---|---|---|
| **extract** | Pulls repo metadata from `api.github.com`, with retry + exponential backoff on rate-limit (403/429) responses | Network calls fail. Retrying immediately on a 403 wastes the retry; backing off respects the API. |
| **validate** | Checks required fields, rejects negative counts, rejects unparseable dates. Aborts the whole batch if >30% of records fail. | Catches a malformed/changed API response *before* it pollutes the database, not after. |
| **transform** | Computes `stars_per_day` and `issue_to_star_ratio` | Derived metrics, not raw data — this is the actual analytical layer. |
| **load** | Upserts into SQLite on `(repo, snapshot_date)` | Idempotent — an Airflow retry after a partial failure never creates duplicate rows. |
| **quality_check** | Post-load row-count and sanity check, raises if it fails | Deliberately separate from `load` so a failure here is unambiguous: "data quality is wrong," not "the load step broke for some reason." |

## Proof this actually runs

I validated this two ways, both real, both shown below:

**1. The DAG was parsed and executed by real Airflow** (not just
syntax-checked) using `airflow dags test`, against the actual live
GitHub API:

```
$ airflow dags test github_repo_health_etl 2026-06-26
...
Done. Returned value was: 5
...
Dag run in success state
Dag run start:2026-06-26 00:00:00+00:00 end:2026-06-26 17:33:41+00:00
```

All 5 tasks (`extract`, `validate`, `transform`, `load`, `quality_check`)
completed successfully, and `airflow.models.DagBag` reported **zero
import errors** when parsing the DAG file — the same check Airflow's own
CI uses (`airflow dags list-import-errors`).

Real data loaded from that run (queried directly from the resulting
SQLite database, same day as this README was written):

| repo | stars | forks | open_issues | stars_per_day | issue_to_star_ratio |
|---|---|---|---|---|---|
| apache/superset | 73,512 | 17,716 | 919 | 18.42 | 0.0125 |
| scikit-learn/scikit-learn | 66,492 | 27,117 | 2,102 | 11.48 | 0.0316 |
| pandas-dev/pandas | 49,090 | 20,041 | 3,109 | 8.49 | 0.0633 |
| apache/airflow | 45,940 | 17,300 | 1,715 | 11.23 | 0.0373 |
| duckdb/duckdb | 39,049 | 3,365 | 555 | 13.36 | 0.0142 |

**2. Resilience under a real failure** — GitHub's unauthenticated API
allows 60 requests/hour, shared across whatever else uses the same
network egress point. During development this limit was hit mid-build.
Rather than hide that, `scripts/run_pipeline_standalone.py` shows
exactly what happens: the retry/backoff logic in `extract_repo_data()`
fires three times with increasing delays (2s, 4s, 8s), genuinely against
the live (failing) API, then the pipeline degrades gracefully to a
documented fixture path rather than crashing:

```
[extract_repo_data] apache/airflow: rate limited (HTTP 403), retry 1/3 in 2.0s
[extract_repo_data] apache/airflow: rate limited (HTTP 403), retry 2/3 in 4.0s
[extract_repo_data] apache/airflow: rate limited (HTTP 403), retry 3/3 in 8.0s
...
Live API unavailable -- falling back to fixture data...
[2/5] VALIDATE -- 5/5 records passed validation
[3/5] TRANSFORM -- stars_per_day and issue_to_star_ratio computed correctly
[4/5] LOAD -- 5 rows upserted
[5/5] DATA QUALITY CHECK -- passed: True
```

In a real production deployment this would use an authenticated GitHub
token (5,000 req/hr instead of 60), which is a one-line change
(`Authorization` header + an Airflow Connection) — not implemented here
to keep the project runnable with zero credentials.

## Repo structure

```
.
├── dags/
│   └── github_repo_health_dag.py   # Airflow DAG (TaskFlow API, orchestration only)
├── scripts/
│   ├── etl_logic.py                 # framework-independent business logic
│   └── run_pipeline_standalone.py   # runs the same logic outside Airflow
├── sql/
│   └── day_over_day_growth.sql      # follow-on analysis query using LAG()
├── tests/
│   ├── fixtures.py                  # realistic GitHub API response fixtures
│   ├── test_etl_logic.py            # 13 unit tests, no network dependency
│   └── test_extract_live.py         # integration test, real network call, skips gracefully if rate-limited
├── data/
│   └── repo_health.db               # SQLite output (committed — small file, lets you query it immediately)
└── requirements.txt
```

## How to run it

```bash
pip install -r requirements.txt

# Run the business logic standalone (no Airflow needed):
python scripts/run_pipeline_standalone.py

# Run unit tests (fast, no network):
python -m pytest tests/test_etl_logic.py -v

# Run the live integration test (needs available API quota):
python -m pytest tests/test_extract_live.py -v

# Run the actual Airflow DAG:
export AIRFLOW_HOME=~/airflow_home
airflow db migrate
airflow dags test github_repo_health_etl $(date +%Y-%m-%d)
```

## What I'd add with more time

- Authenticated GitHub API access (5,000 req/hr) via an Airflow
  Connection, instead of the unauthenticated 60/hr limit
- Swap SQLite for Postgres and add a `docker-compose.yml` for a fully
  local Airflow + Postgres stack
- A `dbt` layer on top of `repo_snapshots` for the transform stage,
  instead of doing transforms in Python — arguably the more idiomatic
  modern data-stack choice for anything beyond simple derived columns
- Alerting (Slack/email) wired to the `quality_check` task's failure
  callback

## Tech stack

Apache Airflow 3.2 (TaskFlow API) · Python 3 stdlib (`urllib`, `sqlite3`
— no `requests`/`pandas` dependency in the core logic, by design) ·
pytest · SQLite
