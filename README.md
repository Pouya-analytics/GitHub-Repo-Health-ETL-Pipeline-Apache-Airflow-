# GitHub Repo Health ETL Pipeline (Apache Airflow)

I built this to show ETL architecture, not just a script that pulls
data. A script that calls an API and writes a CSV is one thing. A
pipeline with separated retryable stages, validation that stops bad
data before it lands, and a quality check that tells you exactly what
failed — that's different.

---

## Architecture

```
extract → validate → transform → load → quality_check
```

Each stage is a separate Airflow task. When something fails, you know
exactly where and why. The load is idempotent — retrying never creates
duplicates. The quality check is separate from the load so a data
problem doesn't look like a code problem.

---

## It actually ran

Full DAG executed against the live GitHub API:

```
Dag run in success state
start: 2026-06-26 00:00:00  end: 2026-06-26 17:33:41
```

Real data loaded that day:

| repo | stars | forks | stars_per_day |
|---|---|---|---|
| apache/superset | 73,512 | 17,716 | 18.42 |
| scikit-learn | 66,492 | 27,117 | 11.48 |
| pandas | 49,090 | 20,041 | 8.49 |
| apache/airflow | 45,940 | 17,300 | 11.23 |
| duckdb | 39,049 | 3,365 | 13.36 |

I also hit GitHub's rate limit mid-build. Rather than hide it, the
retry/backoff fires 3 times with real delays (2s, 4s, 8s) against the
actual failing API, then degrades gracefully:

```
retry 1/3 in 2.0s... retry 2/3 in 4.0s... retry 3/3 in 8.0s...
VALIDATE -- 5/5 passed
LOAD -- 5 rows upserted
QUALITY CHECK -- passed
```

---

## How to run it

```bash
pip install -r requirements.txt
python scripts/run_pipeline_standalone.py
python -m pytest tests/test_etl_logic.py -v
airflow dags test github_repo_health_etl $(date +%Y-%m-%d)
```

---

## Stack

Airflow 3.2 · TaskFlow API · Python stdlib only · pytest · SQLite
# Cookie Cats A/B Test — Statistics From Scratch

I built this because calling scipy.stats.ttest_ind() proves nothing
about whether you actually understand what a t-statistic is. Anyone
can import a function. I wanted to show I know what's happening inside
it — so I implemented Welch's t-test, chi-square, and a two-proportion
z-test directly from their formulas, then validated every result
against scipy to confirm they match.

The dataset is the well-known Cookie Cats mobile game A/B test —
does moving the first progression gate from level 30 to level 40
affect player retention? The data is synthetic but calibrated to the
real published statistics of the original Kaggle dataset.

The validation suite has 15 tests. One of them I think is worth
highlighting: for a 2x2 contingency table, the z-test and chi-square
are mathematically equivalent — z² should equal the chi-square
statistic exactly. The test suite checks this identity between two
independently written implementations. If either had a bug, this
cross-check would catch it even without comparing to scipy.

The actual finding is that sum_gamerounds shows no significant
difference between gates, but 7-day retention does — gate_30 wins.
The interesting part is deciding which metric should drive the product
decision. Engagement among players who stuck around looks the same.
But retention — whether players come back at all — is lower with the
gate moved to 40. I argued explicitly for why retention should win
that argument, not the louder engagement number.

One boundary I kept honest: converting a test statistic into a
p-value requires a CDF lookup against a known distribution. I used
scipy for that specific step and documented it clearly rather than
pretending I reimplemented the entire t-distribution from scratch.
The point of the project is understanding what a t-test computes,
not numerical methods for CDF approximation.
