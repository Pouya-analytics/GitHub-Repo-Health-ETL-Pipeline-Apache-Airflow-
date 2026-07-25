"""
Test fixtures: realistic GitHub repo API response payloads, matching the
exact schema GitHub's REST API returns (field names/types verified against
a live response captured during development -- see README for the note
on rate limiting).

Using fixtures instead of live API calls in unit tests is standard
practice, not a workaround for missing access: tests should be fast,
deterministic, and not depend on network availability or a third party's
rate limit. The one component fixtures can't test -- whether
extract_repo_data() correctly talks to the real API -- is covered
separately by test_extract_live.py, which DOES make a real network call
and is meant to run in an environment with a higher rate limit (e.g. an
authenticated token in CI), or manually when quota is available.
"""

VALID_REPO_RESPONSE = {
    "full_name": "apache/airflow",
    "stargazers_count": 45939,
    "forks_count": 17300,
    "open_issues_count": 1715,
    "watchers_count": 45939,
    "language": "Python",
    "created_at": "2015-04-13T18:04:58Z",
}

VALID_REPO_RESPONSE_2 = {
    "full_name": "duckdb/duckdb",
    "stargazers_count": 22500,
    "forks_count": 1700,
    "open_issues_count": 480,
    "watchers_count": 22500,
    "language": "C++",
    "created_at": "2018-06-26T15:01:00Z",
}

MISSING_FIELD_RESPONSE = {
    # missing 'created_at' -- should fail validation
    "full_name": "broken/repo",
    "stargazers_count": 100,
    "forks_count": 10,
    "open_issues_count": 5,
}

NEGATIVE_COUNT_RESPONSE = {
    # malformed/corrupted data -- should fail validation
    "full_name": "corrupt/repo",
    "stargazers_count": -1,
    "forks_count": 10,
    "open_issues_count": 5,
    "created_at": "2020-01-01T00:00:00Z",
}

UNPARSEABLE_DATE_RESPONSE = {
    "full_name": "baddate/repo",
    "stargazers_count": 50,
    "forks_count": 5,
    "open_issues_count": 2,
    "created_at": "not-a-date",
}
