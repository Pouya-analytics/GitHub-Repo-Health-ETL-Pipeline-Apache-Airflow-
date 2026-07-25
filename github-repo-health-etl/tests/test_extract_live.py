"""
test_extract_live.py
----------------------
Integration test that makes a REAL network call to the GitHub API.
Separated from test_etl_logic.py (which uses static fixtures) because:

1. Unit tests should never depend on network availability or a third
   party's rate limit -- that's why validate/transform/load are tested
   against fixtures in test_etl_logic.py.
2. But extract_repo_data() ITSELF needs at least one real network test,
   or you've never actually proven the HTTP layer works.

This test is marked to skip gracefully (not fail the suite) if the
unauthenticated GitHub rate limit (60 req/hr) is currently exhausted --
which it will be most of the time in a shared CI/sandbox IP. Run it
manually when quota is available, or in a CI pipeline configured with
a GITHUB_TOKEN environment variable for the higher 5,000 req/hr
authenticated limit (not implemented here, see etl_logic.py docstring).

Run with: python -m pytest tests/test_extract_live.py -v
"""
import os
import sys
import urllib.error
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from etl_logic import extract_repo_data, RateLimitError


def test_extract_repo_data_returns_expected_schema():
    try:
        result = extract_repo_data("octocat/Hello-World", max_retries=0)
    except RateLimitError:
        pytest.skip("GitHub API rate limit exhausted -- run again later or with an auth token")
    except urllib.error.URLError:
        pytest.skip("Network unavailable in this environment")

    assert "full_name" in result
    assert "stargazers_count" in result
    assert "forks_count" in result
    assert "open_issues_count" in result
    assert "created_at" in result
    assert isinstance(result["stargazers_count"], int)
