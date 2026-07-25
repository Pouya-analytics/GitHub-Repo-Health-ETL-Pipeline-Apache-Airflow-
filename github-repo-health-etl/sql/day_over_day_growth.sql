-- day_over_day_growth.sql
-- Once this pipeline has run for more than one day, this query computes
-- day-over-day star growth per repo using LAG() -- a natural follow-on
-- analysis once historical snapshots accumulate in repo_snapshots.
-- (Project 1 in this portfolio covers window-function-heavy SQL in much
-- more depth; this file exists so the ETL pipeline's output is shown to
-- connect directly to an analysis use case, not just sit in a database.)

SELECT
    repo,
    snapshot_date,
    stars,
    stars - LAG(stars) OVER (PARTITION BY repo ORDER BY snapshot_date) AS star_growth_since_last_snapshot,
    issue_to_star_ratio,
    ROUND(
        100.0 * (issue_to_star_ratio - LAG(issue_to_star_ratio) OVER (PARTITION BY repo ORDER BY snapshot_date))
        / LAG(issue_to_star_ratio) OVER (PARTITION BY repo ORDER BY snapshot_date), 2
    ) AS issue_ratio_pct_change
FROM repo_snapshots
ORDER BY repo, snapshot_date;
