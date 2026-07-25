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
