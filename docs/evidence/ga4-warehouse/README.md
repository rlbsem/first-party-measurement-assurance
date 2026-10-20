# GA4 warehouse: local execution evidence

Two GitHub Actions jobs ran the isolated synthetic GA4 warehouse on October 20, 2026 (UTC), one on Ubuntu and one on Windows. Both succeeded. [Inspect the original logs and short-lived artifacts](https://github.com/rlbsem/first-party-measurement-assurance/actions/runs/39360154556). The existing measurement application's [Windows/Linux verification](https://github.com/rlbsem/first-party-measurement-assurance/actions/runs/39360154330) also succeeded.

The first admitted batch produced five event facts and three purchase facts. The second admitted batch produced ten event facts and seven purchase facts, including a late historical event and a corrected purchase. The independently computed source oracle found two duplicate-transaction event rows and one unexplained item/revenue variance. The model kept those findings visible.

Five modeled rowsets matched a separate full-refresh database exactly. Replaying the same admitted batch left all five results unchanged. The workflow separately checked source-revision collisions, orphan items and duplicate item indexes through negative tests.

[Normalized observed results](local-acceptance.json) · [Implementation and controls](../../ga4-warehouse.md) · [Original BigQuery public sample evidence](../google-bigquery/README.md).

**Evidence boundary:** This was local DuckDB/dbt with synthetic GA4-style data. The native BigQuery queries published in October are separate aggregate queries against Google's sample, not an execution of these incremental models. No cloud write, GA4 export or native dbt-bigquery run was performed.
