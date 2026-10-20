# GA4 event-to-warehouse modeling

**Current status:** [Local synthetic acceptance passed on Windows and Linux](../docs/evidence/ga4-warehouse/README.md). Native dbt-bigquery not executed.

This isolated dbt project extends First-Party Measurement Assurance with event, item, session and purchase-quality grains, governed daily marts, incremental event corrections and independent source-to-mart reconciliation.

Read [implementation, boundaries and native acceptance requirements](../docs/ga4-warehouse.md). The original native Google sample query evidence is unchanged.

## Local acceptance

```text
python scripts/verify_ga4_pipeline.py --output .local/ga4-run.json
```

The test builds batches one and two, checks late events and corrected source versions, verifies duplicate transaction and item-value findings, replays the admitted batch, and compares five modeled rowsets to an independent full-refresh database.

The `bigquery` profile and two source projections are optional execution contracts. They do not make cloud requests or create datasets from this repository.

The [native acceptance procedure](../docs/bigquery-acceptance.md) includes the optional pinned adapter, offline checks, approval-gated runner and sandbox DML blocker. It has not executed native dbt models.
