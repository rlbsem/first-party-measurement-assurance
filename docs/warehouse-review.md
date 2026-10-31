# GA4 warehouse verification: native SQL and incremental correctness

## Execution boundaries

This review strengthens the existing warehouse layer without changing the browser application, financial definitions, fixtures or previously recorded observations. [New evidence](evidence/warehouse-review/README.md) is separate from the October 17 [native sample queries](evidence/google-bigquery/README.md) and October 20 [local warehouse acceptance](evidence/ga4-warehouse/README.md).

| Lane | Observed result |
| --- | --- |
| Existing seven model SELECT bodies | Executed as one read-only GoogleSQL CTE query against a bounded public GA4 cohort |
| Independent raw-source comparison | Separate native query; all five row-count/multiset signatures agree |
| Native dbt build or incremental MERGE | **Not executed**: no SDK application-default credentials; the authenticated project is still a billing-disabled sandbox |
| Local DuckDB/dbt incremental | Original two admissions, correction, late arrival, replay and independent full build passed |
| Data-quality regression guard | All event fields and extra/missing facts checked; 14 executed negative cases detected |
| Browser/source financial application | Reproduced separately in a fresh source copy; historical root files preserved |

Google's [current sandbox documentation](https://docs.cloud.google.com/bigquery/docs/sandbox) excludes DML. Browser authentication is not [application-default credentials](https://cloud.google.com/docs/authentication/application-default-credentials) for Python/dbt. The provider-native BigQuery connector was also reported disabled by the workspace administrator. No credential setup, billing activation, IAM change or persistent dataset was performed. The existing [guarded native runner](bigquery-acceptance.md) is unchanged and still requires separate approval and DML-capable access.

## What ran on BigQuery

The source is Google's obfuscated `ga4_obfuscated_sample_ecommerce` **20210101** shard. The scope includes every purchase event and same-day events for pseudo-users who purchased. It is a purchase-user cohort, not the full day's population. Sessions are observed `(user_pseudo_id, session_id)` groups within that slice; they can be censored at its boundaries.

[warehouse_preview.py](../scripts/warehouse_preview.py) reads the actual seven model SQL files and source-relation macro, using strict Jinja rendering with `target.type=bigquery`, one snapshot admission and `is_incremental=false`. It resolves references into CTEs and removes materialization configuration. **This is not dbt compilation or execution.** BigQuery then actually executes those SELECT bodies, including event selection, separate item grain, purchase quality, sessions and daily aggregation.

The [independent source query](../bigquery/warehouse_source_oracle.sql) does not reference modeled relations. It computes purchase item values through correlated subqueries and sessions directly from the raw cohort, avoiding event/item fan-out. Both queries fingerprint the same ordered normalized fields using SHA-256 per row and SHA-256 of the sorted concatenation, retaining duplicate multiplicities and nulls. Opaque snapshot event keys are excluded; item comparisons include parent event fields. This establishes agreement for the selected normalized fields, not every original GA4 field or a vendor-signed result attestation.

| Compared rowset | Native rows |
| --- | ---: |
| Event facts | 1,856 |
| Item facts, including non-purchase events | 4,828 |
| Purchase-event quality | 14 |
| Observed user/session keys | 12 |
| Daily mart | 1 |

All five signatures match independently executed source results. These are dataset observations, not customer outcomes. The complete purchase fields include duplicate-transaction and revenue findings; no test requires those findings to become zero.

The normalized USD amounts use Google's `purchase_revenue_in_usd` and `price_in_usd`, rounded to integer cents per unit. Missing amounts remain null. This is not a new FX engine and does not make the older illustrative projections a production multi-currency adapter.

### Identity and late data

GA4 does not guarantee a canonical immutable event ID from the selected fields. The read-only preview therefore uses a content hash plus occurrence ordinal **within one snapshot**, preserving identical/repeated rows instead of silently deduplicating them. These keys do not identify corrections across re-exports. A production landing authority must supply stable identity, revisions and complete committed batches before incremental admission.

The existing incremental event fact retains its source-assigned event key and source-version contract. It selects by admission batch, not event-date watermark. The unchanged fixture admits `ev06` with an older date and corrects `ev03`, including replacement of its item set. Five complete modeled rowsets match a separate full database and an independent Python oracle; replay leaves them unchanged. Additional local SQL tests verify that one session can cross midnight, identical session IDs across people remain separate, and repeated purchase events retain transaction-quality findings.

## Regression guard correction

The old `assert_no_regression` checked date, arrival batch, version and revenue only, using a one-way join. It could miss changed event names, users, sessions or transaction IDs, and an extra fact outside the admitted slice. The revised test uses a full outer join and null-safe comparison of **all normalized event fields**.

[Executed before/after observations](evidence/warehouse-review/local/guard-observations.json) preserve the original SQL hash and baseline revision. The original guard missed eight of 14 negative cases. The revised guard detects all 14, while the unmodified positive fixture remains clean. This corrects a data-quality acceptance gap; it is not a claim of a discovered production incident.

The test blocks acceptance; it does not roll back dbt's non-atomic model writes. Do not publish or grant consumers access to a failed candidate build. The native runner already requires successful dbt tests and independent readback before it reports acceptance.

## Resource and provenance record

Both data queries ran uncached and processed **19,631,768 bytes** combined. Provider metadata records **20,971,520 job-reported billed bytes**, which is a query statistic, not an invoice. A separate metadata query processed 0 bytes. The console remained a billing-disabled sandbox.

The redesigned editor exposed validator estimates of **9.53 MB**, **9.19 MB** and **0 B**, inspected before execution. Per-job byte-cap and timeout settings were not verified in this UI run; no configured limits are claimed. Actual data scans were each below the 100 MB review threshold. The existing SDK runner retains its stricter configured limits and was not invoked. Only temporary query results and job history were created; no persistent warehouse resources or live cloud configuration changed.

[Provider job IDs, exact counters, SQL hashes and timestamps](evidence/warehouse-review/native-readonly/job-statistics-results.json) bind the native jobs to the tracked SQL, including the editor's CRLF line endings. Account/project identifiers are excluded. Source files and model bodies are separately hashed. SQL ran while those exact source bytes were uncommitted; the subsequent source checkpoint is recorded explicitly, without claiming a clean checkout at execution time.

## Reproduce without Google credentials

```text
python scripts/check_warehouse_preservation.py
python scripts/check_google_evidence.py
python scripts/check_warehouse_preview_evidence.py
python -m pytest -q tests/test_ga4_regression_guard.py tests/test_warehouse_preview.py tests/test_warehouse_preview_evidence.py
python scripts/verify_ga4_pipeline.py --output .local/ga4-new.json
python scripts/verify_current_copy.py --output .local/core-new
```

Use a fresh directory for the final command. It runs the unchanged full application verifier inside an isolated copy, retaining new browser/dbt output there and leaving original evidence intact. The preservation check protects the original application, source contracts, model bodies, fixtures, tests, license and historical artifacts.

`python scripts/warehouse_preview.py --output .local/sql-preview` regenerates the SELECT queries offline. It performs no authentication or Google calls. The checked SQL files are now executed evidence sources; preserve them and create a separately versioned capture if transformation semantics change.

To finish native dbt acceptance later, the minimum missing access is separately approved short-lived ADC for an isolated DML-capable project, plus exact resource/cost authorization. Do not attach billing, create credentials, loosen the current runner gate or treat the read-only preview as MERGE verification. [Original native acceptance plan](bigquery-acceptance.md).
