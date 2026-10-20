# GA4 governed warehouse: incremental modeling contract

This isolated module extends the measurement project without changing its local CRM/payment application or historical native BigQuery job evidence. It models a GA4-export-shaped **event grain**, a separate **event + item-index grain**, session aggregates and retained data-quality findings. [Local acceptance passed on Windows and Linux](evidence/ga4-warehouse/README.md) against deterministic **synthetic** data. Native dbt-bigquery execution has not occurred.

## Run locally (no vendor access)

Install the existing `requirements.lock` in an isolated Python 3.12 environment, then run:

```text
python scripts/verify_ga4_pipeline.py --output .local/ga4-run.json
```

The verifier:
1. Seeds a synthetic landing ledger and builds batch 1 in DuckDB/dbt.
2. Admits batch 2, including an older event date, a corrected purchase, a new duplicate transaction ID and an unexplained item/revenue variance.
3. Tests every model and independently compares daily marts against raw source versions.
4. Replays batch 2 to ensure no additional effect or drift.
5. Builds a second, empty physical database from scratch and compares **all modeled rowsets**, not just counts, to incremental output.

The event fact is incremental, keyed by stable `event_key`. It uses increasing **landing batch**, never an event-time watermark. The latest source revision wins for that event key; older versions cannot regress state. Item and reporting models are rebuilt from the selected event version, so an item removed by a correction does not remain as an orphan. This demo scans a small synthetic ledger for item rebuilds. Scaling this to a large dataset needs partitioned item replacement and an explicit batch commit cursor.

**Source contract:** An ingestion authority assigns unique event keys and strictly increasing landing batches. Reusing the same event key/version, colliding event hashes, corrupt item indexes or missing source revisions must be rejected, not silently deduplicated. A native source re-export may require a separate versioning/landings service before this contract is met. The model does not assert that GA4 natively supplies immutable event IDs, corrected-event version numbers, or a trustworthy arrival cursor.

**Findings stay visible:** Two different events use transaction `T200` and both are flagged. A 300-cent reported/item-revenue discrepancy is retained. Two events have missing session or pseudo-user identity. These are fixture outcomes, **not** findings from Google's GA4 public sample. For that sample, the previously executed native queries independently recorded 14 purchase events, 57 item rows, 13 distinct transaction IDs and two item/revenue disagreements on the 20210101 shard.

## Optional BigQuery acceptance (not run)

The isolated dbt project contains an optional `bigquery` profile and `ga4_landing.event_versions`/`ga4_landing.item_versions` sources. Two [BigQuery event](../bigquery/ga4_event_projection.sql) and [item](../bigquery/ga4_item_projection.sql) projections show how to select one explicitly bounded public shard into that contract. They are **read-only** source queries and have not been executed; no destination tables were created. The hash recipe can collide when GA4 rows share all selected identity fields. Check and reject such collisions before assigning landing versions.

The [native acceptance runner and approval procedure](bigquery-acceptance.md) pin a separate optional adapter lock, three new datasets, bounded synthetic admission and complete rowset comparisons. The existing billing-disabled sandbox does **not** support DML and therefore cannot demonstrate incremental `MERGE`. No billing activation or native model execution has occurred. An eligible project, Python authentication and explicit resource/cost approval remain necessary; local adapter parsing is not native acceptance.

Do not submit BigQuery queries or create any cloud resources solely because this module was merged. The earlier [native aggregate evidence](evidence/google-bigquery/README.md) remains separate and unchanged.
