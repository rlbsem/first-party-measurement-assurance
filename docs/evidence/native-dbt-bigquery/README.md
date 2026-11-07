# Native dbt-BigQuery acceptance

**Executed November 7, 2026 UTC (November 6 in Toronto).** The unchanged seven-model GA4 warehouse ran through the pinned BigQuery adapter against the existing synthetic versioned fixture, in a separately approved Google Cloud Free Trial project. This closes the native build/MERGE acceptance gap. It is not a production GA4 export or a public-sample dbt deployment.

Source revision: [`a1438e3eef101e23d61f0fd7889272098e50f9f6`](https://github.com/rlbsem/first-party-measurement-assurance/tree/a1438e3eef101e23d61f0fd7889272098e50f9f6). Python 3.12.3, dbt Core 1.9.8, dbt-BigQuery 1.9.2, google-cloud-bigquery 3.46.2. The exact approval plan binds 27 source files; no fixture, SQL, financial definition or native runner changes were needed.

## Executed results

| Phase | Event / item / purchase / session / daily rows | Native dbt result |
|---|---|---|
| First admission batch | 5 / 4 / 3 / 3 / 3 | Seven models and 30 tests passed |
| Second batch: correction and late event | 10 / 8 / 7 / 6 / 5 | Seven models and 30 tests passed; MERGE affected six rows |
| Replay without another source append | 10 / 8 / 7 / 6 / 5 | Seven models and 30 tests passed; MERGE affected zero rows |
| Independent full-refresh dataset | 10 / 8 / 7 / 6 / 5 | Seven models and 30 tests passed |
| Duplicate probe after parity capture | One duplicate inserted only in the disposable full-build fact | The exact uniqueness test failed with one result, as required |

Every field in all five rowsets matches the independent Python source oracle at each phase, preserving nulls and duplicate multiplicities. Incremental, replay and independent full-build rowsets match exactly. Historical purchase `ev03` advances to version 2 and 13,000 cents; the old item version is excluded. Late historical event `ev06` remains present. Two purchase-event rows retain a duplicate-transaction finding, and one retains an item/revenue discrepancy. Passing quality tests do not erase those intentional source findings.

Actual MERGE job IDs:

- Correction/admission: `d59a1ff0-0672-4543-88cc-806a61990676`.
- Replay: `f9581672-4e00-486f-909c-ae14e8636b72`.

Both provider records identify the statement as `MERGE`, not merely SQL text containing that word. Their independently reread metadata agrees with dbt's job IDs and affected-row responses.

## Inspect the evidence

- [Native acceptance and phase summaries](2026-11-07/acceptance.json).
- [All four 37-node dbt results and the expected negative failure](2026-11-07/dbt-results.json).
- [Complete modeled rowsets for all four phases](2026-11-07/rowsets.json).
- [178 genuine job records, SQL and resource counters](2026-11-07/jobs.json).
- [Independent post-run API observations](2026-11-07/independent-audit.json).
- [Source hashes, exact revision, dbt node inventory and private-original digests](2026-11-07/provenance.json).
- [Immutable record hashes](2026-11-07/artifact-sha256.json).

Project identity is consistently replaced with `<execution-project>`. Actual job IDs are retained. Each query carries both its original executed SQL hash and a separate sanitized SQL hash; the sanitized text is not represented as byte-identical to the submitted query. Private logs, authentication data, account identity and the original 355-file archive remain private. This compact record avoids importing every generated manifest and compiled-file copy.

## Resources, cleanup and limits

The run used three newly created private **US** datasets, four batch loads and **174 top-level query jobs**. Independent API reads confirmed all 178 recorded jobs terminal without service errors and freshly returned NotFound for each dataset after teardown. The runner's own ownership-checked cleanup agreed. The negative dbt test fails on returned duplicate findings; its underlying SELECT job succeeds normally.

Provider counters report **45,741 bytes processed**, **2,306,867,200 bytes billed** and **22,083 slot-milliseconds**. Per-table/query minimums explain why billed bytes exceed the tiny source scan. At the published US on-demand USD6.25/TiB rate, billed bytes correspond to approximately **USD0.013113 before free allowances**, below the approved USD0.25 planning allowance. These are job statistics and a list-price calculation, not an invoice, actual debit, or billing clearance. [Google pricing](https://cloud.google.com/bigquery/pricing).

Every query retained the 100,000,000-byte per-query limit. The planning envelope was not an aggregate hard cap. The Free Trial was not upgraded; no billing association or IAM grants were changed. Only the specifically approved Cloud Billing API was enabled for metadata verification. The historical billing-disabled sandbox was untouched. Delayed billing, time-travel/fail-safe retention and storage accounting remain distinct from confirmed dataset absence; no zero-charge guarantee is claimed.

## Revalidate without credentials

```text
python scripts/check_bigquery_execution.py
python -m pytest -q tests/test_bigquery_execution_evidence.py
python scripts/check_warehouse_preservation.py
python scripts/package.py --check
```

The checker validates source binding, record hashes, complete oracle parity, all 37 dbt nodes per successful build, native MERGE job types and six/zero affected rows, dbt-to-provider job links, query caps, independent resource totals and fresh cleanup observations. Fault tests reject altered data, dropped nulls, duplicate facts, missing tests/jobs, changed SQL, changed counters and unsupported production/billing claims. These checks inspect recorded observations offline; GitHub CI does not repeat cloud execution.

To export a future authorized run, retain its private plan, preflight, complete dbt artifacts and independent audit; validate the private archive first. Run [the local exporter](../../../scripts/export_bigquery_execution.py) with `--input <private-run-directory> --archive <verified-private-archive> --output <new-record-directory>`. It verifies the original file hashes, selects only the documented fields, redacts project identity and refuses to overwrite an existing record. The published checker is pinned to this acceptance revision and date; a different executed version needs a separately reviewed record/checker update. No cloud run is authorized by these commands. See the [approval-gated runbook](../../bigquery-acceptance.md).

## Evidence boundaries

The [October public-sample GoogleSQL queries](../google-bigquery/README.md), [later read-only model SELECT preview](../warehouse-review/README.md), [local synthetic incremental builds](../ga4-warehouse/README.md) and this native synthetic dbt acceptance are separate executed evidence sets. This finite fixture does not prove production export ingestion, automatic landing-version authority, multi-currency correctness, warehouse performance at enterprise scale, or commercial outcomes. Measurement Protocol debug remains unexecuted without an approved isolated GA4 property.

Historical evidence and its original index remain byte-for-byte preserved. This page is the current native acceptance entry point.
