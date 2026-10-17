# Native Google execution: explicit optional lanes

## Current status

**Native BigQuery executed on 2026-10-17.** BigQuery Studio ran the two tracked public-data queries uncached in an explicitly authorized billing-disabled sandbox, followed by a read-only metadata query. [Results, job IDs, SQL hashes and resource usage](evidence/google-bigquery/README.md). Both console validator estimates were reviewed before execution. This was browser-based Google Cloud Console execution; the Python client and ADC route below remain unexecuted.

**Measurement Protocol debug remains blocked:** no approved isolated GA4 property is configured. No debug request, production collection request or GA4 property change was made. No billing account, payment method, paid resource or persistent dataset was configured. The credential-free core remains independent of both optional Google access paths.

## Reproduce through BigQuery Studio

1. Sign in to an authorized billing-disabled project and confirm the **sandbox** banner before running anything. Do not select Start free or Upgrade.
2. Paste each tracked SQL file into a separate SQL query tab. Inspect the real console validator estimate for both queries before either runs. Abort if either is invalid, unknown or above 100,000,000 bytes. The recorded estimates were displayed as 8.17 MB and 459 KB; these are rounded estimates, not exact byte counts.
3. For **each** tab, choose **Edit > Query settings**. Clear **Use cached results**, set **Job timeout** to `90000`, expand **Advanced options**, and set **Maximum bytes billed** to `100000000`. Keep temporary results as the destination. Review the visible settings before Run.
4. Execute each query once. Capture Results/JSON and Job information, excluding account identity and project details from public copies. Run the [recorded metadata lookup](evidence/google-bigquery/job-statistics.sql) in the same project with your new job IDs and execution date to obtain exact byte counters, timestamps and provider SQL hashes. Apply the same limits; its recorded validator estimate and processed bytes were zero.
5. Preserve genuine irregularities. Two independent queries must agree on purchase count; event counts must not be replaced by item counts or distinct transaction IDs. Console line endings may be CRLF: record the actual provider hash separately from the LF source-file hash.

The completed run used this console path without exporting browser credentials. Three query jobs completed: two public-data jobs and one metadata-only job. Only aggregate results and non-secret job identifiers are published. Console client version is not exposed; no Python client version is claimed.

## Optional Python BigQuery route (not executed)

Use an isolated Google Cloud project with the BigQuery API enabled, preferably an owner-approved sandbox without billing. Google documents sandbox access to public datasets. The operator needs permission to create query jobs in that project and read the official public dataset. Do not supply private client datasets or credentials in the repository.

1. Supply the approved project ID and configure Application Default Credentials outside the repository using Google's supported authentication flow. If a billing-enabled project is used, obtain explicit cost approval first; a byte cap is not a guarantee of zero cost.
2. Install the optional Google client in a separate environment or this ignored virtual environment: `python -m pip install google-cloud-bigquery`. This client is not part of the credential-free core lock and its executed version must be recorded when the native lane is run.
3. Review both public-data files in `bigquery/`. They read only the public `20210101` shard and perform no DDL, load jobs or persistent writes.
4. Run a dry-run estimate:

```text
python scripts/native_bigquery.py --dry-run --project YOUR_APPROVED_PROJECT
```

5. Review the estimates and remaining free quota. After owner authorization, run:

```text
python scripts/native_bigquery.py --execute --project YOUR_APPROVED_PROJECT --approved-project YOUR_APPROVED_PROJECT --max-bytes 100000000
```

The runner estimates both queries first, refuses unknown/over-budget estimates, then applies a server-enforced maximum of 100 million billed bytes **per query**, with query cache disabled. At most two query jobs are submitted. The default command without execution flags makes no network request. The approval flag is an operator attestation, not proof of billing state or IAM enforcement.

Results go under ignored `.local/`: SQL hashes, job IDs, bytes processed/billed, slot milliseconds, cache state, aggregate result rows and independent purchase-count parity. No user-level public rows are exported. Resource measurements are query statistics, not a bill. A query failure is not a successful native proof; inspect privately and publish only reviewed aggregates and sanitized identifiers. Neither a dry run nor stub tests count as native execution.

The official sample is obfuscated and can contain inconsistencies. It is an ecommerce query-validation lane, not data from fictional Evermere or its high-value projects. Missing transaction IDs and item/revenue disagreements are diagnostic observations, not automatically test failures. Independent purchase counts must agree. The completed console proof establishes GoogleSQL execution against the sample, **not a GA4 property integration, live export, or dbt-bigquery execution**.

## Measurement Protocol debug validation

Use an isolated, explicitly approved test property. Configure `ASSURANCE_GA4_MEASUREMENT_ID` and `ASSURANCE_GA4_API_SECRET` only in the local process environment. Do not paste them into chat, source files, browser JavaScript or command-line arguments.

```text
python scripts/validate_mp.py
python scripts/validate_mp.py --execute
```

The first command only writes local examples of six supported lead/payment events. The opt-in command submits one six-event positive request and one negative request with a deliberately invalid event name to Google's **debug** endpoint, with `ENFORCE_RECOMMENDATIONS`. Unvalued leads omit value/currency; signed contracts and actual payments have separate USD values. It requires an empty positive result and a `NAME_INVALID` negative result. It never calls `/mp/collect`, follows redirects or logs request URLs. Public evidence retains only validation codes and field paths.

Google documents that debug validation does not populate reports and does not validate all configuration credentials. Passing this lane would establish payload validation only. It would not establish property authentication, successful reporting, consent compliance or a production GA4-to-BigQuery integration.

See [official sources](sources.md) for the schemas, dataset and access documentation.
