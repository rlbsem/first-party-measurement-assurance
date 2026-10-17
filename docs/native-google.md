# Native Google execution: explicit optional lanes

## Current status

Native BigQuery and Measurement Protocol validation **have not executed**. Local tooling discovery found no BigQuery connector or `bq`/`gcloud` executable, and no configured Google Application Default Credentials. No project was provisioned, billing enabled, credential read, or vendor request submitted. The reproducible core is complete without this access.

## BigQuery public dataset

Use an isolated Google Cloud project with the BigQuery API enabled, preferably an owner-approved sandbox without billing. Google documents sandbox access to public datasets. The operator needs permission to create query jobs in that project and read the official public dataset. Do not supply private client datasets or credentials in the repository.

1. Supply the approved project ID and configure Application Default Credentials outside the repository using Google's supported authentication flow. If a billing-enabled project is used, obtain explicit cost approval first; a byte cap is not a guarantee of zero cost.
2. Install the optional Google client in a separate environment or this ignored virtual environment: `python -m pip install google-cloud-bigquery`. This client is not part of the credential-free core lock and its executed version must be recorded when the native lane is run.
3. Review both files in `bigquery/`. They read only the public `20210101` shard and perform no DDL, load jobs or persistent writes.
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

The official sample is obfuscated and can contain inconsistencies. It is an ecommerce query-validation lane, not data from fictional Evermere or its high-value projects. Missing transaction IDs and item/revenue disagreements are diagnostic observations, not automatically test failures. Independent purchase counts must agree. This proof would establish GoogleSQL execution against the sample, **not a GA4 property integration, live export, or dbt-bigquery execution**.

## Measurement Protocol debug validation

Use an isolated, explicitly approved test property. Configure `ASSURANCE_GA4_MEASUREMENT_ID` and `ASSURANCE_GA4_API_SECRET` only in the local process environment. Do not paste them into chat, source files, browser JavaScript or command-line arguments.

```text
python scripts/validate_mp.py
python scripts/validate_mp.py --execute
```

The first command only writes local examples of six supported lead/payment events. The opt-in command submits one six-event positive request and one negative request with a deliberately invalid event name to Google's **debug** endpoint, with `ENFORCE_RECOMMENDATIONS`. Unvalued leads omit value/currency; signed contracts and actual payments have separate USD values. It requires an empty positive result and a `NAME_INVALID` negative result. It never calls `/mp/collect`, follows redirects or logs request URLs. Public evidence retains only validation codes and field paths.

Google documents that debug validation does not populate reports and does not validate all configuration credentials. Passing this lane would establish payload validation only. It would not establish property authentication, successful reporting, consent compliance or a production GA4-to-BigQuery integration.

See [official sources](sources.md) for the schemas, dataset and access documentation.
