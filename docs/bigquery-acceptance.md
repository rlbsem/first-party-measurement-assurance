# Native BigQuery/dbt acceptance

**Status: prepared, not executed.** The existing [native public-sample queries](evidence/google-bigquery/README.md) and [local incremental acceptance](evidence/ga4-warehouse/README.md) remain separate historical records. This extension adds a pinned adapter, an approval-gated synthetic runner and offline safety checks. It does not establish a production GA4 export integration.

## Environment assessment — October 20, 2026

The existing authenticated Google Cloud console showed the original project's **BigQuery sandbox** banner and the signed-in principal's **Owner** role. Its explorer showed no datasets. This is a UI observation, not an SDK dataset inventory or a successful permission probe. No new query job, dataset, model deployment, billing change or IAM change was submitted during assessment. Account identifiers and the principal's email are omitted from public evidence.

The Work environment had neither `gcloud` nor `bq` on its search path, no standard application-default credential file, and no configured Google credential environment variable. Browser sign-in does not authenticate Python/dbt. Credentials were not requested or retrieved.

Google's [sandbox documentation](https://docs.cloud.google.com/bigquery/docs/sandbox) excludes DML. The event fact requires a real `MERGE`; a table rebuild would not demonstrate that incremental contract. Consequently **the billing-disabled sandbox cannot complete this acceptance**. The runner independently reads project billing metadata before any BigQuery write/job and rejects billing-disabled or unknown status. It never attaches billing or enables an API.

Lowest-cost complete route: separately approve a small run in an existing eligible, billing-enabled project using available free query allowance if any. This does **not** guarantee zero charges. Attaching billing to the current project would itself require separate owner authorization; it is not proposed as an automatic setup step. A sandbox-only full build could provide partial additional evidence but would leave MERGE acceptance unverified.

## Changes to the existing implementation

- Preserve all seven models, their financial definitions, source fixtures, admission batches and five comparison rowsets. The new landing-value test rejects missing item amounts/quantities rather than allowing `SUM` to conceal unknowns. Missing user/session identity remains a visible finding.
- Render numeric finding tests as numeric literals (`quote: false`) for BigQuery's strict comparison types; the allowed values remain exactly 0 and 1.
- Pin `dbt-bigquery==1.9.2` with existing `dbt-core==1.9.8` in [an optional complete lock](../requirements-bigquery.lock). The original core lock remains unchanged. The profile bounds queries and disables job retries. Run labels use the actual adapter's `query-comment` mechanism, not an unsupported profile field.
- Extend the package allowlist to include the previously omitted `ga4_pipeline/`, its CSV fixtures and the optional adapter lock.
- Add [an independent Python oracle](../scripts/bigquery_oracle.py) for every field in all five modeled rowsets. Comparisons preserve duplicate multiplicities and nulls; matching row counts or aggregate revenue alone is insufficient.
- Prepare [the native runner](../scripts/verify_bigquery.py). Offline planning creates no Google client. Execution requires a matching plan digest, unchanged source hashes, unexpired plan and a new private output directory.

## Landing authority and nested export boundaries

The trusted acceptance inputs are the existing **11 synthetic event revisions and nine item-version rows**, admitted physically in two batches. The first batch has five events/four items; the next has six event revisions/five items. Event revision `ev03` corrects historical revenue/items; `ev06` arrives with an earlier event date. These values were not inferred from Google's sample.

The optional [event](../bigquery/ga4_event_projection.sql) and [item](../bigquery/ga4_item_projection.sql) SQL unpack actual export fields: repeated `event_params`, nested `ecommerce`, and repeated `items` with positional offsets. The scalar session lookup now rejects repeated `ga_session_id` keys rather than picking an arbitrary one. Missing monetary values stay null. These projections have **not** been run or adopted as a landing service.

GA4 does not promise that the candidate hash is an immutable event ID. Identical identity fields can collide, and changing a transaction ID changes the candidate hash. A landing authority must reject collisions, retain stable canonical keys across corrections, assign trusted versions and commit complete event/item batches before admission. The hardcoded projection version `1` is a single-snapshot illustration, not correction detection. Versioned item positions must be unique, amounts/quantities complete, and every item revision must belong to an event revision. Currency must be homogeneous or converted by a separately governed process; the existing fixture's integer cents do not implement foreign exchange. No production connector, export polling, automatic re-export deduplication or multi-currency assurance is claimed.

## Exact proposed resources and permissions

For owner-approved project `<project>` and fresh run ID `<run>`, the offline plan pins these **three new US datasets**:

| Dataset | Contents |
| --- | --- |
| `fpa_<run>_landing` | `event_versions`, `item_versions`; explicit schemas, four batch-load jobs |
| `fpa_<run>_incremental` | Seven dbt models, including one partitioned incremental event table |
| `fpa_<run>_full` | Seven independently built models; disposable duplicate probe after parity capture |

dbt may create temporary relations during materialization. Each dataset is private by default, carries exact run/plan labels and has a 24-hour default table expiration. No bucket, scheduler, export, service account, key, reservation, streaming API or public-data scan is created by the runner. No existing dataset can be reused: it checks all names for collision before creating any. A partially acknowledged create is recorded for ownership-checked cleanup.

The authenticated principal needs project job creation and dataset creation, own-job listing/get/cancel, dataset get/delete, and table create/get/list/getData/update/updateData/delete on these new datasets. A typical controlled arrangement is BigQuery User on the execution project with ownership of the datasets it creates; do not grant project-wide Data Owner merely for this test. The billing metadata check also requires `resourcemanager.projects.get` on this project and access to the Cloud Billing API. See Google's [BigQuery IAM reference](https://docs.cloud.google.com/bigquery/docs/access-control) and [project billing-info API](https://docs.cloud.google.com/billing/docs/reference/rest/v1/projects/getBillingInfo). The observed Owner role is broader; no new role assignment has been made or tested.

## Limits, cost proposal and approval

Proposed single-run planning allowance: **USD 0.25**, subject to explicit approval. One thread; 100,000,000 maximum bytes billed per query; query cache disabled for runner readbacks; 120-second query execution timeout; 30-second job-creation timeout; zero dbt job retries. No automatic restart of an interrupted run.

Using a conservative **250-job × 100 MB** planning envelope yields 25 GB, or approximately **USD 0.142 in query processing** at US on-demand USD 6.25/TiB before any free allowance. The expected tiny synthetic table storage for one day adds far less than one cent. Actual job counts, multi-statement execution, remaining shared free allowance, account pricing and post-deletion storage/time-travel accounting must be checked at execution. See [official pricing](https://cloud.google.com/bigquery/pricing?authuser=1).

This is a planning estimate, **not a hard aggregate spending cap**. The runner stops before another dbt phase when the observed job count exceeds its planning envelope. Per-job limits do not impose an account-wide currency cap. Budget alerts also are not hard caps. If the owner requires an absolute zero-charge guarantee, leave this native lane blocked; do not enable billing to work around that requirement.

Before execution, present the exact project, generated dataset names, plan digest, source hashes, authenticated capability results, remaining allowance if available, and cleanup scope. Obtain explicit approval for those writes/jobs and the possibility of charges within the planning allowance. No approval is implied by a PR merge or a browser login.

## Reproducible procedure

1. Install Python 3.12 dependencies from the optional lock in a separate environment. For credential-free adapter validation, run:

   ```text
   python -m pip install -r requirements-bigquery.lock
   python -m pip check
   python -m pytest -q tests/test_bigquery_acceptance.py tests/test_ga4_contract.py
   python -m dbt.cli.main parse --project-dir ga4_pipeline --profiles-dir ga4_pipeline --target bigquery --no-partial-parse
   ```

   `parse` checks the actual adapter/project configuration without connecting. It does not validate native SQL execution, effective IAM, quotas or billing.

2. Produce the offline, account-specific plan. It expires after 24 hours and pins all execution inputs:

   ```text
   python scripts/verify_bigquery.py plan --project <project> --run-id <run> --output .local/bigquery-plan.json
   ```

3. Stop for the resource/cost approval described above. The present sandbox is ineligible. After separate authorization for an eligible project, establish short-lived local ADC through an approved Google Cloud CLI user-login flow, such as `gcloud auth application-default login`, or an existing workload identity. Never paste tokens or commit ADC files. Installing/authenticating the CLI is not done by this runner. A browser session is insufficient; no service-account JSON key is required by the design.

4. Only after approval, execute the exact reviewed digest into a fresh ignored directory:

   ```text
   python scripts/verify_bigquery.py execute --plan .local/bigquery-plan.json --approved-sha256 <reviewed-digest> --output .local/bigquery-acceptance-<run>
   ```

   The runner verifies eligible billing metadata and collision-free names, creates the datasets, loads and reads back batch 1 exactly, and builds/tests seven models. It appends and reads back batch 2, builds/tests incremental MERGE, replays the admitted batch without another append, and builds/tests a fresh independent dataset. All five modeled rowsets must equal both the Python source oracle and the independent full build. It captures an executed successful MERGE, then inserts exactly one duplicate into the **disposable full-build fact** and requires the specific dbt uniqueness test to fail. No source or incremental model is corrupted by that probe.

5. Review private `acceptance.json`, `jobs.json`, complete rowsets, per-phase dbt manifests/results/compiled SQL/logs and cleanup outcomes. Jobs include actual IDs, SQL and its hash, state/errors, labels, processed/billed bytes, slot milliseconds, cache status and query caps. These are evidence artifacts, not invoices. They exist only after real execution.

## Cleanup and failure recovery

The runner attempts cleanup in `finally`, including failed tests. It inventories only this principal's jobs since run start with the exact run label, reconciles its explicit submission IDs, requests cancellation where needed and requires terminal job state before deleting inputs. Dataset deletion requires both exact plan/run labels and US location. It then performs fresh `get_dataset` calls requiring NotFound for all created datasets. Authorization errors and unavailable job metadata are not treated as absence.

Dataset/explicit job intents are written before submission. A lost load acknowledgement is not retried as a new append. A failed or interrupted run must **not** be blindly restarted, even with the same input plan: dataset collision checks refuse reuse. Inspect the private intent registry and job IDs, reconcile results using Google metadata, terminate only this run's jobs, and delete only datasets whose identity/labels match the approved plan. If submission status is still ambiguous, leave it marked incomplete rather than deleting or claiming cleanup. Cloud-side dbt job discovery relies on labels and the service's job listing; a host crash, authentication expiry or delayed job listing still requires operator follow-up. A 24-hour table TTL is a fallback, not an independent scheduled cleanup service; views/empty datasets and retained storage are not proven absent by TTL.

No cleanup or delayed billing clearance has been executed for this proposed run. After native execution, independently inventory exact dataset IDs, confirm terminal jobs and report any residual retention or unavailable billing data separately.

## Public evidence checklist

Keep raw artifacts private. Publish only a reviewed synthetic subset with account/email/dataset identifiers redacted consistently, while preserving a private mapping to genuine job IDs and source/SQL hashes. Reject credentials, private paths, ADC data, authorization headers and unreviewed logs. Include actual seven-model/test coverage, the expected negative failure, five-rowset hashes/counts, replay/full-build equality and cleanup observations. Do not overwrite October 17 query evidence or earlier local records. A redacted native evidence checker can be finalized against the actual service artifacts after execution; no fabricated native result is supplied here.
