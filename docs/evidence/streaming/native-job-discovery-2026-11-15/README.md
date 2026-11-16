# Native submission and job-discovery root cause

At source [`2c7941d8701165e14cc01182b3d83992cb21e3e1`](https://github.com/rlbsem/first-party-measurement-assurance/tree/2c7941d8701165e14cc01182b3d83992cb21e3e1), the real image build, full runtime deployment and independent guard-arming checks passed. The first full task read succeeded at a ten-second RPC timeout. The Beam driver submitted Dataflow job `2026-11-15_13_38_12-3184962932157250092`. This closes the earlier RPC readback boundary, not native processing acceptance.

## Established controller defect

The controller then raised `dataflow_submission_not_uniquely_resolved`. Its original [ListJobs summary](list-summary.json) contains the exact job name/ID and `JOB_STATE_PENDING`, but **no labels field**. The old checker filtered that list on `labels.acceptance`, discarded the job and started retirement. The service documents that [list always returns summaries](https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/list). A separate exact [job read](job-summary.json) returns the ownership label. The original filter's false negative is reproduced directly in regression tests; this is an implementation defect, not a quota or credential failure.

The original controller cleanup record incorrectly reported no jobs and logical absence. Its bytes and digest remain preserved; [outcome.json](outcome.json) explicitly invalidates that inference. It must not be used as cleanup proof. Current inventory discovers candidates by the run namespace and verifies exact ID, name, project, region and ownership label through an independent `JOB_VIEW_SUMMARY` read. Missing ownership, authorization failure or an unavailable exact read blocks verification.

## Actual failure and closure sequence

An independent exact read found the job pending. Google rejected normal cancellation with `FAILED_PRECONDITION`: the job was not yet ready for cancellation. The [captured error and job messages](provider-observations.json) retain actual message IDs and workflow diagnostic codes; no explicit request ID was exposed in captured responses. [Pending jobs can transition only to running or failed](https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#JobState).

During the premature teardown, provider messages reported inaccessible staged packages and a missing or disabled service account. These observations are consistent with removing dependencies before the job was terminal. They do not prove that processing would otherwise have succeeded. The job reached `JOB_STATE_FAILED` at **2026-11-15T21:41:44.953243Z**. No force cancellation or new job was executed.

A corrected fresh native inventory at **2026-11-16T01:18:35Z** identifies that terminal job and no active owned resources, including workers, disks, networking, datasets, live buckets, identities, image repository or deadline runner. All eight archived generations (251,610,089 bytes, including staging artifacts) were independently size/hash verified. Two buckets remain soft-deleted until November 22 UTC and two custom roles remain in deletion grace; provider history and managed service identities remain. Actual charges are unavailable; logical retirement is not billing clearance.

Current cancellation logic waits within a finite ten-minute budget if pending cancellation is rejected. An unresolved job prevents staging/archive/infrastructure deletion. Tests exercise pending-to-running-to-cancelled, perpetual pending, missing summary labels, foreign ownership and failed exact reads. The corrected code has not been deployed in another campaign.

No source cohort was published and no canary, native financial reconciliation, recovery or performance stage completed. The native guard's own deadline cancellation was not exercised. Existing local financial and failure-control evidence remains separate. Original private responses remain untouched; public job records are labeled display subsets with the project identifier replaced by `example-project`. [SHA-256 inventory](sha256.json).

The actual driver also emitted four mixed-window warnings for operational-observation flattening ([preserved observations](driver-observations.json)). They are a separate pre-campaign review item, not the established cause of this controller abort. Native processing did not advance far enough to validate those paths. No replacement campaign has been prepared or started.
