# Streaming submission, window and sink safety review

This review covers the three follow-up findings on source `e1030a4`. It is an implementation review with executed offline checks, not a new external audit or native acceptance. The [original job-discovery failure](evidence/streaming/native-job-discovery-2026-11-15/README.md), invalid initial cleanup claim and four real window warnings remain unchanged.

## Submission and teardown admission

A generation-conditional GCS registry is created before arming. Each unique job name is PREPARED before starting the driver. The driver atomically marks it POSSIBLE immediately before `pipeline.run()`, then records the returned provider ID before reporting success. Local SQLite generation transactions exercise crashes and races without cloud credentials.

| Durable state | Meaning | Teardown admission |
|---|---|---|
| OPEN with no attempts | No dispatch recorded; new dispatch remains possible | Blocked until the registry is sealed |
| PREPARED | Driver has not crossed the dispatch fence | Sealing converts it to ABSENT; a later driver cannot dispatch |
| POSSIBLE | Submission may have reached Google, ID unresolved | Blocked regardless of empty listings or elapsed time |
| CONFIRMED | Provider ID recorded | Exact `JOB_VIEW_SUMMARY` ownership and terminal reads required |
| TERMINAL | Exact matching provider terminal receipt recorded | Fresh independent reads must still agree |
| ABSENT | Registry sealed before dispatch | Only this local pre-dispatch proof establishes absence; no empty-list shortcut |

Owner retirement and the cloud stopper share resolution and sealing code but use separate identities and fresh provider reads. Exact IDs are read even when list responses are empty. Unknown IDs are discovered by namespace and verified with exact ID/project/region/name/label checks. Conflicting IDs, unregistered owned jobs, 403s, unresolved 404s, corrupted registries and terminal-to-active contradictions block retirement. Pending cancellation rejection waits within the existing 600-second stop budget; accepted cancellation is not repeatedly submitted. Three bounded guard attempts do not create new processing jobs.

The sealed terminal/absent closure is archived before deleting GCS dependencies and remains available for independent post-teardown verification. If an ambiguous dispatch never becomes observable, automated retirement deliberately remains blocked. Preserve the guard, staging, identities and evidence; escalate the exact submission record. No safe timeout can prove that provider-side creation never happened. This can outlive the cost reservation and is a residual operational risk, not a spending-cap guarantee.

The registry uses [GCS generation preconditions](https://cloud.google.com/storage/docs/request-preconditions). Checksums detect corruption; they are not protection against a privileged administrator deliberately rewriting evidence. A trusted owner can always bypass cloud controls outside this application.

## Event time and operational transport

Fixed-window provisional aggregates retain their event-time assignment, watermark trigger, accumulated counts, on-time/late pane indices and extracted window boundaries. Only after extracting those fields is the provisional observation stream placed in GlobalWindows to match decisions, exceptions and timer observations. The underlying admitted ledger and financially authoritative SQL are unchanged. Timers retain keyed identity and expiry clocks.

A real cross-language Storage Write graph is expanded locally, including both BigQuery sinks and all six relevant Flatten transforms. The graph verifier checks serialized input window strategies and rejects the specific mixed-window warning. It never calls `pipeline.run()`. DirectRunner TestStream checks on-time and late panes, unchanged source bytes and event-time expiry; the existing processing-time expiry, correction, replay and financial tests remain active. Java expansion proves graph construction, not managed-runner execution.

## Independent sink rejection evidence

Both [Beam Storage Write failed-row outputs](https://beam.apache.org/releases/pydoc/2.77.0/apache_beam.io.gcp.bigquery.html) feed an independent GCS journal. No error is sent back into BigQuery. A generation-CAS index intent precedes append-only detail persistence; a missing detail therefore remains unresolved. Original rejects remain immutable even after exact replay recovery. Provider text is bounded and redacted; restricted records retain source binding, cohort/segment/ordinal, physical delivery identity, normalized content hash and retryability classification. Classification is diagnostic and never grants retry authority.

Limits are 1,000 distinct rejects, 16 KiB per detail, 4 KiB per diagnostic, 128 KiB per index write, 12 CAS attempts and a 60-second health scan budget checked between records. Provider operations have bounded deadlines. Exhaustion, unavailable preservation, corrupt records, missing index/detail, or unindexed details fail closed. The Beam failure DoFn raises after persistence so the rejected bundle cannot be treated as accepted. No blind row resubmission is implemented.

A source effect can be marked recovered only by the independent verifier's immutable resolution: exact persisted normalized row, complete duplicate-collapsed source-position/content comparison and source-manifest oracle must agree. Original physical rejection is not relabeled as acknowledged or persisted. `reconcile-sink` is an explicit future authorized operation; it does not publish data or approve financial results. Operational/pane failures remain blocked pending explicit review. Independent financial rowset/totals reconciliation is still required afterward.

Transport submission, broker acknowledgment, raw sink persistence, observed rejection, replay recovery and canonical admission are separate states. Raw completeness, model building, candidate verification and each new approval check journal health. Historical receipt reconciliation is read-only and remains available during an outage; it does not authorize a new effect. The final native acceptance validator requires a clear, source-bound sink-health record before cleanup.

## Critical publication and recovery boundaries

BigQuery owns the candidate fingerprint, current policy/source selector, expected predecessor epoch, exact-request receipt and competing-approval transaction. GCS failure admission and that transaction are not atomic across services. Pre/post health checks refuse acceptance if the journal changes, and preserve an effect that may already have committed; they do not falsely report rollback. A concurrent rejection may be discovered after a financially reconciled selector transaction commits. This lane does not promise instantaneous global quarantine or zero transient visibility across GCS and BigQuery. A production requirement for that guarantee needs a warehouse-local release fence and consumer clearance contract, independently designed and tested.

The finite campaign has immutable source batches, full source-byte coverage and independent financial proofs before candidate release. This protects the published financial content separately from operational sink health. Unresolved errors block acceptance; they never justify deleting dependencies of a possible job. Source authority stays bound to authenticated subscriptions, not publisher-supplied tenant fields. Beam state is an optimization and diagnostic boundary; durable source versions, canonical revision rules and independent reconciliation retain authority through replay and state expiry.

IAM adds four conditional bucket grants, no new compute/service: cleanup may mutate only the exact submission fence; worker may mutate only the failure index and append/read failure details; verifier may append resolution evidence. Workers receive no publication SQL or resolution-write permission. Effective IAM remains to be tested natively; mocked Terraform tests are not genuine denial evidence.

## Next native acceptance gates

| Gate | Required observation | Current boundary |
|---|---|---|
| Source and image | Exact source, saved plans, immutable image digest; no scope drift | Local implementation; new image not built |
| Stopper | Independent full task readback, durable fence visibility, scoped CAS permission, cancellation behavior | New protocol not deployed |
| Submission | Driver record/ID survives list lag; unknown outcomes retain dependencies | Local crash/race tests |
| Canary | Both sinks, source completeness, independent financial rowsets | Managed processing unexecuted |
| Sink negatives | Real rejected row and independent durable readback; unresolved error blocks release; controlled recovery | Local failure DoFn and journal tests |
| Security | Actual source/tenant/publication and journal IAM denials | Offline contracts; native checks still required |
| Recovery and approvals | Sink-ahead replay, exact receipts, competing successor, current policy | Offline regression; native stage pending |
| Comparative scale | Same frozen sources for direct-ingestion and 1/2-worker Dataflow; 100K/1M conditional on correctness/cost/time | No native performance numbers |
| Retirement | Sealed submission closure, exact terminal reads, generation export, independent residual inventory | New protocol not deployed |

No new cloud execution is authorized by this document. Private review preparation may read billing linkage, quota/pricing/permission metadata and generate Terraform plans; it must not apply them. Prior usage, retention and delayed reporting remain unresolved; historical owner-reported credits are not a verified spendable balance. No automatic full-campaign retry, account change, or recurring work is permitted. The next campaign requires separate exact-plan approval and fresh cost/headroom checks.

## Reproduction

Use the pinned streaming dependencies and Java 17 or later for graph expansion:

```text
python -m pytest streaming_tests -q
python scripts/check_streaming_graph.py --output .local/new-graph-check
python scripts/check_streaming_evidence.py
python scripts/package_streaming.py --check
```

The recovery entry point is `python -m streaming.native reconcile-sink --manifest PRIVATE_MANIFEST --failure-id HASH --source-archive INDEPENDENT_ARCHIVE --work FRESH_PRIVATE_DIRECTORY`. It makes read-only warehouse queries and writes a scoped resolution artifact, so it belongs only inside an explicitly authorized future recovery operation. Never use it to clear an operational/pane reject automatically.
