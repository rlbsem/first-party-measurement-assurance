# Governed Streaming Measurement & Revenue Integrity — native Google Cloud acceptance

**Status: design/acceptance specification, not executed evidence.** This extends the existing native BigQuery/dbt/MERGE project; it does not replace previously executed evidence, alter its fixtures, or claim native Pub/Sub/Dataflow execution before that happens.

## Objective

Build enterprise-grade, source-governed streaming infrastructure demonstrating canonical event identity, tenant/legal-entity isolation, event-time and processing-time semantics, late correction, idempotent replay, no-regress revision selection, independent financial reconciliation, fail-closed publication, recoverability, observability and native cloud authorization tests.

**Tools execute. The control layer determines what is true.** Browser activity and transport acknowledgments never become authorities for signed contracts or actual payments. All workloads use labeled, deterministic synthetic data, not real client records.

## Proposed architecture

~~~mermaid
flowchart LR
  W[Consent-aware web events] --> P[Pub/Sub]
  F[Authoritative synthetic signature/payment sources] --> P
  P --> D[Apache Beam / Dataflow validation]
  D --> B[BigQuery Bronze: durable delivery history]
  D --> Q[Conflicts and quarantine]
  B --> S[Governed revision selection / Silver]
  S --> G[Gold measures by correct grain and value basis]
  F --> O[Independent source oracle]
  G --> R[Source coverage and financial reconciliation]
  O --> R
  R -->|passes| L[Versioned publication selector]
  R -->|fails| Q
  L --> C[Approved-only consumer views]
  D --> M[Monitoring / Logging]
  Q --> M
~~~

Use Pub/Sub, Apache Beam on Dataflow, BigQuery, Cloud Storage for bounded replay/staging/evidence, service-account IAM and Cloud Monitoring/Logging. Do not add unrelated services to inflate the stack. Keep a local credential-free Beam DirectRunner path and a separate native execution path. Source contract, lineage and authority must remain explicit at every boundary.

## Canonical transport and source contract

A typed versioned envelope should encode at minimum: tenant ID; legal entity; source system; immutable source record ID; trusted source revision and event type; canonical business key; event-schema version; producer event ID; event/effective timestamp and ingest timestamp; normalized payload hash; currency and integer minor units when financially valued; explicit value basis (unvalued, signed commitment, actual cash); policy/source authority reference; and pipeline provenance.

Acceptance rules:
- Exact replay must not create another financial effect.
- Identical canonical key and revision with different bytes is a named conflict, not last-write-wins.
- A later authorized correction creates a new revision and candidate publication.
- Older, lower-authority or stale revisions cannot regress approved source state.
- Missing source inventory/coverage, cross-entity key collisions and open material variances block publication.
- Consent-aware browser events may be unvalued lead signals, never authoritative financial facts.
- A payment event is actual collected cash, not the entire signed contract; GA4 nested item grain is never treated as the purchase-event grain.

Pub/Sub transport message IDs are not canonical business IDs. Publisher retries may create separate message IDs. Document exact deduplication boundaries and the selected Dataflow processing mode plus BigQuery Storage Write API sink mode. Do not make a blanket claim of exactly-once business effects from a transport feature. Test the configured semantics and maintain a deterministic governed deduplication/publication layer.

## End-to-end processing and governance

1. **Ingress:** bounded topic/subscription, deterministic synthetic generator, immutable source manifest and receipt/readback evidence; explicitly model publisher retries, duplicate deliveries and delayed correction batches.
2. **Validation:** closed schema, version/type validation, tenant/legal entity/source authority checks, integer-value conservation, trusted-event time and eligibility. Route failed cases to an inspectable exception store without retaining sensitive payloads in logs.
3. **Bronze:** append delivery history with source version, provenance, content hash and replay/conflict observations. Do not delete evidence to make results match.
4. **Silver:** deterministic canonical revision selection at explicit cutoffs, quarantine equal-revision conflicts, reject lower-authority writes and preserve original source memberships.
5. **Gold:** event, item, session, payment transaction, signed contract, company/day and campaign grains remain separate. Reconcile against independent Python source traversal, not only circular SQL-to-SQL comparisons.
6. **Publication:** build immutable candidates, verify independent source coverage, exact rowsets and unresolved exceptions, then move an explicit manifest/selector to the approved version. A partial candidate cannot become visible. Do not claim cross-table atomicity.
7. **Consumers:** approved-version-only views; independent readback and negative principal tests. Observability must report backlogs, throughput, lateness, errors, lag and actual job/worker behavior.

## Required native acceptance (each must be executed or explicitly marked not executed)

| Area | Acceptance evidence |
|---|---|
| Pub/Sub | Genuine topic/subscription publish, receipt, redelivery/replay observation and message metadata |
| Dataflow | Genuine job ID, SDK/runtime versions, worker configuration, task state, job metrics and execution logs |
| BigQuery | Actual sink readback, query IDs/bytes/slot usage, documented Storage Write API mode and committed rows |
| Identity | Source/business-key replay under different transport IDs creates one financial effect |
| Conflict | Same identity and revision with different content is quarantined |
| Late correction | New trusted revision yields corrected candidate; previously approved publication stays queryable |
| No-Regress | Older or lower-authority revision cannot overwrite newer approved source fact |
| Completeness | Missing source batch and incompatible legal entity block publication |
| Finance | Full rowset equality to independent oracle for each material grain and revision |
| Failure | Deliberate runtime failure/restart, durable recovery and no double financial publication |
| Sink failure | Malformed/rejected writes leave inspectable exception and safe recovery state |
| Security | Separate authenticated restricted consumer; actual allowed reads and prohibited cross-tenant/raw/write operations |
| Operations | Actual throughput, backlog/lag, worker/cost metrics, late-data and failure counters |
| Cleanup | Owned-job drain/cancel, exact-owned resource removal and independent residual inventory |

Preserve failed native attempts with original run IDs and corrected follow-up evidence. A workspace-login failure does not count as a data-plane authorization denial. A local test does not count as native Dataflow acceptance.

## Enterprise-scale study

Create deterministic synthetic fixtures for **50 acquired companies**, each with independent source IDs and legal entities, uneven distributions, bursty senders, corrections, duplicates, late events, malformed envelopes, missing batches and deliberate cross-tenant conflicts.

Run native correctness before throughput benchmarking. Target measured workloads of **100K** and **1M** deliveries; evaluate 5M+ only if projected account capacity and cost permit. Record attempted, published, delivered, unique business, Bronze, Silver, quarantine and published-financial counts separately. Each benchmark must include payload size, timestamps, job ID, worker resources, region, event rate, end-to-end lag percentiles, cost/DBU-equivalent provider units as applicable, and independent reconciliation results.

Run comparable constrained and scaled configurations; include a controlled backlog burst and at least one failure/recovery experiment. Preserve actual performance even if worse than expected. Do not claim measured scalability based solely on fixture size or provider marketing.

## Automation, deployment and safety

- Versioned schema/contracts, deterministic fixture generator, pinned Python/Beam dependencies, tested IAM/resource configuration, controlled deployment and cleanup scripts, and CI with local Beam correctness tests.
- Isolated project/run label; no changes to unrelated resources. Use short-lived identity, least privilege, no committed JSON keys.
- Explicit timeouts, bounded Dataflow streaming runtime, worker ceilings, bounded Pub/Sub backlog/retention, finite retries and measurable stop conditions. Drain or cancel all jobs and verify terminal status; no permanent stream or schedule.
- Keep private credentials and cloud identifiers out of public records. Export sanitized native job/result evidence, source hashes, actual failure states and resource inventory before teardown.
- Verify logical absence separately from storage retention and final billing. Never declare a budget alert to be an absolute spending cap.

**Owner-reported funds:** CAD 421 in Google Cloud credits. Suggested engineering execution envelope CAD 350, leaving CAD 71 unallocated. This is a proposal, not verified billing availability, an account-enforced ceiling, or unlimited spending authorization. Obtain current service/region pricing, scope and permissions before writes.

**Canary principle:** Perform one automatic, small acceptance fixture as the first stage **inside the complete implementation and same native execution campaign**. It checks the real API, identity, sink and runtime. If it passes, continue to the already-prepared correctness, fault, permission and bounded scale runs without another planning session or user-prompt loop. Stop only for genuine security, correctness, account, resource/cost exposure or cleanup blockers. Delayed usage data is not evidence of zero charges; calculate conservative exposure from bounded worker-time/resource assumptions.

## Definition of done

1. Source contracts, governed financial authority, architecture/decision records and threat/failure model committed.
2. Real Beam/Dataflow implementation, publisher, BigQuery models, exception/recovery logic, IAM and cleanup implemented.
3. Offline regression and property-based replay/correction/conflict tests green.
4. Genuinely executed Pub/Sub/Dataflow/BigQuery acceptance with independent financial oracle and negative security tests; actual provider IDs and logs preserved.
5. At least one meaningful measured scaling comparison, if native constraints permit; otherwise explicitly mark blocked and state why.
6. Reproducible native runbook, source-bound sanitized evidence, content hashes, independent evidence verifier, cleanup and residual inventory.
7. Hiring-manager-facing README updated only after native execution, reporting what actually ran rather than proposed results. No account-funding labels.
8. Draft PR and passing checks; preserve historical files and leave the GitHub profile untouched. Do not merge without review.

Official references:
- https://cloud.google.com/dataflow/docs/guides/streaming-modes
- https://cloud.google.com/dataflow/docs/concepts/exactly-once
- https://cloud.google.com/dataflow/docs/guides/write-to-bigquery
- https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub
- https://cloud.google.com/pubsub/docs/exactly-once-delivery
