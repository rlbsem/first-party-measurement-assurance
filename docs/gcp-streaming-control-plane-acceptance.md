# Governed Streaming Measurement & Recovery — Google Cloud engineering contract

**Status: architecture and acceptance specification only.** This document does not claim Pub/Sub, Dataflow, or streaming benchmark execution. The existing native BigQuery/dbt/MERGE evidence, browser implementation, GA4 public-sample evidence, synthetic business definitions, and historical artifacts remain independent and unchanged.

## Thesis and hiring signal

**Question:** Can delayed, duplicated, conflicting and unauthorized first-party event streams become timely, accurate business measures while recovering from crashes, sink advancement, replay and publication races—without losing events, repeating financial value, or publishing incomplete results?

Build an **isolated streaming lane within First-Party Measurement Assurance**. It is not another acquisition-finance lakehouse or generic cloud ETL port. The proof is **stateful distributed processing, authority, operational correctness and measured architecture trade-offs**.

The architecture must pass a skeptical senior data/platform architecture review: source-specific IAM; keyed state and timers; event-time windows and corrected state; durable canonical business identity; source/sink checkpoint divergence; concurrency-safe publication; independently derived source oracle; constrained native performance; and audit-grade failure evidence.

**Principle: tools execute; the control layer determines what is true.** A consented browser signal does not establish a contract or payment. Pub/Sub message IDs and Dataflow delivery guarantees do not establish durable business identity. Business event time does not establish source completeness. Candidate tables do not become authoritative merely because a Dataflow job succeeded.

## Physical and logical architecture

~~~mermaid
flowchart TD
  P[Authenticated synthetic source publishers] --> A[Sealed source archive + manifest]
  P --> S[Pub/Sub ingress topics / subscriptions]
  S --> D[Beam / Dataflow: schema, keyed state, timers, event time]
  D --> E[BigQuery append-oriented delivery and exception ledgers]
  D --> Q[Quarantine + late-horizon rebuild requests]
  A --> R[Bounded archive replay / reconstruction]
  R --> E
  E --> M[dbt SQL: canonical revisions, measured facts, candidate releases]
  A --> O[Independent source-only Python oracle]
  M --> G[Coverage, authority, reconciliation + policy gate]
  O --> G
  G -->|verified and current predecessor| V[Transactional release selector]
  V --> C[Published-only restricted consumer]
  S --> X[Direct Pub/Sub-to-BigQuery baseline]
  D --> T[Cloud Monitoring / execution logs]
  G --> T
~~~

Required services: Google Cloud Pub/Sub, Apache Beam on Dataflow, BigQuery, Cloud Storage for frozen source archives/staging, scoped IAM service accounts, Cloud Logging and Cloud Monitoring. Use Terraform for exact-owned isolated resources and reproducibility. Use short-lived identities, pinned SDK/dependencies and runtime images. Private Dataflow workers and Private Google Access are desirable if supported by a scoped network/cost plan; do not casually deploy permanent NAT or unrelated cloud services. An isolated local DirectRunner path is **not native evidence**.

**Baseline is mandatory:** a separate Pub/Sub BigQuery subscription delivering the same frozen source cohort directly to a landing table, followed by matching governed SQL/dbt logic. Compare its correctness, freshness, simplicity and cost against the Dataflow path. Native Dataflow must earn its added stateful processing complexity; direct ingestion may win for simpler requirements.

The two paths consume independent subscriptions. They may share immutable source/archive fixtures and the independent oracle but must not modify each other's landing tables or publication selectors.

## Synthetic scenario and provenance

Use versioned multi-tenant **measurement** fixtures: consented browser inquiries, qualified or disqualified leads, source-authorized contract signatures and actual deposits/progress payments. Keep explicit event, item, session, payment transaction, signed commitment, marketing event, campaign/day, tenant and publication grains. Model multiple independent home-services operators with varying volume; **do not replicate the 50-company acquisition consolidation financial model**.

Every source segment is sealed by a manifest containing source ID, tenant, legal entity, permitted event classes, segment sequence/count, source content hash, effective cutoff, manifest version and final-segment marker. An independent source-only oracle reconstructs expected canonical events and amounts from these archived records, without importing the Beam/SQL transformation helpers.

An immutable event envelope includes tenant ID, legal-entity ID, registered source ID, source record ID, business-event type, canonical business key, trusted source revision and effective timestamp, producer event ID, transport message metadata, schema version, payload hash, permitted value basis, currency/integer minor units when valued, issuer identity binding, ingest time and execution/policy version. Source-specific publisher authentication and Pub/Sub IAM/topic ownership must bind actual identity to authorized source/tenant/event classes. Self-declared payload source/tenant strings have no authority.

Use distinct evidence counters for publisher attempts, Pub/Sub accepted messages, delivered messages, persisted sink records, unique canonical business events, admitted financial effects, late corrections and quarantined exceptions. **Exactly-once transport processing is not a promise that every physical publisher attempt appears in the sink.**

## Executable stream-time contract

Use an explicit, versioned **acceptance profile**, with fixture-scale defaults reviewed for native cost; do not silently import GA4 backdating rules as streaming delivery semantics.

| Dimension | Acceptance requirement |
|---|---|
| Timestamp | Extract trusted source event time from a validated field/attribute; retain separate source effective time, broker publish time, ingest time and processing time |
| Window | Begin with one-minute event-time fixed windows for provisional metrics; label these nonfinancial, never approved revenue |
| Allowed lateness | Configurable; initial fixture profile tests a 10-minute allowed-lateness horizon, including samples just before, at, and after the boundary |
| Triggers | Explicit on-time emission and recorded late-update behavior; capture pane identity and whether pane replacement or accumulation is selected |
| Per-key state | Keyed tenant/source/business ID; explicit canonical revision, seen payload hash, current allowed source classes, first/last timestamps and conflict marker |
| Timers | Event-time timer for window closure plus processing-time safety/idle timer as implemented; prove firing, cleanup and restart behavior |
| Expiry | Bound short-lived operational state and measure its growth; *expiry must not delete durable business identity or suppress later corrections* |
| Beyond-horizon data | Preserve in append ledger and named rebuild/correction queue; replay archived source into a **new** candidate; never silently discard as an ordinary late pane |
| Clock anomalies | Test ahead-of-time, behind-time, malformed and incompatible-zone timestamps; quarantine untrusted extremes without advancing watermarks indefinitely |
| Completeness | A watermark never seals a financial source batch: the separately authenticated source manifest and all expected segments must reconcile before publication |

Predeclare exact cutoff rules, state/timer lifecycle and accumulation semantics as executable tests before the first cloud run. Include a deliberate idle source and a skewed hot tenant; prove no watermark/stale-source deadlock creates a false completed publication.

### Canonical business identity and no-regress behavior

Pub/Sub can redeliver messages and different publish attempts can receive different broker IDs. Attribute-based deduplication has a finite window; it is **not** durable source-level idempotency.

Canonical identity is the tuple of authorized source, tenant/entity, business key, revision and normalized payload hash under an explicit schema/policy version. Exact replay yields no new financial effect; same key/revision with differing hashes yields an inspectable conflict; later authorized source revision yields a *new candidate*; earlier revision arriving last can never regress an approved value. Untrusted user/browser records cannot claim payment or contract authority. Preserve the original anomalous event instead of rewriting history.

Persist durable event/revision/conflict state in BigQuery-controlled records (or an explicitly justified alternative durable store) beyond Beam state lifetime. Document any consistency/collision tradeoff. Sink write retries can produce duplicates; canonical fact selection and approval gates must be replay-safe. Do not make uncontrolled external business mutations from retryable Beam transforms.

## Source, sink and state recovery

Test **three separate authorities**:

1. **Source:** sealed immutable Cloud Storage source segments and original publisher receipts, including acknowledged and unacknowledged segments.
2. **Processing:** Dataflow checkpoint/save-state or native snapshot where supported, including watermark/keyed timer state and exact job/source metadata.
3. **Sink:** BigQuery already contains committed append history, candidate rows and approved release selectors; it is **not rolled back** when processing state is restored.

Required failure experiment: make a sink commit and then interrupt, cancel or fault processing before its acknowledgment or checkpoint is safely reflected. Restore or restart from an independently recorded processing boundary while retaining the advanced sink. Compare whole sink content, every canonical source effect and the release selector to the source-only oracle. No double payment, duplicate publication, stale-release resurrection or lost source segment is acceptable.

Also test **independent archive replay** after state/window expiration, not only Pub/Sub redelivery within the broker retention horizon. No seeking backward on a live Dataflow subscription as a casual replay tactic; use a separately bounded subscription/archive strategy and preserve original failed attempts.

If Dataflow snapshot or streaming update is unsupported for the selected SDK/job configuration, record the provider failure and execute the explicit archive-reconstruction recovery separately. Never call a recreated job with empty state a successful snapshot restore.

## Versioned publication and concurrent approvals

Bronze is append-oriented, Silver is canonical source- and cutoff-specific, Gold calculates precisely scoped business metrics. A full cross-system transaction spanning Pub/Sub, Dataflow, Cloud Storage and BigQuery is **not claimed**.

Create immutable candidate tables/partitions with source hashes, policy version, complete manifests, exact independent-oracle reconciliation and unresolved variances. Block the affected tenant/cutoff publication when a source segment is missing or authority is unconfirmed. Keep prior approved versions readable.

Release selection must use a **single transactionally enforced expected-predecessor change** or other demonstrably atomic compare-and-set mechanism, with checked row count, candidate verification and concurrent-writer conflict handling. A separate read followed by an unguarded update is not sufficient. Repeat the exact approval request after a lost response; it must not republish. A stale approver and two competing valid candidates must not both become the successor. Test failure before commit, failure after commit before acknowledgment, parallel approvals, and stale candidate attempts.

Consumer queries must resolve only committed approved versions under separate identities. An unpublished candidate—even one with all material rows written—must be invisible to the approved-only consumer.

## Native security acceptance

Provision **distinct** scoped identities for publisher, Dataflow worker, release verifier/approver, and consumer, plus an independently authorized teardown role. Bound publisher rights to exact topics/sources; workers to required subscription and append destinations; verifier to source archive and candidate validation; consumer to published-only authorized views.

Use separate authenticated principals for positive and negative tests. Demonstrate source/tenant spoofing rejection, cross-tenant read denial, direct raw ledger denial, unauthorized publication/write denial and role-escalation denial. Attribute a failed request to its real principal and classify authentication, workspace entitlement, IAM and data-plane permission failures separately. Do not publish account identity metadata or secrets. These are synthetic known-fixture security conformance tests, not a claim of production SSO or formal compliance certification.

## Quantified operating envelope: performance and trade-offs

**Correctness and recovery are mandatory; raw volume is not the primary goal.** Use small fixture acceptance as an *automated internal stage* of a complete campaign, not a separate prompt or independent deliverable. Progress to native 100K and 1M event comparisons when permissions, observability, provider quota and bounded cost projection are satisfied. A larger run is optional if it exposes a meaningful bottleneck.

Experiment matrix:
- Identical deterministic cohort across direct Pub/Sub BigQuery subscription and Dataflow paths, accounting for their distinct processing semantics.
- Uniform tenant/key distribution versus high-cardinality state and deliberately hot tenant/key distributions.
- Steady input and rapid backlog bursts, plus post-failure backlog recovery and delayed-source correction.
- At least one constrained worker configuration and one larger worker configuration supported by measured project-specific quota; repeat comparable runs.
- Validate publisher throughput independently so source generation bottlenecks do not masquerade as Dataflow saturation.
- Measure p50/p95/p99 **end-to-end source-eligible-to-approved-publication** latency separately from sink-visibility and processing latency; report job setup/worker provisioning time separately.

Metrics required: published vs delivered vs canonical accepted counts; per-stage exception/conflict counts; watermark lag; backlog; worker-hours, Streaming Engine and disk usage where applicable; source byte sizes; job/Query IDs; throughput; state growth; recovery time; cost per **million unique accepted business events** when billed usage becomes visible. Use provider-observed utilization and actual charge rows when available; do not call planning estimates billed cost.

Predeclare freshness, loss, duplication, parity and recovery service-level **test targets** before running. Record misses, negative results, unstable variance and confidence/number of repeated runs. A single 1M-event success is not an unrestricted enterprise capacity claim.

## Infrastructure, budget and bounded native campaign

Use a named, tagged run namespace; explicit provider API permissions; Terraform plans with readable proposed resource changes; pinned container/dependency digests; region-consistent Pub/Sub, GCS, BigQuery and Dataflow configuration; finite retention and restricted IAM. Confirm actual project-specific vCPU, IP, disk, Dataflow and BigQuery quotas and the selected worker/Streaming Engine pricing before provisioning. Configure Dataflow worker and maximum-worker counts within verified capacity, with explicit job life limits and cancellation/drain checks.

No unbounded streaming jobs, scheduled recurrences, excessive logging, permanent NAT, keys in source, external production API access, billing changes or unrelated account resource modifications. Prefer a separate teardown identity and exact-run owned resource inventory. Independent cleanup must check terminal jobs, snapshots, Pub/Sub subscriptions/topics, BigQuery datasets, GCS retention, staging objects, build images and IAM principals. **Logical deletion is not proof of physical erasure or final billing clearance.**

Private operational spending authority is handled outside this public engineering specification. Before any cloud writes, prepare an account-specific CAD price/usage envelope and owner-approved limits; budget alerts are not hard spending caps. Monitor conservative worst-case cost from planned worker/runtime, Streaming Engine, Pub/Sub, BigQuery and storage retention even when provider billing usage is delayed.

Execute one staged campaign: automated tiny canary → source/identity/event-time correctness → fault and sink-ahead recovery → real security denials → comparative baseline and Dataflow performance → evidence export → cleanup. Continue automatically past a passing canary and routine stages without repeated user approvals. Stop on a genuine policy, permission, correctness, quota or material spending exposure blocker. Do not bypass a failed gate to chase impressive row counts.

## Definition of done and evidence integrity

1. Architecture decision records document source authority, delivery guarantees, durable state, late cutoffs, sink advance, concurrent promotion, BigQuery subscription baseline and why Dataflow is/is not warranted.
2. Terraform plan, isolated IAM, publisher/archive fixture generator, real Beam transforms, direct-ingestion baseline, canonical facts/dbt publication and guarded release control implemented.
3. Credible offline DirectRunner/contract/property-based tests: replay, conflicting revisions, timer lifecycle, watermark skew, beyond-horizon rebuilding, missing manifests, spoofed authority, lost acknowledgment and competing approvals.
4. Source-bound native Pub/Sub, Dataflow and BigQuery execution logs with actual job/resource IDs, native results, test failures and fixes; separate authenticated security-denial observations.
5. Independent Python oracle reconstructs all expected facts and financial measures directly from archived source records; every material candidate compared with complete rowsets.
6. Measured operating-envelope experiments, including direct ingestion comparison and one stateful Dataflow failure/recovery under constrained resources; scale results report real limits.
7. Evidence exporter, manifest hashes, offline evidence verifier, residual inventory and teardown report. Preserve original failed runs and historical artifacts unchanged.
8. Updated README links a concise **executed** architecture + results matrix and genuine sanitized images or diagrams, not proposed findings or funding details. Draft PR with CI; no merge until review.

**Public evidence labels:** native-executed, local-executed, planned/not-executed, synthetic input, provider-reported measurement, cost estimate vs actual charge. Never relabel any one category as another.

## Official semantics references

- https://docs.cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub
- https://docs.cloud.google.com/dataflow/docs/concepts/exactly-once
- https://docs.cloud.google.com/dataflow/docs/guides/using-snapshots
- https://docs.cloud.google.com/pubsub/docs/bigquery
- https://docs.cloud.google.com/bigquery/docs/transactions
- https://docs.cloud.google.com/dataflow/docs/guides/routes-firewall
- https://docs.cloud.google.com/dataflow/quotas
