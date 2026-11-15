# Native bootstrap attempt — November 15, 2026 UTC

**Streaming acceptance did not execute.** The approved attempt created the isolated image-build prerequisites, verified live compute capacity, submitted one real Cloud Build and then stopped on an HTTP 404 in the controller's operation polling. No Dataflow job, streaming warehouse dataset, Pub/Sub source, deadline task, canary, financial reconciliation or performance campaign was started.

The regional create call returned a legacy operation name. Polling that name through the global operation route failed. The controller now resolves the accepted build ID and polls the canonical regional Build resource. A fresh read-only GET against the actual cancelled build returned HTTP 200, independently validating that route. This does not establish that a new image build or streaming workflow has succeeded.

[Actual outcome and cleanup observations](outcome.json). The build reached CANCELLED. Fresh metadata reads returned 404 for the owned repository, build identity, build bucket and private network; the owned Dataflow job count was zero. Terraform retired all 15 state entries, while enabled APIs and Google-managed identities remain deliberately retained. The source bucket is soft-deleted until the recorded November 22 UTC deletion time; billing clearance and actual charges are not established. The source generation was exported and verified before deletion.

The failed attempt and its raw private records remain preserved. No automatic full-run retry occurred. Further provisioning requires a new explicit owner approval and a fresh run namespace. This is bootstrap/API/cleanup evidence, not managed streaming or commercial-results evidence.
