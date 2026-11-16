# Local submission, window and sink safety verification

Executed locally with synthetic sources after the independent review of `e1030a4`. Exact tested source hashes are in [verification.json](verification.json); the containing Git commit identifies the reviewable package. This is not a managed-cloud result.

- 165 streaming regression tests passed, including real DirectRunner processing, delayed/absent discovery, durable restart, pending cancellation, corrupt evidence, sink preservation failure and independent source readback.
- The [actual cross-language graph](graph-verification.json) contains 147 transforms. All six inspected operational/ledger Flatten inputs have compatible window strategies; the mixed-window warning is absent. Graph construction did not submit a job.
- Core reproduction passed 195 tests, three dbt builds of nine models, expected negative SQL detection, independent full-build parity and 16 actual Chromium/HTTP checks. Streaming dbt separately parsed; it did not execute natively.
- Both Terraform modules validated and five mocked security tests passed. Local smoke exercised DirectRunner and financial reconstruction; the separate GitHub container job is the container-build boundary.
- [Fresh local financial conformance](local-conformance.json) retains 16 canonical rows and separate contract/payment minor-unit totals. No commercial outcome is claimed.

The [critical architecture review and acceptance matrix](../../../streaming-safety-review.md) explain the durable dispatch fence, global-window observation transport, independent bounded failure journal, four conditional IAM additions and cross-service publication limitation. No new source cohort, cloud resource, query workload, native recovery or performance test ran for this correction.

The [original failed native job and invalidated cleanup inference](../native-job-discovery-2026-11-15/README.md) remain unchanged. Historical artifacts were not rewritten. Credentials, provider state, detailed runtime logs and current owner-review plans remain private.
