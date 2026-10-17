# Verification and preserved controls

The scenario changes business definitions and authority while retaining the earlier invariant tests. Existing test functions still cover retry identity, transaction identity, closed fields/types, rejected payload privacy, value conservation, current consent, arrival bounds, rollback/restart, concurrent replay, independent economics, GA4 unit conversion, HTTP boundaries, same-count wrong-value corruption and source-hash tampering. Native budget tests remain in place.

The fixed-retail catalog check is now a project/service-binding check plus independent actual-payment reconciliation. Bespoke project amounts cannot be proved by a fixed catalog price. Internal unknown-version tests now use version 3 because version 2 is explicitly implemented.

| Experiment | Actual assertion |
|---|---|
| Existing contract/privacy cases | No unknown fields, invalid values, unsupported currency, raw URL or PII-like rejected content in snapshots |
| Exact retry / changed content | One immutable event and distinct duplicate/conflict receipts |
| Concurrent delivery | 16 calls / 8 threads; one admission |
| Pre-commit fault | Event and receipt roll back; reopen and retry succeeds |
| Current consent and regrant | New grant cannot adopt an old source association; fresh eligible source works |
| Browser default denial | No measurement POST, data-layer message, cookie or persistent identifier |
| Browser response loss | Real committed HTTP event, discarded acknowledgement, one effect after retry |
| Network exhaustion / revoke | Exactly three failed attempts, retained queue, clearing and no replay |
| Delayed grant | Later revocation wins |
| Data-layer contract | Actual `event` message validates separately; no source/session authority or monetary lead value |
| Browser offline claim | Rejected despite `origin=offline`; no signature/payment/source HTTP writer |
| Proposal conversion | Proposal cannot authorize `close_convert_lead` |
| Duplicate contract | New ID cannot repeat signed value; changed economics conflict |
| Multiple payments | Deposit + progress payment accumulate cash once; contract value stays unchanged |
| Full-contract deposit error | Valid item math still yields a source value mismatch |
| Source safety | Unknown source, project mismatch, altered time, duplicate source, missing contract and overpayment rejected |
| Business inquiry retry | Stable source key; changed-request rejection; denied inquiry remains not eligible |
| Legacy database | Refused byte-for-byte without reset or reinterpretation |
| dbt equivalence | Baseline, late-corrected incremental and independent full build; every row in nine models |
| SQL negative experiment | Corrupted payment item quantity fails `item_conservation` |
| Independent negative experiment | Same count with wrong measured cents fails the Python comparison |
| Model grain | Currency/value basis remain separate; item and project source totals cannot fan out |
| Reproducibility | Two fresh source-ledger snapshots hash identically |
| Responsive browser | Three views at 390, 768 and 1440 pixels; mobile amounts/findings remain visible |
| Public delivery | Screenshot/source hashes, Markdown targets, privacy patterns, manifest and ZIP members verified |

The current build runs nine models and 21 data tests. Python/API totals and browser assertions come from [verification.json](evidence/verification.json). The expected SQL rejection is recorded separately from successful builds.

Chromium executes locally; no cross-browser certification or complete accessibility audit is claimed. Visual inspection supplements layout checks. GitHub Actions executes credential-free verification on Windows and Ubuntu. [Hosted results](https://github.com/rlbsem/first-party-measurement-assurance/actions/workflows/verify.yml) report the status for each commit. [Native BigQuery executed separately](evidence/google-bigquery/README.md) through Google Cloud Console. Eight additional offline evidence tests preserve the original 77-test suite and reject altered or incomplete records. Local adapter and evidence checks do not submit cloud jobs. MP debug remains blocked without an approved isolated property configuration.
