# First-Party Measurement Assurance

**A $245,000 project is signed. The customer pays a $73,500 deposit. Why does marketing report another $245,000 purchase?**

An inquiry, signed project and payment represent different business events. This application traces those events through browser collection, offline source authority, durable receipts and dbt reconciliation. **Evermere Design + Build** is a fictional US home-services operator used for the synthetic measurement scenarios.

The local browser, HTTP and DuckDB/dbt workflows execute with synthetic USD project data. Native BigQuery evidence uses Google's separate public GA4 sample. The core application runs without private credentials.

![Executed project measurement-health console](docs/images/measurement-health.png)

The exact fixture has **$920,000 in proposals, $435,000 in signed contracts and $143,500 in actual payments**. Measured contract conversions total $190,000; measured payment conversions total $302,000. The system exposes three financial findings: a missing $245,000 contract conversion, a missing $13,000 progress-payment conversion and a deposit overstated by $171,500.

Proposals, signed commitments and collected cash retain separate measures and SQL grains. A deposit is not counted as another full contract. [Exact synthetic results](docs/evidence/demo.json) · [Evidence index](docs/evidence/README.md).

## Try the application

Use **Python 3.12** from the existing source checkout:

```text
python -m venv .venv
```

Activate `.venv\Scripts\Activate.ps1` on Windows, or `source .venv/bin/activate` on macOS/Linux, then:

```text
python -m pip install -r requirements.lock
python -m playwright install chromium
python -m assurance.server --fixture --workspace .local/projects-v2
```

Open `http://127.0.0.1:8550`. On Linux, browser tests may require `python -m playwright install --with-deps chromium`. This server binds to loopback. Use a fresh v2 workspace: earlier databases are refused and left untouched.

1. **Measurement health:** compare source contracts and payments with their separate measured conversions. Inspect stage counts, rejected attempts, late arrivals and receipt details.
2. **Inquiry laboratory:** request a consultation before permission, allow analytics, inspect the real GTM-compatible data-layer message, replay it, introduce schema drift and lose an acknowledgement.
3. **Warehouse proof:** inspect recorded local dbt results, the exact contract-versus-deposit failure and explicit native-access limits.

![Real inquiry instrumentation with the actual data-layer message](docs/images/browser-laboratory.png)

Screenshots show the running application; decorative visuals use CSS. The synthetic opportunities range from $65,000 to $485,000. Browser inquiries collect no contact details and carry no assumed sales or payment value. [Browser and screenshot evidence](docs/evidence/browser.json).

## Commercial definitions and authority

| Business fact | Measurement | Authority and value |
|---|---|---|
| Website inquiry / consultation request | `generate_lead` | Browser after an independent inquiry receipt; unvalued |
| Qualified lead | `qualify_lead` | Trusted offline source; unvalued |
| Consultation/contact underway | `working_lead` | Trusted offline source; unvalued |
| Disqualified lead | `disqualify_lead` | Trusted offline source; unvalued |
| Estimate or proposal | Source context only | No sale, payment or assumed conversion revenue |
| Signed contract | `close_convert_lead` | Trusted signature source; signed USD commitment once |
| Actual deposit or progress payment | `purchase` | Trusted payment source; unique transaction and actual USD amount |

The browser collector cannot claim offline source authority by supplying `origin=offline`. Trusted offline events require a registered project, source stage, timestamp and eligible session association. Signature and payment facts enter through the Python fixture/adapter, not a public browser endpoint.

## Controls under failure

| Problem | Executed behavior |
|---|---|
| No analytics permission | Inquiry can succeed; measurement is suppressed before queuing |
| Permission revoked during retry | Pending events are cleared; current server permission gates admission |
| A delayed grant response arrives | Generation fencing prevents it from overriding revocation |
| Response lost after commit | Retry the same ID; record another receipt and one admitted effect |
| New ID for an existing conversion/payment | Independent conversion and transaction keys prevent repeated value |
| Same identity, different content | Named conflict; original admitted content remains immutable |
| Unknown fields or schema drift | Closed internal v2 contract rejects without retaining payload bytes |
| Payment items do not sum to value | Integer-cent admission check and dbt conservation gate |
| Payment uses full contract value | Self-consistent event remains visibly mismatched with authoritative payment |
| Late conversion | Explicit seven-day arrival policy, late flag and original event date |
| Nested payment items cause fanout | Separate event/item/payment grains and source-total checks |

## Engineering evidence

```text
python scripts/verify.py
```

The verifier checks Python/API and adapter contracts, Chromium interactions, three dbt builds, a deliberately failing SQL case, incremental/full-build equivalence, independent reconciliation and artifact integrity. Runtime data stays in ignored workspaces.

The revised demonstration has **29 admission attempts, 19 distinct events, three contained replays, one late arrival and three unresolved financial findings**. All **nine dbt models** compare exactly with an independent full build. Each successful build passes **21 data tests**. [Executed verification](docs/evidence/verification.json) and [control-to-test map](docs/testing.md).

**Native GoogleSQL also executed** against Google's separate public GA4 sample in a billing-disabled sandbox: two uncached queries agree on 14 purchase events, while 57 item rows, 13 distinct transaction IDs and two item/revenue disagreements remain visible. [Native results, job IDs and exact resource usage](docs/evidence/google-bigquery/README.md).

![Executed warehouse proof and source-value distinction](docs/images/warehouse-proof.png)

## GA4 warehouse modeling extension

The [isolated GA4 warehouse module](ga4_pipeline/README.md) takes synthetic GA4-style event versions and item rows through dbt into governed event, purchase, session and daily metrics. Its acceptance harness tests late-arriving events, corrected purchases, replay safety, preserved transaction/revenue discrepancies and row-for-row incremental versus full-refresh parity.

The local DuckDB/dbt verification and optional BigQuery source projections are separate from the [previously executed public GA4 BigQuery queries](docs/evidence/google-bigquery/README.md). **Native dbt-bigquery execution remains unverified.** [Architecture and execution contract](docs/ga4-warehouse.md).

## GTM and Google boundaries

| Evidence lane | Actual status |
|---|---|
| Real browser instrumentation | Local HTTP and actual `dataLayer.push` with the standard `event` property |
| GTM-compatible data layer | Version 1 contract tested independently of internal collection envelope v2 |
| Imported/published GTM container | **Not configured or executed**; concrete [implementation guide](docs/gtm-implementation.md) provided |
| Local source-to-analytics processing | SQLite, DuckDB/dbt, independent source oracle and full-build comparison executed |
| GA4 payload projection | Six documented lead/payment event patterns, USD units and explicit value basis locally tested |
| Google Measurement Protocol debug | **Not executed**; requires an approved isolated property configuration |
| Native BigQuery public sample | **Executed 2026-10-17** through BigQuery Studio in a billing-disabled sandbox; two public-data queries plus a job-metadata lookup. [Evidence](docs/evidence/google-bigquery/README.md) |
| Production GA4 to BigQuery integration | **Not implemented or claimed** |
| Hosted GitHub CI | [Windows and Ubuntu verification](https://github.com/rlbsem/first-party-measurement-assurance/actions/workflows/verify.yml); inspect the run for the relevant commit |

The native GoogleSQL results come from Google's public ecommerce sample, not Evermere. GTM deployment, GA4 export and dbt-bigquery execution remain outside this test. [Native runbook](docs/native-google.md) · [Google references](docs/sources.md).

[Architecture and source lineage](docs/architecture.md) / [Operations and limits](docs/operations.md) / [Scenario revision and retained controls](docs/scenario-revision.md).

This project focuses on browser measurement, source authority and conversion integrity. It complements [Customer Signal & Activation](https://github.com/rlbsem/customer-signal-activation) and [Revenue Metrics Platform](https://github.com/rlbsem/revenue-metrics-platform) without recreating their activation or financial systems.

Built by [Richard Butts](https://github.com/rlbsem).
