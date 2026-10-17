# External specifications and portfolio context

Reviewed 2026-10-16. These are implementation references, not claims of certification or affiliation.

| Primary source | Implementation use |
|---|---|
| [Google recommended lead and ecommerce events](https://developers.google.com/analytics/devguides/collection/ga4/reference/events) | `generate_lead`, `qualify_lead`, `working_lead`, `disqualify_lead`, `close_convert_lead` and payment-only `purchase`; a restricted local subset |
| [Google Tag Manager data layer](https://developers.google.com/tag-platform/tag-manager/datalayer) | Standard `event` property, atomic parameter message and preserved data-layer identity |
| [GTM event mapping examples](https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtm) | Data Layer Variables, Custom Event trigger and GA4 Event tag implementation guide |
| [Google MP validation](https://developers.google.com/analytics/devguides/collection/protocol/ga4/validating-events) | Debug endpoint, strict validation, positive/negative responses and reporting limitations |
| [Google consent setup](https://developers.google.com/tag-platform/security/guides/consent) | Default-denied and explicit updates as design context; no Google Consent Mode implementation is claimed |
| [GA4 BigQuery export schema](https://support.google.com/analytics/answer/7029846) | Nested event parameters, item arrays and ecommerce transaction fields |
| [Official ecommerce public dataset](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset) | Read-only optional GoogleSQL proof over obfuscated export data; the official period is November 2020 through January 2021 |
| [BigQuery sandbox](https://docs.cloud.google.com/bigquery/docs/sandbox) | Optional no-billing project route; access and quotas still require owner setup |

The implementation is original code. No public sample rows have been downloaded or represented as local fixtures. Local source economics come from `assurance/fixtures.py`; actual browser actions create unvalued synthetic inquiry receipts. The fictional operator is unrelated to Google's public ecommerce sample.

Public portfolio inspection covered the [profile](https://github.com/rlbsem), [Customer Signal & Activation](https://github.com/rlbsem/customer-signal-activation), [Revenue Metrics Platform](https://github.com/rlbsem/revenue-metrics-platform), [Enterprise MarTech / AI Control Plane](https://github.com/rlbsem/enterprise-martech-ai-control-plane), and [MarTech Data Reliability](https://github.com/rlbsem/martech-data-reliability). This implementation focuses on browser measurement contracts, source authority and conversion integrity rather than rebuilding CRM activation, finance or generic feed recovery.
