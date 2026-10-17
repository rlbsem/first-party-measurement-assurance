# Project scenario and contract revision

Internal collection contract v2 replaces the initial retail-only demonstration with an explicit high-value USD project lifecycle. This version uses a new workspace schema. Earlier runtime databases are refused without modification rather than silently converted.

The browser contract now has its own version, `data-layer-v1.json`, using GTM's standard `event` property. It is separate from private `event-v2.json`. Unknown envelope versions and legacy databases fail closed. Use a fresh v2 workspace. This version supplies no migration that could reinterpret old monetary or source semantics.

## Retained engineering controls

The suite preserves original assertions for consent, exact replay, changed identity, payment duplication/conflict, closed fields/types, privacy rejection, item-value conservation, arrival windows, rollback/restart, concurrent calls, independent reconciliation, GA4 unit conversion, HTTP restrictions, corruption and hash tampering. Existing native budget/adapter tests remain. No test was removed solely to make the new scenario pass.

The earlier fixed catalog-price assertion is replaced with project/service binding and independently recorded payment amounts. Bespoke signed contracts and deposits cannot be validated by a retail constant. Browser commerce actions are intentionally replaced with an inquiry action; financial conversions move behind a source-authority boundary. Independent source comparison still detects a structurally valid but incorrect value.

Two models extend the original graph: `fct_conversions` preserves lifecycle keys and `mart_project_values` prevents source/measurement fanout. Existing event, item, payment, daily, campaign, health and reconciliation models remain, with explicit value-basis grains. Exact incremental/full comparison now covers nine models.

## Finite source economics

| Project | Opportunity/proposal | Signed contract | Actual payments |
|---|---:|---:|---:|
| SYN-P01 / hardscaping | $65,000 | $65,000 | $19,500 deposit + $13,000 progress |
| SYN-P02 / outdoor kitchen | $125,000 | $125,000 | $37,500 deposit |
| SYN-P03 / custom pool | $245,000 | $245,000 | $73,500 deposit |
| SYN-P04 / premium renovation | $485,000 | No signature | None |
| SYN-P05 / landscape construction | $50,000 opportunity only; disqualified | None | None |

Proposal total is $920,000, signed total is $435,000 and paid total is $143,500. These are overlapping lifecycle measures and must not be added together. Campaign attribution is descriptive, not commercial uplift. All businesses and values are fictional.

The native public GA4 ecommerce sample is retained unchanged as a separate GoogleSQL diagnostic lane. It does not become data from this fictional business.
