# GA4 warehouse review evidence

- [Access assessment](access-assessment.json): current SDK/connector blockers and observed sandbox, without private account identifiers.
- [Native read-only acceptance](native-readonly/acceptance.json): exact source binding and deliberately limited execution claims.
- [Actual model results](native-readonly/model-results.json) and [independent source results](native-readonly/source-results.json): five matching row counts and duplicate-preserving multiset signatures.
- [Provider job metadata](native-readonly/job-statistics-results.json): uncached SELECT jobs, source SQL hashes, timestamps and resource counters.
- [Local incremental/replay/full build](local/pipeline.json) and [complete Python oracle](local/oracle.json): original synthetic fixture, not the public source.
- [Reproducible complete-oracle acceptance](local/complete-oracle-acceptance.json): every field and duplicate multiplicity checked against both actual dbt databases.
- [Actual regression-guard fault observations](local/guard-observations.json): original misses retained beside corrected results.
- [Protected baseline inventory](preserved-baseline.json): original application, model bodies, fixtures and historical evidence remain byte-for-byte unchanged.

[Engineering explanation and exact reproduction commands](../../warehouse-review.md).

The new native result comprises seven Jinja-rendered model SELECT bodies executed through BigQuery Studio and a separate source oracle. **No native dbt compile/build, incremental MERGE, live GA4 export or production integration was executed.** SDK credentials remain unavailable, and the existing billing-disabled sandbox excludes DML. No billing/IAM configuration or persistent dataset was created.

Native observations use Google's separate obfuscated public dataset. Local late-arrival, correction, replay and adversarial tests use synthetic source data. Neither demonstrates commercial outcomes or reconciles Google's sample to the fictional home-services business.
