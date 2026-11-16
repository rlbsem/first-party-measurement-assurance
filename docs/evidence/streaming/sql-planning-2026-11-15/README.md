# BigQuery SQL planning — November 15, 2026

Google BigQuery accepted the actual seven model definitions and published-view template in **dry-run mode**, against inline synthetic schemas. This is native SQL type/planning validation, not query execution, data correctness, dbt build acceptance, IAM enforcement or transaction acceptance. No dataset was created; the provider returned no job ID and estimated zero bytes processed.

[Provider planning observation and exact source hashes](verification.json). The helper records a Git base plus exact working source hashes, rather than labeling new bytes as that earlier commit. Three earlier dry runs rejected issues in the inline validation wrapper (reserved alias, missing transport column, empty-table syntax); those private failure records remain preserved. No production model behavior was inferred from the dry run.
