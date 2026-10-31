-- Exact native read-only preview jobs; exclude project and user identifiers.
SELECT job_id, creation_time, start_time, end_time, state,
  error_result IS NULL AS completed_without_error, job_type, statement_type, priority,
  total_bytes_processed, total_bytes_billed, total_slot_ms, cache_hit,
  LOWER(TO_HEX(SHA256(query))) AS sql_sha256
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE creation_time >= TIMESTAMP('2026-10-31')
  AND creation_time < TIMESTAMP('2026-11-01')
  AND job_id IN ('job_ib0BxcS6RY-xnY1v-bmmSzpjuNng', 'job_jQgap-SnGKMDxYOUmYmvO4OqqIO3')
ORDER BY creation_time
