-- Read only the two validation job records; exclude project IDs and user identities.
SELECT job_id, creation_time, start_time, end_time, state,
  error_result IS NULL AS completed_without_error, job_type, statement_type, priority,
  total_bytes_processed, total_bytes_billed, total_slot_ms, cache_hit,
  LOWER(TO_HEX(SHA256(query))) AS sql_sha256
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE creation_time >= TIMESTAMP('2026-10-17')
  AND creation_time < TIMESTAMP('2026-10-18')
  AND job_id IN ('job_EgSHl_of2fLqQeIIjpuktW4Bk8AM', 'job_JKIqXynG1hJxZJ06oOVpnxXeV1Dm')
ORDER BY creation_time
