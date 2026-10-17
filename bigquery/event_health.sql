-- Native GoogleSQL. One official public shard; no writes or persistent resources.
SELECT event_name, COUNT(*) AS event_count,
  COUNTIF(user_pseudo_id IS NULL) AS missing_pseudo_id,
  COUNTIF((SELECT COUNT(*) FROM UNNEST(event_params) WHERE key='ga_session_id') > 1) AS duplicate_session_keys,
  COUNTIF((SELECT COUNT(*) FROM UNNEST(event_params) WHERE key='ga_session_id') = 0) AS missing_session_key,
  SUM(ARRAY_LENGTH(items)) AS item_rows
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX = '20210101'
GROUP BY event_name
ORDER BY event_name
