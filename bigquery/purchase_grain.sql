-- Keep purchase rows at event grain. UNNEST only inside correlated aggregation.
WITH purchases AS (
  SELECT ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue AS reported_revenue,
    (SELECT SUM(i.price * i.quantity) FROM UNNEST(items) i) AS item_value
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX = '20210101' AND event_name = 'purchase'
)
SELECT COUNT(*) AS purchase_events,
  COUNT(DISTINCT NULLIF(transaction_id,'')) AS distinct_transaction_ids,
  COUNTIF(transaction_id IS NULL OR transaction_id='') AS missing_transaction_ids,
  COUNTIF(reported_revenue IS NULL) AS missing_revenue,
  COUNTIF(item_value IS NOT NULL AND reported_revenue IS NOT NULL
          AND ABS(item_value-reported_revenue)>0.01) AS item_revenue_disagreements
FROM purchases
