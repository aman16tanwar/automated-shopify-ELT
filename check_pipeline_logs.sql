-- Check recent pipeline job logs
SELECT 
  timestamp,
  log_level,
  message,
  store_url,
  component
FROM `happyweb-340014.shopify_logs.pipeline_logs`
WHERE job_id IN (
  SELECT job_id 
  FROM `happyweb-340014.shopify_logs.pipeline_jobs`
  WHERE store_url = 'sorio-romstar.myshopify.com'
  ORDER BY started_at DESC
  LIMIT 5
)
ORDER BY timestamp DESC
LIMIT 100;

-- Check job status
SELECT 
  job_id,
  store_url,
  status,
  started_at,
  completed_at,
  error_message
FROM `happyweb-340014.shopify_logs.pipeline_jobs`
WHERE store_url = 'sorio-romstar.myshopify.com'
ORDER BY started_at DESC
LIMIT 10;