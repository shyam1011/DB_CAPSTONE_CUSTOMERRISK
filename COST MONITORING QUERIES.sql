#-------------------------
# 1. ALERT CREATION
#--------------------------

#Daily DBU Spike Alert
SELECT 
  DATE(CURRENT_TIMESTAMP) AS check_date,
  ROUND(SUM(dbu_usage), 2) AS dbu_today
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE()
HAVING dbu_today > 100 -- Threshold

#Set up this query in a SQL dashboard, and add an alert rule:
#“If dbu_today > 100 → send alert”


#Alert if any user exceeds 50 DBUs in a day
SELECT 
  user_name,
  ROUND(SUM(dbu_usage), 2) AS total_dbu
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE()
GROUP BY user_name
HAVING total_dbu > 50;

#Set up alert: "Trigger when result row count > 0"




#-------------------------
# 2. DASHBOARD CREATION - MONITORING
#--------------------------

#-------------------------
# DBU COST BY USERS
#--------------------------

SELECT 
  user_name, 
  ROUND(SUM(dbu_usage), 2) AS total_dbu
FROM system.billing.usage
GROUP BY user_name
ORDER BY total_dbu DESC;


#-------------------------
# DBU COST BY CLUSTERS
#--------------------------
SELECT 
  cluster_name,
  cluster_type,
  ROUND(SUM(dbu_usage), 2) AS total_dbu
FROM system.billing.usage
GROUP BY cluster_name, cluster_type
ORDER BY total_dbu DESC;


#-------------------------
# STORAGE COST BY TABLES
#--------------------------
SELECT 
  catalog_name,
  schema_name,
  table_name,
  ROUND(total_size_bytes / 1e9, 2) AS size_gb
FROM system.information_schema.table_storage_metrics
ORDER BY size_gb DESC
LIMIT 10;

#-------------------------
# DBU OVER TIME
#--------------------------
SELECT 
  DATE_TRUNC('day', usage_start_time) AS usage_date,
  ROUND(SUM(dbu_usage), 2) AS daily_dbu
FROM system.billing.usage
GROUP BY usage_date
ORDER BY usage_date;