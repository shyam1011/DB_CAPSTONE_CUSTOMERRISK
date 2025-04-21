# -----------------------------------
# GOLD TABLE - CUSTOMER LIFE TIME VALUE
# -----------------------------------

CREATE OR REPLACE MATERIALIZED VIEW customer360.gold.customer_clv_segment
COMMENT "Customer Lifetime Value segmented dynamically using percentiles"
AS
WITH cltv AS (
  SELECT
    customer_id,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction_value,
    MAX(transaction_timestamp) AS last_transaction_ts
  FROM customer360.silver.transaction_data
  GROUP BY customer_id
),
cltv_ranked AS (
  SELECT *,
    NTILE(3) OVER (ORDER BY total_revenue DESC) AS segment_rank
  FROM cltv
)
SELECT *,
  CASE segment_rank
    WHEN 1 THEN 'High'
    WHEN 2 THEN 'Medium'
    ELSE 'Low'
  END AS cltv_segment
FROM cltv_ranked;
