SELECT gp_segment_id, COUNT(*) AS rows_cnt,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM sales_pipeline
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

SELECT gp_segment_id, COUNT(*) AS rows_cnt,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM accounts
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- Оценка перекоса: отношение max/min по сегментам (чем ближе к 1, тем ровнее).
WITH s AS (
  SELECT gp_segment_id, COUNT(*) AS c FROM sales_pipeline GROUP BY gp_segment_id
)
SELECT MAX(c)::numeric / NULLIF(MIN(c), 0) AS skew_ratio_max_over_min FROM s;
