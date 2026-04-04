\set ECHO all

-- Запрос 1: воронка + компании (JOIN по account)

EXPLAIN (COSTS OFF, VERBOSE)
SELECT p.deal_stage, COUNT(*), SUM(p.close_value)
FROM sales_pipeline p
JOIN accounts a ON a.account = p.account
GROUP BY p.deal_stage;

-- Запрос 2: три таблицы — сделки, продукты, команды продаж

EXPLAIN (COSTS OFF, VERBOSE)
SELECT t.regional_office, pr.series, COUNT(*)
FROM sales_pipeline p
JOIN products pr ON pr.product = p.product
JOIN sales_teams t ON t.sales_agent = p.sales_agent
GROUP BY t.regional_office, pr.series;

-- Запрос 3: иерархия компаний (дочерние счета) + суммы по сделкам

EXPLAIN (COSTS OFF, VERBOSE)
SELECT COALESCE(a.subsidiary_of, a.account) AS group_name,
       COUNT(DISTINCT p.opportunity_id) AS deals,
       SUM(p.close_value) AS revenue
FROM sales_pipeline p
JOIN accounts a ON a.account = p.account
WHERE p.deal_stage = 'Won'
GROUP BY 1
ORDER BY revenue DESC NULLS LAST
LIMIT 20;
