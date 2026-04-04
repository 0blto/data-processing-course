ALTER TABLE sales_pipeline SET DISTRIBUTED BY (opportunity_id);
ANALYZE sales_pipeline;
ALTER TABLE accounts SET DISTRIBUTED BY (account);
ALTER TABLE sales_teams SET DISTRIBUTED BY (sales_agent);
