ALTER TABLE sales_pipeline SET DISTRIBUTED BY (account);
ANALYZE sales_pipeline;
ALTER TABLE accounts SET DISTRIBUTED BY (sector);
ALTER TABLE sales_teams SET DISTRIBUTED BY (regional_office);

