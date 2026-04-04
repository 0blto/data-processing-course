DROP TABLE IF EXISTS sales_pipeline;
DROP TABLE IF EXISTS sales_teams;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
  account           text,
  sector            text,
  year_established  integer,
  revenue           numeric(14,2),
  employees         integer,
  office_location   text,
  subsidiary_of     text
) DISTRIBUTED BY (account);

INSERT INTO accounts SELECT * FROM ext_accounts_jdbc;

CREATE TABLE products (
  product     text,
  series      text,
  sales_price numeric(14,2)
) DISTRIBUTED BY (product);

INSERT INTO products SELECT * FROM ext_products_jdbc;

CREATE TABLE sales_teams (
  sales_agent     text,
  manager         text,
  regional_office text
) DISTRIBUTED BY (sales_agent);

INSERT INTO sales_teams SELECT * FROM ext_sales_teams_jdbc;

CREATE TABLE sales_pipeline (
  opportunity_id text,
  sales_agent    text,
  product        text,
  account        text,
  deal_stage     text,
  engage_date    date,
  close_date     date,
  close_value    numeric(14,2)
) DISTRIBUTED BY (opportunity_id);

INSERT INTO sales_pipeline SELECT * FROM ext_sales_pipeline_jdbc;

ANALYZE accounts;
ANALYZE products;
ANALYZE sales_teams;
ANALYZE sales_pipeline;
