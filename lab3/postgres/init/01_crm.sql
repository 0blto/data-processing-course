CREATE TABLE accounts (
  account           text PRIMARY KEY,
  sector            text NOT NULL,
  year_established  integer,
  revenue           numeric(14,2),
  employees         integer,
  office_location   text,
  subsidiary_of     text
);

CREATE TABLE products (
  product     text PRIMARY KEY,
  series      text NOT NULL,
  sales_price numeric(14,2) NOT NULL
);

CREATE TABLE sales_teams (
  sales_agent     text PRIMARY KEY,
  manager         text NOT NULL,
  regional_office text NOT NULL
);

CREATE TABLE sales_pipeline (
  opportunity_id text PRIMARY KEY,
  sales_agent    text NOT NULL,
  product        text NOT NULL,
  account        text,
  deal_stage     text NOT NULL,
  engage_date    date,
  close_date     date,
  close_value    numeric(14,2)
);

COPY accounts FROM '/import/accounts.csv' WITH (FORMAT csv, HEADER true, NULL '');
COPY products FROM '/import/products.csv' WITH (FORMAT csv, HEADER true, NULL '');
COPY sales_teams FROM '/import/sales_teams.csv' WITH (FORMAT csv, HEADER true, NULL '');
COPY sales_pipeline FROM '/import/sales_pipeline.csv' WITH (FORMAT csv, HEADER true, NULL '');

ANALYZE accounts;
ANALYZE products;
ANALYZE sales_teams;
ANALYZE sales_pipeline;
