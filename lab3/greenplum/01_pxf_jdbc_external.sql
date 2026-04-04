DROP EXTERNAL TABLE IF EXISTS ext_sales_pipeline_jdbc;
DROP EXTERNAL TABLE IF EXISTS ext_sales_teams_jdbc;
DROP EXTERNAL TABLE IF EXISTS ext_products_jdbc;
DROP EXTERNAL TABLE IF EXISTS ext_accounts_jdbc;

CREATE READABLE EXTERNAL TABLE ext_accounts_jdbc (
  account           text,
  sector            text,
  year_established  integer,
  revenue           numeric(14,2),
  employees         integer,
  office_location   text,
  subsidiary_of     text
)
LOCATION ('pxf://public.accounts?PROFILE=jdbc&SERVER=pg_crm')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

CREATE READABLE EXTERNAL TABLE ext_products_jdbc (
  product     text,
  series      text,
  sales_price numeric(14,2)
)
LOCATION ('pxf://public.products?PROFILE=jdbc&SERVER=pg_crm')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

CREATE READABLE EXTERNAL TABLE ext_sales_teams_jdbc (
  sales_agent     text,
  manager         text,
  regional_office text
)
LOCATION ('pxf://public.sales_teams?PROFILE=jdbc&SERVER=pg_crm')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

CREATE READABLE EXTERNAL TABLE ext_sales_pipeline_jdbc (
  opportunity_id text,
  sales_agent    text,
  product        text,
  account        text,
  deal_stage     text,
  engage_date    date,
  close_date     date,
  close_value    numeric(14,2)
)
LOCATION ('pxf://public.sales_pipeline?PROFILE=jdbc&SERVER=pg_crm')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
