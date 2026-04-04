CREATE READABLE EXTERNAL TABLE ext_accounts_gpfdist (
  account           text,
  sector            text,
  year_established  integer,
  revenue           numeric(14,2),
  employees         integer,
  office_location   text,
  subsidiary_of     text
)
LOCATION ('gpfdist://gpfdist:8080/accounts.csv')
FORMAT 'CSV' (HEADER);

DROP TABLE IF EXISTS accounts_from_gpfdist;

CREATE TABLE accounts_from_gpfdist (
  LIKE accounts
) DISTRIBUTED BY (account);

INSERT INTO accounts_from_gpfdist SELECT * FROM ext_accounts_gpfdist;

ANALYZE accounts_from_gpfdist;
