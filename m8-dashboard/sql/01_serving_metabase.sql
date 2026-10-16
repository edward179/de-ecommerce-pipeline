-- M8: serving database + Metabase app database on de-postgres.
-- Run once as the M1 superuser (POSTGRES_USER). Passwords are passed with psql -v, never stored here.

-- 1) Login roles
CREATE ROLE serving_writer LOGIN PASSWORD :'serving_writer_pw';
CREATE ROLE bi_reader      LOGIN PASSWORD :'bi_reader_pw';
CREATE ROLE metabase_app   LOGIN PASSWORD :'metabase_db_pw';

-- 2) Databases, one owner each
CREATE DATABASE serving  OWNER serving_writer;
CREATE DATABASE metabase OWNER metabase_app;

-- 3) By default every user can CONNECT to every database: revoke, then grant only where needed
REVOKE CONNECT ON DATABASE olist    FROM PUBLIC;
REVOKE CONNECT ON DATABASE serving  FROM PUBLIC;
REVOKE CONNECT ON DATABASE metabase FROM PUBLIC;
GRANT  CONNECT ON DATABASE serving  TO bi_reader;

-- 4) marts schema in the serving database
\connect serving
CREATE SCHEMA marts AUTHORIZATION serving_writer;
GRANT USAGE ON SCHEMA marts TO bi_reader;
-- Tables that serving_writer creates later in marts are readable by bi_reader automatically
ALTER DEFAULT PRIVILEGES FOR ROLE serving_writer IN SCHEMA marts
  GRANT SELECT ON TABLES TO bi_reader;
