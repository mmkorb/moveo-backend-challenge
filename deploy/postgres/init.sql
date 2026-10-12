-- Runs once, as analytics_owner (POSTGRES_USER), on an empty data volume.
-- Creates only the DML-only runtime role; the password comes from a Compose secret (ADR 0010, 0015).
\getenv dbname POSTGRES_DB
\set app_pw `cat /run/secrets/analytics_app_password`

CREATE ROLE analytics_app LOGIN PASSWORD :'app_pw';
REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO analytics_app;
