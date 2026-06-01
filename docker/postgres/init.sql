-- CDC setup script: run this ONCE on your existing Postgres.
--
-- Usage:
--   psql -U postgres -d <your_db> -f docker/postgres/init.sql
--
-- Prerequisites:
--   wal_level = logical  (SHOW wal_level; -- must return "logical")

-- 1. Demo table (skip if you already have one to watch)
CREATE TABLE IF NOT EXISTS public.customers (
    id    SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name  TEXT NOT NULL,
    email      TEXT UNIQUE NOT NULL
);

-- 2. Full row image so Debezium can capture before/after on updates
ALTER TABLE public.customers REPLICA IDENTITY FULL;

-- 3. Replication role for Debezium (idempotent: won't fail if it exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
        CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
    END IF;
END
$$;

-- GRANT CONNECT on whatever database this script is running in.
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO replicator', current_database());
END
$$;
GRANT USAGE ON SCHEMA public TO replicator;
GRANT SELECT ON public.customers TO replicator;

-- 4. Publication for CDC (INSERT + UPDATE + DELETE)
--    DROP + CREATE so re-running this script is safe.
DROP PUBLICATION IF EXISTS dbz_cdc_pub;
CREATE PUBLICATION dbz_cdc_pub FOR TABLE public.customers;
