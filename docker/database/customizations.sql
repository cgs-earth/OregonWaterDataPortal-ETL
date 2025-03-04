-- init-uuid-ossp.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
SELECT pg_reload_conf();

GRANT pg_monitor TO postgres_exporter;
ALTER USER postgres_exporter SET SEARCH_PATH TO postgres_exporter,pg_catalog,public;


-- This is relevant but can't be ran until FROST is loaded and initializes the tables
-- Also unclear if this would mess up the FROST feature where the phenomenon time of a 
-- datastream is automatically updated with the time of the linked observations.
-- Likely best to play it safe and keep this commented out unless needed
-- https://github.com/FraunhoferIOSB/FROST-Server/discussions/2047
-- ALTER TABLE public."OBSERVATIONS" DISABLE TRIGGER datastreams_actualization_insert; 
