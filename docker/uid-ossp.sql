-- init-uuid-ossp.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- This is relevant but can't be ran until FROST is loaded and initializes the tables
-- https://github.com/FraunhoferIOSB/FROST-Server/discussions/2047
-- ALTER TABLE public."OBSERVATIONS" DISABLE TRIGGER datastreams_actualization_insert; 
