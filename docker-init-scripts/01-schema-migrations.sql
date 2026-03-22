-- Schema migrations and fixes for yellow_tripdata table
-- This script runs automatically when the PostgreSQL container initializes

-- Fix tpep_pickup_datetime column type if it exists as text
-- This prevents "operator does not exist: text >= timestamp" errors
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'yellow_tripdata'
    ) THEN
        -- Check if column exists and is of wrong type
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'yellow_tripdata' 
            AND column_name = 'tpep_pickup_datetime'
            AND data_type = 'text'
        ) THEN
            ALTER TABLE yellow_tripdata
            ALTER COLUMN tpep_pickup_datetime TYPE timestamp USING tpep_pickup_datetime::timestamp;
            RAISE NOTICE 'Successfully converted tpep_pickup_datetime from text to timestamp';
        ELSE
            RAISE NOTICE 'tpep_pickup_datetime is already the correct type or does not exist yet';
        END IF;
    ELSE
        RAISE NOTICE 'yellow_tripdata table does not exist yet - will be created during ingestion';
    END IF;
END $$;
