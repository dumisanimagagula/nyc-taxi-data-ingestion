{{
  config(
    materialized='incremental',
    unique_key=['year', 'month', 'day_of_week', 'pickup_location_id'],
    on_schema_change='fail',
    schema='gold',
    incremental_strategy='merge',
    # Performance: Partition for faster incremental updates
    pre_hook="ALTER TABLE {{ this }} SET TBLPROPERTIES ('iceberg.write.distribution-mode'='hash')"
  )
}}

-- Daily Trip Statistics - Incremental Materialized View
-- Aggregated metrics by day and location
-- Only reprocesses last 7 days to handle late-arriving data

SELECT
    year,
    month,
    day_of_week,
    pickup_location_id,
    
    -- Trip metrics
    COUNT(*) as total_trips,
    SUM(passenger_count) as total_passengers,
    
    -- Distance metrics
    AVG(trip_distance) as avg_trip_distance,
    MIN(trip_distance) as min_trip_distance,
    MAX(trip_distance) as max_trip_distance,
    
    -- Fare metrics
    AVG(fare_amount) as avg_fare,
    MIN(fare_amount) as min_fare,
    MAX(fare_amount) as max_fare,
    SUM(total_amount) as total_revenue,
    
    -- Duration metrics
    AVG(trip_duration_minutes) as avg_trip_duration,
    
    -- Metadata
    CURRENT_TIMESTAMP as calculated_at

FROM {{ source('silver', 'nyc_taxi_clean') }}

{% if execute %}
  -- Incremental filter: only process recent data and late arrivals
  {% if is_incremental() %}
    WHERE pickup_datetime >= (SELECT DATE_SUB(MAX(DATE(pickup_datetime)), INTERVAL 7 DAY)
                              FROM {{ this }})
  {% endif %}
{% endif %}
    year,
    month,
    day_of_week,
    pickup_location_id
