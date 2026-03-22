{{
  config(
    materialized='table',
    schema='gold',
    # Performance: Full refresh is efficient for this smaller dataset
    # Indexed on common query patterns
    indexes=[
      {columns: ['year', 'month']},
      {columns: ['hour_of_day', 'pickup_location_id']},
      {columns: ['dropoff_location_id']}
    ]
  )
}}

-- Hourly Location Analysis - Materialized for Fast Access
-- Pickup and dropoff patterns by hour and location
-- Small dataset suitable for full refresh
SELECT
    year,
    month,
    day_of_week,
    hour_of_day,
    pickup_location_id,
    dropoff_location_id,
    
    -- Trip count
    COUNT(*) as trip_count,
    
    -- Fare metrics
    AVG(fare_amount) as avg_fare,
    SUM(total_amount) as total_revenue,
    
    -- Duration metrics
    AVG(trip_duration_minutes) as avg_duration,
    
    -- Distance metrics
    AVG(trip_distance) as avg_distance,
    
    -- Metadata
    CURRENT_TIMESTAMP as calculated_at

FROM {{ source('silver', 'nyc_taxi_clean') }}

GROUP BY 
    year,
    month,
    day_of_week,
    hour_of_day,
    pickup_location_id,
    dropoff_location_id
