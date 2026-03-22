{{
  config(
    materialized='incremental',
    unique_key=['year', 'month', 'payment_type'],
    on_schema_change='fail',
    schema='gold',
    incremental_strategy='merge',
    partition_by=['year', 'month'],
    # Performance: Partition for efficient incremental updates
    cluster_by=['payment_type']
  )
}}

-- Revenue by Payment Type - Incremental Materialized View
-- Revenue breakdown and trip statistics by payment method
-- Partitioned by year/month for efficient incremental refreshes

SELECT
    year,
    month,
    payment_type,
    
    -- Trip count
    COUNT(*) as trip_count,
    
    -- Revenue metrics
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_total_amount,
    
    -- Tip metrics
    AVG(tip_amount) as avg_tip_amount,
    SUM(tip_amount) as total_tips,
    
    -- Calculate tip percentage
    CASE 
        WHEN SUM(fare_amount) > 0 
        THEN (SUM(tip_amount) / SUM(fare_amount)) * 100 
        ELSE 0 
    END as avg_tip_percentage,
    
    -- Metadata
    CURRENT_TIMESTAMP as calculated_at

FROM {{ source('silver', 'nyc_taxi_clean') }}

{% if execute %}
  -- Incremental filter: only process recent data
  {% if is_incremental() %}
    WHERE pickup_datetime >= (SELECT DATEADD(month, -1, MAX(DATE(pickup_datetime)))
                              FROM {{ this }})
  {% endif %}
{% endif %}

GROUP BY 
    year,
    month,
    payment_type
