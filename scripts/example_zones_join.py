"""
Example query showing how to join taxi trips with zone information.
"""
import os
from sqlalchemy import create_engine, text
import pandas as pd

PG_USER = os.getenv('PG_USER', 'root')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'root')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'ny_taxi')

CONNECTION_STRING = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = create_engine(CONNECTION_STRING)

print("\n=== NYC Taxi Trips with Zone Information ===\n")

# Example 1: Sample trips with pickup and dropoff zone names
query1 = """
SELECT 
    t.tpep_pickup_datetime,
    pickup_zone."Borough" as pickup_borough,
    pickup_zone."Zone" as pickup_zone,
    dropoff_zone."Borough" as dropoff_borough,
    dropoff_zone."Zone" as dropoff_zone,
    t.trip_distance,
    t.total_amount
FROM yellow_tripdata t
LEFT JOIN zones pickup_zone ON CAST(t."PULocationID" AS INTEGER) = pickup_zone."LocationID"
LEFT JOIN zones dropoff_zone ON CAST(t."DOLocationID" AS INTEGER) = dropoff_zone."LocationID"
WHERE t.tpep_pickup_datetime IS NOT NULL
ORDER BY t.tpep_pickup_datetime DESC
LIMIT 10;
"""

print("Sample trips with zone information:")
print("=" * 120)
df = pd.read_sql(query1, engine)
if not df.empty:
    for _, row in df.iterrows():
        print(f"{row['tpep_pickup_datetime']} | {str(row['pickup_borough']):12s} {str(row['pickup_zone']):30s} → "
              f"{str(row['dropoff_borough']):12s} {str(row['dropoff_zone']):30s} | "
              f"${float(row['total_amount']):6.2f} ({float(row['trip_distance']):.1f} mi)")
else:
    print("No trip data found. Please ingest trip data first.")

print("\n")

# Example 2: Most popular pickup zones
query2 = """
SELECT 
    z."Borough",
    z."Zone",
    COUNT(*) as trip_count,
    ROUND(AVG(CAST(t.total_amount AS NUMERIC)), 2) as avg_fare
FROM yellow_tripdata t
JOIN zones z ON CAST(t."PULocationID" AS INTEGER) = z."LocationID"
WHERE t.total_amount IS NOT NULL AND t.total_amount != ''
GROUP BY z."Borough", z."Zone"
ORDER BY trip_count DESC
LIMIT 15;
"""

print("Most popular pickup zones:")
print("=" * 80)
df2 = pd.read_sql(query2, engine)
if not df2.empty:
    for _, row in df2.iterrows():
        print(f"{row['Borough']:15s} | {row['Zone']:40s} | {row['trip_count']:8,} trips | Avg: ${row['avg_fare']}")
else:
    print("No trip data found.")

print("\n")

# Example 3: Top borough-to-borough routes
query3 = """
SELECT 
    pickup_zone."Borough" as from_borough,
    dropoff_zone."Borough" as to_borough,
    COUNT(*) as trip_count
FROM yellow_tripdata t
JOIN zones pickup_zone ON CAST(t."PULocationID" AS INTEGER) = pickup_zone."LocationID"
JOIN zones dropoff_zone ON CAST(t."DOLocationID" AS INTEGER) = dropoff_zone."LocationID"
GROUP BY pickup_zone."Borough", dropoff_zone."Borough"
ORDER BY trip_count DESC
LIMIT 10;
"""

print("Top borough-to-borough routes:")
print("=" * 60)
df3 = pd.read_sql(query3, engine)
if not df3.empty:
    for _, row in df3.iterrows():
        print(f"{row['from_borough']:15s} → {row['to_borough']:15s} | {row['trip_count']:8,} trips")
else:
    print("No trip data found.")

print("\n")
